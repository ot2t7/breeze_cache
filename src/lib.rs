#![feature(test)]
extern crate test;

use axum::extract::BodyStream;
use bytes::{BufMut, Bytes, BytesMut};
use futures::StreamExt;
use memory_cache::MemoryCache;
use std::{path::PathBuf, time::Duration};

pub struct Librarian {
    capacity: usize,
    length: usize,
    storage: MemoryCache<PathBuf, Bytes>,
    max_value_size: usize,
    save_path: PathBuf,
}

impl Librarian {
    pub fn new(
        capacity: usize,
        keep_alive: Duration,
        max_value_size: usize,
        save_path: PathBuf,
    ) -> Self {
        return Librarian {
            capacity: capacity,
            length: 0,
            storage: MemoryCache::with_full_scan(keep_alive),
            max_value_size,
            save_path,
        };
    }

    pub async fn get(&self, key: PathBuf) -> tokio::io::Result<Bytes> {
        return self
            .storage
            .get(&key)
            .map(|data| Ok(data.clone()))
            .unwrap_or(Ok(Bytes::from(
                tokio::fs::read_to_string(self.save_path.clone().join(key)).await?,
            )));
    }

    /// Stream in the data and save it under the `key`. The return boolean
    /// indicates whether or not the stream was also cached in memory.
    /// `true` if it was. The `content_length` can come from the Content-Length
    /// header.
    pub async fn insert(
        &mut self,
        key: PathBuf,
        mut stream: BodyStream,
        content_length: usize,
    ) -> tokio::io::Result<bool> {
        let mut can_cache = content_length < self.max_value_size;
        let mut buf = if !can_cache {
            BytesMut::new()
        } else {
            BytesMut::with_capacity(content_length)
        };

        while let Some(chunk) = stream.next().await {
            let chunk = chunk.unwrap_or_default();

            tokio::fs::write(self.save_path.clone().join(&key), &chunk).await?;

            if can_cache {
                if buf.len() + chunk.len() > buf.capacity() {
                    can_cache = false;
                    // Drop the buf, not gonna use it anymore
                    buf = BytesMut::new();
                } else {
                    buf.put(chunk);
                }
            }
        }

        // TODO: then write the file to the cache
        // TODO: add to length
        // TODO: check if adding this file will go over capacity

        return Ok(can_cache);
    }
}
