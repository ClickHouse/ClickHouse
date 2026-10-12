use std::error::Error;

use crate::config::Config;
use crate::traits::disk::Disk;

pub struct ClickHouseDisk {
    client: clickhouse::Client,

    source_table: String,
    target_table: String,
}

#[derive(Debug, clickhouse::Row, serde::Serialize, serde::Deserialize)]
struct CacheLine {
    #[serde(with = "serde_bytes")]
    blob: Vec<u8>,
    hash: String,
    compiler_version: String,

    compiler_args: Vec<String>,
    postprocessed_compiler_args: Vec<String>,

    #[serde(rename = "elapsed_compilation_time")]
    elapsed_compilation_time_ms: u128,
}

impl Disk for ClickHouseDisk {
    fn from_config(config: &Config) -> Self {
        let client = clickhouse::Client::default()
            .with_url(&config.hostname)
            .with_user(&config.user)
            .with_password(&config.password)
            .with_compression(clickhouse::Compression::Lz4)
            .with_option("async_insert", "1")
            .with_option("wait_for_async_insert", "0");

        ClickHouseDisk {
            client,

            source_table: config.source_table.clone(),
            target_table: config.target_table.clone(),
        }
    }

    async fn read(&self, _hash: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        unimplemented!()
    }

    async fn write(&self, _hash: &str, _data: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        unimplemented!()
    }
}

impl ClickHouseDisk {
    pub async fn read(
        &self,
        compiler_version: &str,
        hash: &str,
    ) -> Result<Vec<u8>, Box<dyn Error>> {
        let query = format!(
            "SELECT ?fields FROM {} WHERE hash = ? and compiler_version = ? LIMIT 1",
            &self.source_table
        );

        let mut cursor = self
            .client
            .query(&query)
            .bind(hash)
            .bind(compiler_version)
            .fetch::<CacheLine>()
            .unwrap();

        while let Some(row) = cursor.next().await? {
            return Ok(row.blob);
        }

        Err("Cache miss".into())
    }

    pub async fn write(
        &self,
        compiler_version: &str,
        compiler_args: Vec<String>,
        postprocessed_compiler_args: Vec<String>,
        elapsed_compilation_time_ms: u128,
        hash: &str,
        data: &Vec<u8>,
    ) -> Result<(), Box<dyn Error>> {
        let mut insert = self.client.insert(&self.target_table).unwrap();

        let row = CacheLine {
            blob: data.clone(),
            hash: hash.to_string(),
            compiler_version: compiler_version.to_string(),
            compiler_args,
            postprocessed_compiler_args,
            elapsed_compilation_time_ms,
        };

        insert.write(&row).await?;
        insert.end().await.map_err(Into::into)
    }
}
