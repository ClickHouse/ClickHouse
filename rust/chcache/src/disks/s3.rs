use aws_sdk_s3::primitives::ByteStream;
use std::error::Error;

use crate::traits::disk::Disk;
use crate::{config::Config, disks::local::LocalDisk};

use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::Client;
use aws_sdk_s3::config::{Builder, Credentials};

pub struct S3Disk {
    client: aws_sdk_s3::Client,

    bucket: String,
    key_prefix: String,
}

impl Disk for S3Disk {
    async fn from_config(config: &Config) -> Self {
        // let s3_config = aws_config::defaults(BehaviorVersion::v2025_08_07())
        //     .load()
        //     .await;

        let creds = Credentials::new("clickhouse", "clickhouse", None, None, "static");

        let s3_config = Builder::new()
            .behavior_version(BehaviorVersion::v2025_08_07())
            .endpoint_url("http://localhost:11111")
            .region(Region::new("us-east-1"))
            .credentials_provider(creds)
            .force_path_style(true)
            .build();

        let client = Client::from_conf(s3_config);

        S3Disk {
            client,

            bucket: config.s3_bucket.clone(),
            key_prefix: config.s3_key_prefix.clone(),
        }
    }

    async fn read(&self, hash: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        let s3_path = format!("{}/{}", self.key_prefix, LocalDisk::path_from_hash(&hash));

        let object = self
            .client
            .get_object()
            .bucket(self.bucket.clone())
            .key(s3_path)
            .send()
            .await?;

        let data = object.body.collect().await?;
        let data = data.into_bytes().to_vec();

        Ok(data)
    }

    async fn write(&self, hash: &str, data: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        let s3_path = format!("{}/{}", self.key_prefix, LocalDisk::path_from_hash(&hash));
        self.client
            .put_object()
            .bucket(self.bucket.clone())
            .key(s3_path)
            .body(ByteStream::from(data.clone()))
            .send()
            .await?;

        Ok(())
    }
}
