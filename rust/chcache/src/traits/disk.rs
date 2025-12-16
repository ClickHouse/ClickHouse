use crate::config::Config;
use std::error::Error;

pub trait Disk {
    fn from_config(config: &Config) -> Self;

    async fn read(&self, hash: &str) -> Result<Vec<u8>, Box<dyn Error>>;
    async fn write(&self, hash: &str, data: &Vec<u8>) -> Result<(), Box<dyn Error>>;
}
