use log::trace;
use std::error::Error;
use std::fs;
use xdg::BaseDirectories;

use crate::config::Config;
use crate::traits::disk::Disk;

pub struct LocalDisk {
    local_path: BaseDirectories,
}

impl Disk for LocalDisk {
    fn from_config(_config: &Config) -> Self {
        let local_path = xdg::BaseDirectories::with_prefix("chcache").unwrap();
        LocalDisk { local_path }
    }

    async fn read(&self, hash: &str) -> Result<Vec<u8>, Box<dyn Error>> {
        let cache_file = self
            .local_path
            .get_cache_file(LocalDisk::path_from_hash(&hash));
        trace!("Cache file: {:?}", cache_file);

        let data = fs::read(cache_file)?;
        return Ok(data);
    }

    async fn write(&self, hash: &str, data: &Vec<u8>) -> Result<(), Box<dyn Error>> {
        let cache_file = self
            .local_path
            .place_cache_file(LocalDisk::path_from_hash(&hash))?;

        let cache_file_dir = cache_file.parent().unwrap();

        fs::create_dir_all(cache_file_dir)?;
        fs::write(cache_file, data)?;

        Ok(())
    }
}

impl LocalDisk {
    fn path_from_hash(hash: &str) -> String {
        return format!(
            "{}/{}/{}",
            hash.get(0..2).unwrap(),
            hash.get(2..4).unwrap(),
            hash
        );
    }
}
