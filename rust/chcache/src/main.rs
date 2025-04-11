use log::{error, info, trace, warn};
use std::error::Error;

mod compilers;
mod config;
mod disks;
mod traits;

use crate::compilers::clang::{Clang, ClangXX};
use crate::compilers::rustc::RustC;
use crate::config::Config;
use crate::disks::clickhouse::ClickHouseDisk;
use crate::disks::local::LocalDisk;
use crate::traits::compiler::CompilerMeta;
use crate::traits::disk::Disk;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    compiler_cache_entrypoint(&Config::init()).await
}

async fn compiler_cache_entrypoint(config: &Config) -> Result<(), Box<dyn Error>> {
    let compiler_path: String = std::env::args().nth(1).unwrap();
    let rest_of_args: Vec<String> = std::env::args().skip(2).collect();

    trace!("Compiler: {}", compiler_path);
    trace!("Args: {:?}", rest_of_args);

    let just_compiler_name = compiler_path
        .split('/')
        .last()
        .unwrap_or(&compiler_path)
        .to_string();

    let compiler = match just_compiler_name.as_str() {
        RustC::NAME => RustC::from_args(
            compiler_path.clone(),
            rest_of_args.clone(),
        ),
        Clang::NAME => Clang::from_args(
            compiler_path.clone(),
            rest_of_args.clone(),
        ),
        ClangXX::NAME => ClangXX::from_args(
            compiler_path.clone(),
            rest_of_args.clone(),
        ),
        _ => {
            panic!("Unknown compiler: {}", compiler_path);
        }
    };

    if !compiler.cacheable() {
        trace!("Call is not cacheable");

        let output = std::process::Command::new(compiler_path)
            .args(&rest_of_args)
            .output()
            .unwrap();

        let stdout = String::from_utf8_lossy(&output.stdout)
            .lines()
            .filter(|c| !c.is_empty())
            .collect::<Vec<&str>>()
            .join("\n");

        println!("{}", stdout);
        eprintln!("{}", String::from_utf8_lossy(&output.stderr));
        if !output.status.success() {
            std::process::exit(output.status.code().unwrap_or(1));
        }

        return Ok(());
    }

    let local_disk = LocalDisk::from_config(config);
    let clickhouse_disk: ClickHouseDisk = ClickHouseDisk::from_config(config);

    let total_hash = compiler.cache_key();
    let compiler_version = compiler.version();

    let mut did_load_from_cache = false;
    let mut did_load_from_clickhouse = false;

    let compiled_bytes: Vec<u8> = match local_disk.read(&total_hash).await {
        Ok(bytes) => {
            info!("Local cache hit");

            compiler
                .apply_cache(&bytes)
                .expect("Unable to apply local cache");
            did_load_from_cache = true;

            bytes
        }
        Err(e) => {
            trace!("Got error: {:?}", e);
            trace!("Cache miss");

            let compiled_bytes = match clickhouse_disk.read(&compiler_version, &total_hash).await {
                Ok(bytes) => {
                    info!("Loaded from ClickHouse");

                    compiler
                        .apply_cache(&bytes)
                        .expect("Unable to apply cache from ClickHouse");

                    did_load_from_cache = true;
                    did_load_from_clickhouse = true;

                    bytes
                }
                Err(e) => {
                    trace!("Got error from CH: {:?}", e);
                    compiler.compile().expect("Unable to compile")
                }
            };

            compiled_bytes
        }
    };

    if did_load_from_clickhouse {
        local_disk
            .write(&total_hash, &compiled_bytes)
            .await
            .unwrap();
    }

    let should_upload = {
        let default_config = Config::default();

        !did_load_from_cache && config.user != default_config.user
    };

    if should_upload {
        let mut tries = 3;
        loop {
            let upload_result = clickhouse_disk
                .write(
                    &compiler_version,
                    &total_hash,
                    &compiled_bytes,
                )
                .await;

            if upload_result.is_ok() {
                info!("Uploaded to ClickHouse");
                break;
            }
            warn!("Failed to upload to ClickHouse, retrying...");

            tries -= 1;
            if tries == 0 {
                error!(
                    "Failed to upload to ClickHouse: {}",
                    upload_result.err().unwrap()
                );
                break;
            }
        }
    }

    Ok(())
}
