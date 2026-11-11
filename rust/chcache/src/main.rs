use log::{error, info, trace, warn};
use std::error::Error;
use std::path::PathBuf;

mod compilers;
mod config;
mod counters;
mod disks;
mod traits;

use crate::compilers::clang::{Clang, ClangXX};
use crate::compilers::rustc::RustC;
use crate::config::Config;
use crate::counters::CacheStatsTracker;
use crate::disks::clickhouse::ClickHouseDisk;
use crate::disks::local::LocalDisk;
use crate::traits::compiler::CompilerMeta;
use crate::traits::disk::Disk;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::init();

    xdg::BaseDirectories::with_prefix("chcache").create_cache_directory("chcache")?;

    compiler_cache_entrypoint(&Config::init()).await
}

async fn compiler_cache_entrypoint(config: &Config) -> Result<(), Box<dyn Error>> {
    let stats = CacheStatsTracker::init();
    stats.increment_invocation();

    let compiler_path_or_command: String = std::env::args().nth(1).unwrap();
    let rest_of_args: Vec<String> = std::env::args().skip(2).collect();
    let compiler_cmdline = rest_of_args.clone();

    match compiler_path_or_command.as_str() {
        "stats" => {
            stats.dump();
            return Ok(());
        }
        _ => {}
    }

    let compiler_path: PathBuf = PathBuf::from(compiler_path_or_command);

    trace!("Compiler: {}", compiler_path.display());
    trace!("Args: {:?}", rest_of_args);

    let mut compiler_binary_name = compiler_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap()
        .to_string();

    if compiler_binary_name.starts_with(RustC::NAME) {
        compiler_binary_name = RustC::NAME.to_string();
    } else if compiler_binary_name.starts_with(ClangXX::NAME) {
        compiler_binary_name = ClangXX::NAME.to_string();
    } else if compiler_binary_name.starts_with(Clang::NAME) {
        compiler_binary_name = Clang::NAME.to_string();
    } else {
        panic!("Unknown compiler: {}", compiler_binary_name);
    }

    let compiler = match compiler_binary_name.as_str() {
        RustC::NAME => RustC::from_args(compiler_path.as_path(), rest_of_args.clone()),
        Clang::NAME => Clang::from_args(compiler_path.as_path(), rest_of_args.clone()),
        ClangXX::NAME => ClangXX::from_args(compiler_path.as_path(), rest_of_args.clone()),
        _ => {
            panic!("Unknown compiler: {}", compiler_path.display());
        }
    };

    if !compiler.cacheable() {
        trace!("Call is not cacheable");

        stats.increment_uncacheable();

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

    let mut did_load_from_local_cache = false;
    let mut did_load_from_clickhouse = false;

    let compiled_bytes: Vec<u8> = match local_disk.read(&total_hash).await {
        Ok(bytes) => {
            info!("Local cache hit");

            stats.increment_local_hit();

            compiler
                .apply_cache(&bytes)
                .expect(&("Unable to apply local cache for hash ".to_owned() + &total_hash));
            did_load_from_local_cache = true;

            bytes
        }
        Err(e) => {
            trace!("Got error: {:?}", e);
            trace!("Cache miss");

            let compiled_bytes = match clickhouse_disk.read(&compiler_version, &total_hash).await {
                Ok(bytes) => {
                    info!("Loaded from ClickHouse");

                    stats.increment_remote_hit();

                    compiler
                        .apply_cache(&bytes)
                        .expect("Unable to apply cache from ClickHouse");

                    did_load_from_clickhouse = true;

                    bytes
                }
                Err(e) => {
                    trace!("Got error from CH: {:?}", e);

                    stats.increment_miss();
                    compiler.compile().expect("Unable to compile")
                }
            };

            compiled_bytes
        }
    };

    if config.use_local_store && !did_load_from_local_cache {
        local_disk
            .write(&total_hash, &compiled_bytes)
            .await
            .unwrap();
    }

    let should_upload = {
        let default_config = Config::default();

        !did_load_from_local_cache
            && !did_load_from_clickhouse
            && config.user != default_config.user
    };

    if should_upload {
        let mut tries = 3;
        loop {
            let upload_result = clickhouse_disk
                .write(
                    &compiler_version,
                    compiler_cmdline.clone(),
                    compiler.get_args(),
                    compiler.get_compile_duration(),
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
