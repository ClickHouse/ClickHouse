clickhouse_import_crate(MANIFEST_PATH Cargo.toml)
target_include_directories(_ch_rust_blake3 INTERFACE include)
add_library(ch_rust::blake3 ALIAS _ch_rust_blake3)
