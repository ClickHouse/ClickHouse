clickhouse_import_crate(MANIFEST_PATH Cargo.toml)
target_include_directories(_ch_rust_prql INTERFACE include)
add_library(ch_rust::prql ALIAS _ch_rust_prql)
