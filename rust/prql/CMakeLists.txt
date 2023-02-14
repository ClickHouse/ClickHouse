clickhouse_import_crate(MANIFEST_PATH Cargo.toml)
target_include_directories(_prql_library INTERFACE include)
add_library(ch_rust::prql_lib ALIAS _prql_library)
