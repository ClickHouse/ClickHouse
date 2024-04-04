configure_file("${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml.in" "${CMAKE_CURRENT_SOURCE_DIR}/.cargo/config.toml" @ONLY)
clickhouse_import_crate(MANIFEST_PATH Cargo.toml)
target_include_directories(_ch_rust_prql INTERFACE include)
add_library(ch_rust::prql ALIAS _ch_rust_prql)
