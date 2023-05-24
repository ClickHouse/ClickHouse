clickhouse_import_crate(MANIFEST_PATH Cargo.toml)

set(CXXBRIDGE_CXXFLAGS "-Wno-dollar-in-identifier-extension -Wno-unused-macros")
set(RUST_CXXFLAGS "${RUST_CXXFLAGS} ${CXXBRIDGE_CXXFLAGS}")

target_include_directories(_prql_library INTERFACE include)
add_library(ch_rust::prql_lib ALIAS _prql_library)
