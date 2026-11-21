# `ENABLE_AVRO_RUST` is a dependent option declared in `contrib/CMakeLists.txt` once `ENABLE_AVRO` and `ENABLE_RUST` are known.
if (NOT ENABLE_AVRO_RUST)
    message(STATUS "Not using avro_rust")
    return()
endif()

# The entry points are the `avro_*` C functions declared in `include/avro_rust.h`. The crate is a member of the Rust
# workspace, whose one static library has its internal Rust symbols stripped (see `contrib/rust-contrib-workspace`).
clickhouse_add_rust_member(_ch_rust_avro FEATURE avro)
target_include_directories(_ch_rust_avro INTERFACE include)
add_library(ch_rust::avro ALIAS _ch_rust_avro)
