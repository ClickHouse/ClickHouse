option (ENABLE_DWARF_PARSER "Enable DWARF input format" ${ENABLE_LIBRARIES})

if (SANITIZE STREQUAL "memory" OR OS_DARWIN OR OS_FREEBSD OR OS_WASM)
    set (ENABLE_DWARF_PARSER OFF)
endif()

if (NOT ENABLE_DWARF_PARSER)
    message(STATUS "Not using dwarf")
    return()
endif()

# The crate is built into the one static library of the Rust workspace (`contrib/rust-contrib-workspace`), which also
# strips its internal Rust symbols and keeps the `ch_dwarf_*` C entry points.
clickhouse_add_rust_member(_ch_rust_dwarf FEATURE dwarf)
target_include_directories(_ch_rust_dwarf INTERFACE include)
add_library(ch_rust::dwarf ALIAS _ch_rust_dwarf)
