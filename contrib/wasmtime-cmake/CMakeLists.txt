option(ENABLE_WASMTIME "Enable WasmTime engine" ${ENABLE_LIBRARIES})
if (NOT ENABLE_WASMTIME)
    message (STATUS "Not using wasmtime")
    return()
endif()

set (WASMTIME_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/wasmtime")
set (WASMTIME_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/wasmtime")

add_library(_ch_wasmtime INTERFACE)

set(WASMTIME_FEATURE_PROFILING ON)
set(WASMTIME_FEATURE_WAT ON)
set(WASMTIME_FEATURE_CACHE ON)
set(WASMTIME_FEATURE_PARALLEL_COMPILATION ON)
set(WASMTIME_FEATURE_WASI ON)
set(WASMTIME_FEATURE_LOGGING ON)
set(WASMTIME_FEATURE_DISABLE_LOGGING OFF)
set(WASMTIME_FEATURE_COREDUMP ON)
set(WASMTIME_FEATURE_ADDR2LINE ON)
set(WASMTIME_FEATURE_DEMANGLE ON)
set(WASMTIME_FEATURE_THREADS ON)
set(WASMTIME_FEATURE_GC ON)
set(WASMTIME_FEATURE_GC_DRC ON)
set(WASMTIME_FEATURE_GC_NULL ON)
set(WASMTIME_FEATURE_ASYNC ON)
set(WASMTIME_FEATURE_CRANELIFT ON)
set(WASMTIME_FEATURE_WINCH ON)
set(WASMTIME_FEATURE_DEBUG_BUILTINS ON)

set(WASMTIME_HEADER_DST ${WASMTIME_BINARY_DIR}/include)
set(WASMTIME_HEADER_SRC ${WASMTIME_SOURCE_DIR}/crates/c-api/include)
target_include_directories(_ch_wasmtime SYSTEM INTERFACE ${WASMTIME_HEADER_DST})

configure_file("${WASMTIME_HEADER_SRC}/wasmtime/conf.h.in" "${WASMTIME_HEADER_DST}/wasmtime/conf.h" NEWLINE_STYLE CRLF)
file(COPY "${WASMTIME_HEADER_SRC}/" DESTINATION "${WASMTIME_HEADER_DST}" FILES_MATCHING REGEX "\\.hh?$")

clickhouse_import_crate(
    MANIFEST_PATH "${WASMTIME_SOURCE_DIR}/crates/c-api/Cargo.toml"
    FEATURES "addr2line, async, cache, coredump, cranelift, debug-builtins, demangle, gc, gc-drc, gc-null, logging, parallel-compilation, profiling, threads, wasi, wat, winch"
)
clickhouse_config_crate_flags(wasmtime)

target_link_libraries(_ch_wasmtime INTERFACE wasmtime)
set_target_properties(_ch_wasmtime PROPERTIES
       INTERFACE_LINK_OPTIONS "-Wl,--allow-multiple-definition")

add_library(ch_contrib::wasmtime ALIAS _ch_wasmtime)
