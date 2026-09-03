option(ENABLE_WASMTIME "Enable WasmTime engine" ${ENABLE_LIBRARIES})

# MSAN: Disabled due to false-positives in WasmTime
# We tried to do __msan_unpoison memory coming from jit generated code,
# however sometimes sanitizer trap happened without entering code on our side,
# e.g. while handling wasm traps.
if (NOT ENABLE_WASMTIME OR NOT ENABLE_RUST OR (SANITIZE STREQUAL "memory"))
    message (STATUS "Not using wasmtime")
    return()
endif()

if (OS_FREEBSD OR NO_ARMV81_OR_HIGHER OR ARCH_S390X)
    message (STATUS "Unsupported architecture for WasmTime")
    return()
endif()

set (WASMTIME_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/wasmtime")
set (WASMTIME_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/wasmtime")

add_library(_ch_wasmtime INTERFACE)

set(WASMTIME_FEATURE_WAT ON)
set(WASMTIME_FEATURE_COREDUMP ON)
set(WASMTIME_FEATURE_ADDR2LINE ON)
set(WASMTIME_FEATURE_DEMANGLE ON)
set(WASMTIME_FEATURE_CRANELIFT ON)
set(WASMTIME_FEATURE_DEBUG_BUILTINS ON)
set(WASMTIME_FEATURE_GC ON)
set(WASMTIME_FEATURE_GC_NULL ON)

set(WASMTIME_FEATURE_PROFILING OFF)
set(WASMTIME_FEATURE_CACHE OFF)
set(WASMTIME_FEATURE_PARALLEL_COMPILATION OFF)
set(WASMTIME_FEATURE_WASI OFF)
set(WASMTIME_FEATURE_LOGGING OFF)
set(WASMTIME_FEATURE_DISABLE_LOGGING ON)
set(WASMTIME_FEATURE_THREADS OFF)
set(WASMTIME_FEATURE_GC_DRC OFF)
set(WASMTIME_FEATURE_ASYNC OFF)
set(WASMTIME_FEATURE_WINCH OFF)

set(WASMTIME_HEADER_DST ${WASMTIME_BINARY_DIR}/include)
set(WASMTIME_HEADER_SRC ${WASMTIME_SOURCE_DIR}/crates/c-api/include)
target_include_directories(_ch_wasmtime SYSTEM INTERFACE ${WASMTIME_HEADER_DST})

configure_file("${WASMTIME_HEADER_SRC}/wasmtime/conf.h.in" "${WASMTIME_HEADER_DST}/wasmtime/conf.h" NEWLINE_STYLE CRLF)
file(COPY "${WASMTIME_HEADER_SRC}/" DESTINATION "${WASMTIME_HEADER_DST}" FILES_MATCHING REGEX "\\.hh?$")

clickhouse_config_crate_flags(_ch_rust_wasmtime)

target_link_libraries(_ch_wasmtime INTERFACE _ch_rust_wasmtime)

add_library(ch_rust::wasmtime ALIAS _ch_wasmtime)
