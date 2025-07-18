option(ENABLE_WASMTIME "Enable WasmTime engine" ${ENABLE_LIBRARIES})
if (NOT ENABLE_WASMTIME)
    message (STATUS "Not using wasmtime")
    return()
endif()

set (WASMTIME_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/wasmtime")
set (WASMTIME_CPP_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/wasmtime-cpp")
set (WASMTIME_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/wasmtime")

add_library(_wasmtime_cpp INTERFACE)

add_subdirectory(
    "${WASMTIME_SOURCE_DIR}/crates/c-api"
    "${WASMTIME_BINARY_DIR}"
)
target_link_libraries(_wasmtime_cpp INTERFACE wasmtime)
target_include_directories(
    _wasmtime_cpp
    SYSTEM INTERFACE
    "${WASMTIME_SOURCE_DIR}/crates/c-api/include"
    "${WASMTIME_CPP_SOURCE_DIR}/include"
)

add_library(ch_contrib::wasmtime_cpp ALIAS _wasmtime_cpp)
