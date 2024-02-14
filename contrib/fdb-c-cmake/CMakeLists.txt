set(FDB_C_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/fdb-c")
add_library(fdbdeps INTERFACE)
target_link_libraries(fdbdeps INTERFACE
    ch_contrib::fmt
    ch_contrib::msgpack
    OpenSSL::SSL
    OpenSSL::Crypto
    absl::debugging
    ch_contrib::re2
)
target_include_directories(fdbdeps INTERFACE "${ClickHouse_SOURCE_DIR}/contrib/boost")

# Some missing headers from contrib/sysroot/ have been added to the include/,
# which should be moved to the contrib/sysroot/ later.
# TODO: move these headers into contrib/sysroot/
if (CMAKE_C_COMPILER_TARGET STREQUAL "x86_64-linux-gnu")
    include_directories(${CMAKE_CURRENT_SOURCE_DIR}/${CMAKE_C_COMPILER_TARGET})
endif ()
add_compile_definitions(XXH_INLINE_ALL=1)

# Set cmake variables
if(SANITIZE STREQUAL "memory")
    set(USE_MSAN ON)
elseif(SANITIZE STREQUAL "address")
    set(USE_ASAN ON)
elseif(SANITIZE STREQUAL "thread")
    set(USE_TSAN ON)
elseif(SANITIZE STREQUAL "undefined")
    set(USE_UBSAN ON)
endif()

# Avoid harmful function name
add_definitions(-Dbasename=_FDB_basename)

add_subdirectory("${FDB_C_SOURCE_DIR}" "${ClickHouse_BINARY_DIR}/contrib/fdb-c")

# Custom fdb_c target
add_library(fdb_c_custom INTERFACE)
target_link_libraries(fdb_c_custom INTERFACE fdb_c)

# Set FDB_API_VERSION
set(FDB_API_VERSION 710)
target_compile_definitions(fdb_c_custom INTERFACE FDB_API_VERSION=${FDB_API_VERSION})

# Add useful headers
file(COPY "${FDB_C_SOURCE_DIR}/src/flow/error_definitions.h" DESTINATION "${CMAKE_CURRENT_BINARY_DIR}/include/foundationdb")
target_include_directories(fdb_c_custom INTERFACE "${CMAKE_CURRENT_BINARY_DIR}/include")

# Sanitizer interceptors
add_library(fdb_sanitizer_interceptors sanitizer_interceptors.cpp)
target_link_libraries(fdb_c_custom INTERFACE fdb_sanitizer_interceptors)

add_library(ch_contrib::fdb_c ALIAS fdb_c_custom)
