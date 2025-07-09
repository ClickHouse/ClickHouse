set(CRC32C_DIR ${ClickHouse_SOURCE_DIR}/contrib/crc32c)

configure_file(
  "${CRC32C_DIR}/src/crc32c_config.h.in"
  "${PROJECT_BINARY_DIR}/include/crc32c/crc32c_config.h"
)

include_directories("${PROJECT_BINARY_DIR}/include")

set(CRC32C_SOURCES
    "${CRC32C_DIR}/src/crc32c_internal.h"
    "${CRC32C_DIR}/src/crc32c_portable.cc"
    "${CRC32C_DIR}/src/crc32c_prefetch.h"
    "${CRC32C_DIR}/src/crc32c_read_le.h"
    "${CRC32C_DIR}/src/crc32c_round_up.h"
    "${CRC32C_DIR}/src/crc32c.cc"
)

set(CRC32C_FLAGS)

if (ARCH_AARCH64)
  set(CRC32C_FLAGS -DHAVE_ARM64_CRC32C)
  list(APPEND CRC32C_SOURCES "${CRC32C_DIR}/src/crc32c_arm64.cc")
endif()

if (ENABLE_SSE42)
  set(CRC32C_FLAGS -DHAVE_SSE42)
  list(APPEND CRC32C_SOURCES "${CRC32C_DIR}/src/crc32c_sse42.cc")
endif()

add_library(_crc32c ${CRC32C_SOURCES})
target_compile_options(_crc32c PRIVATE ${CRC32C_FLAGS})

target_include_directories(_crc32c
  PUBLIC
    $<BUILD_INTERFACE:${CRC32C_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

add_library(ch_contrib::crc32c ALIAS _crc32c)
