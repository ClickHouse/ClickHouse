set(CRC32C_DIR ${ClickHouse_SOURCE_DIR}/contrib/crc32c)
set(CRC32C_CMAKE_DIR ${ClickHouse_SOURCE_DIR}/contrib/crc32c-cmake)

include_directories("${PROJECT_BINARY_DIR}/include")

set(CRC32C_SOURCES
    "${CRC32C_DIR}/src/crc32c_internal.h"
    "${CRC32C_DIR}/src/crc32c_portable.cc"
    "${CRC32C_DIR}/src/crc32c_prefetch.h"
    "${CRC32C_DIR}/src/crc32c_read_le.h"
    "${CRC32C_DIR}/src/crc32c_round_up.h"
    "${CRC32C_DIR}/src/crc32c.cc"
)

if(ARCH_AARCH64)
  include_directories(${CRC32C_CMAKE_DIR}/aarch64)

  # crc32c_arm64.cc uses the vmull_p64 instruction, which is only available when building
  # with the `aes` flag.
  if (ENABLE_AES)
    list(APPEND CRC32C_SOURCES "${CRC32C_DIR}/src/crc32c_arm64.cc")
  endif()
elseif(ARCH_AMD64)
  if(ENABLE_SSE42)
    include_directories(${CRC32C_CMAKE_DIR}/x86_64/sse42)
    list(APPEND CRC32C_SOURCES "${CRC32C_DIR}/src/crc32c_sse42.cc")
  else()
    include_directories(${CRC32C_CMAKE_DIR}/x86_64/no_sse42)
  endif()
endif()

add_library(_crc32c ${CRC32C_SOURCES})

target_include_directories(_crc32c
  PUBLIC
    $<BUILD_INTERFACE:${CRC32C_DIR}/include>
)

add_library(ch_contrib::crc32c ALIAS _crc32c)
