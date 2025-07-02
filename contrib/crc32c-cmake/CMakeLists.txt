set(CRC32C_DIR ${ClickHouse_SOURCE_DIR}/contrib/crc32c)

configure_file(
  "${CRC32C_DIR}/src/crc32c_config.h.in"
  "${PROJECT_BINARY_DIR}/include/crc32c/crc32c_config.h"
)

include_directories("${PROJECT_BINARY_DIR}/include")

# ARM64 CRC32C code is built separately, so we don't accidentally compile
# unsupported instructions into code that gets run without ARM32 support.
add_library(crc32c_arm64 OBJECT "")
target_sources(crc32c_arm64
  PRIVATE
    "${PROJECT_BINARY_DIR}/include/crc32c/crc32c_config.h"
    "${CRC32C_DIR}/src/crc32c_arm64.cc"
    "${CRC32C_DIR}/src/crc32c_arm64.h"
)

# CMake only enables PIC by default in SHARED and MODULE targets.
if(BUILD_SHARED_LIBS)
  set_property(TARGET crc32c_arm64 PROPERTY POSITION_INDEPENDENT_CODE TRUE)
endif(BUILD_SHARED_LIBS)

# SSE4.2 code is built separately, so we don't accidentally compile unsupported
# instructions into code that gets run without SSE4.2 support.
add_library(crc32c_sse42 OBJECT "")
target_sources(crc32c_sse42
  PRIVATE
    "${PROJECT_BINARY_DIR}/include/crc32c/crc32c_config.h"
    "${CRC32C_DIR}/src/crc32c_sse42.cc"
    "${CRC32C_DIR}/src/crc32c_sse42.h"
)

# CMake only enables PIC by default in SHARED and MODULE targets.
if(BUILD_SHARED_LIBS)
  set_property(TARGET crc32c_sse42 PROPERTY POSITION_INDEPENDENT_CODE TRUE)
endif(BUILD_SHARED_LIBS)

add_library(_crc32c ""
  # TODO(pwnall): Move the TARGET_OBJECTS generator expressions to the PRIVATE
  # section of target_sources when cmake_minimum_required becomes 3.9 or above.
  $<TARGET_OBJECTS:crc32c_arm64>
  $<TARGET_OBJECTS:crc32c_sse42>
)
target_sources(_crc32c
  PRIVATE
    "${PROJECT_BINARY_DIR}/include/crc32c/crc32c_config.h"
    "${CRC32C_DIR}/src/crc32c_internal.h"
    "${CRC32C_DIR}/src/crc32c_portable.cc"
    "${CRC32C_DIR}/src/crc32c_prefetch.h"
    "${CRC32C_DIR}/src/crc32c_read_le.h"
    "${CRC32C_DIR}/src/crc32c_round_up.h"
    "${CRC32C_DIR}/src/crc32c.cc"
)

target_include_directories(_crc32c
  PUBLIC
    $<BUILD_INTERFACE:${CRC32C_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(_crc32c
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

add_library(ch_contrib::crc32c ALIAS _crc32c)
