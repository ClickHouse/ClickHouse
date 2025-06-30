set(CRC32C_DIR ${ClickHouse_SOURCE_DIR}/contrib/crc32c)

if(CRC32C_USE_GLOG)
  # glog requires this setting to avoid using dynamic_cast.
  set(DISABLE_RTTI ON CACHE BOOL "" FORCE)

  # glog's test targets trigger deprecation warnings, and compiling them burns
  # CPU cycles on the CI.
  set(BUILD_TESTING_SAVED "${BUILD_TESTING}")
  set(BUILD_TESTING OFF CACHE BOOL "" FORCE)
  add_subdirectory("third_party/glog" EXCLUDE_FROM_ALL)
  set(BUILD_TESTING "${BUILD_TESTING_SAVED}" CACHE BOOL "" FORCE)

  # glog triggers deprecation warnings on OSX.
  # https://github.com/google/glog/issues/185
  if(CRC32C_HAVE_NO_DEPRECATED)
    set_property(TARGET glog APPEND PROPERTY COMPILE_OPTIONS -Wno-deprecated)
  endif(CRC32C_HAVE_NO_DEPRECATED)

  # glog triggers sign comparison warnings on gcc.
  if(CRC32C_HAVE_NO_SIGN_COMPARE)
    set_property(TARGET glog APPEND PROPERTY COMPILE_OPTIONS -Wno-sign-compare)
  endif(CRC32C_HAVE_NO_SIGN_COMPARE)

  # glog triggers unused parameter warnings on clang.
  if(CRC32C_HAVE_NO_UNUSED_PARAMETER)
    set_property(TARGET glog
                 APPEND PROPERTY COMPILE_OPTIONS -Wno-unused-parameter)
  endif(CRC32C_HAVE_NO_UNUSED_PARAMETER)

  set(CRC32C_TESTS_BUILT_WITH_GLOG 1)
endif(CRC32C_USE_GLOG)

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
if(HAVE_ARM64_CRC32C)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    # TODO(pwnall): Insert correct flag when VS gets ARM64 CRC32C support.
    target_compile_options(crc32c_arm64 PRIVATE "/arch:NOTYET")
  else(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    target_compile_options(crc32c_arm64 PRIVATE "-march=armv8-a+crc+crypto")
  endif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
endif(HAVE_ARM64_CRC32C)

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
if(HAVE_SSE42)
  if(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    target_compile_options(crc32c_sse42 PRIVATE "/arch:AVX")
  else(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    target_compile_options(crc32c_sse42 PRIVATE "-msse4.2")
  endif(CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
endif(HAVE_SSE42)

# CMake only enables PIC by default in SHARED and MODULE targets.
if(BUILD_SHARED_LIBS)
  set_property(TARGET crc32c_sse42 PROPERTY POSITION_INDEPENDENT_CODE TRUE)
endif(BUILD_SHARED_LIBS)

# Must be included before CMAKE_INSTALL_INCLUDEDIR is used.
include(GNUInstallDirs)

add_library(_crc32c ""
  # TODO(pwnall): Move the TARGET_OBJECTS generator expressions to the PRIVATE
  # section of target_sources when cmake_minimum_required becomes 3.9 or above.
  $<TARGET_OBJECTS:crc32c_arm64>
  $<TARGET_OBJECTS:crc32c_sse42>
)
target_sources(_crc32c
  PRIVATE
    "${PROJECT_BINARY_DIR}/include/crc32c/crc32c_config.h"
    "${CRC32C_DIR}/src/crc32c_arm64.h"
    "${CRC32C_DIR}/src/crc32c_arm64_check.h"
    "${CRC32C_DIR}/src/crc32c_internal.h"
    "${CRC32C_DIR}/src/crc32c_portable.cc"
    "${CRC32C_DIR}/src/crc32c_prefetch.h"
    "${CRC32C_DIR}/src/crc32c_read_le.h"
    "${CRC32C_DIR}/src/crc32c_round_up.h"
    "${CRC32C_DIR}/src/crc32c_sse42.h"
    "${CRC32C_DIR}/src/crc32c_sse42_check.h"
    "${CRC32C_DIR}/src/crc32c.cc"

  # Only CMake 3.3+ supports PUBLIC sources in targets exported by "install".
  $<$<VERSION_GREATER:CMAKE_VERSION,3.2>:PUBLIC>
    "${CRC32C_DIR}/include/crc32c/crc32c.h"
)

target_include_directories(_crc32c
  PUBLIC
    $<BUILD_INTERFACE:${CRC32C_DIR}/include>
    $<INSTALL_INTERFACE:${CMAKE_INSTALL_INCLUDEDIR}>
)

set_target_properties(_crc32c
  PROPERTIES VERSION ${PROJECT_VERSION} SOVERSION ${PROJECT_VERSION_MAJOR})

add_library(ch_contrib::crc32c ALIAS _crc32c)
