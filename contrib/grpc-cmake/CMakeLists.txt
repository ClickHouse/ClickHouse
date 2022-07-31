# disable grpc due to conflicts of abseil (required by grpc) dynamic annotations with libtsan.a
if (SANITIZE STREQUAL "thread" AND COMPILER_GCC)
  set(ENABLE_GRPC_DEFAULT OFF)
else()
  set(ENABLE_GRPC_DEFAULT ${ENABLE_LIBRARIES})
endif()
option(ENABLE_GRPC "Use gRPC" ${ENABLE_GRPC_DEFAULT})

if(NOT ENABLE_GRPC)
  message(STATUS "Not using gRPC")
  return()
endif()

set(_gRPC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/grpc")
set(_gRPC_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/grpc")

# Use re2 from ClickHouse contrib, not from gRPC third_party.
set(gRPC_RE2_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_RE2_INCLUDE_DIR "")
set(_gRPC_RE2_LIBRARIES ch_contrib::re2)

# Use zlib from ClickHouse contrib, not from gRPC third_party.
set(gRPC_ZLIB_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_ZLIB_INCLUDE_DIR "")
set(_gRPC_ZLIB_LIBRARIES ch_contrib::zlib)

# Use protobuf from ClickHouse contrib, not from gRPC third_party.
set(gRPC_PROTOBUF_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_PROTOBUF_LIBRARIES ch_contrib::protobuf)
set(_gRPC_PROTOBUF_PROTOC "protoc")
set(_gRPC_PROTOBUF_PROTOC_EXECUTABLE $<TARGET_FILE:protoc>)
set(_gRPC_PROTOBUF_PROTOC_LIBRARIES ch_contrib::protoc)

if(TARGET OpenSSL::SSL)
  set(gRPC_USE_UNSECURE_LIBRARIES FALSE)
else()
  set(gRPC_USE_UNSECURE_LIBRARIES TRUE)
endif()

# Use OpenSSL from ClickHouse contrib, not from gRPC third_party.
set(gRPC_SSL_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_SSL_INCLUDE_DIR "")
set(_gRPC_SSL_LIBRARIES OpenSSL::Crypto OpenSSL::SSL)

# Use abseil-cpp from ClickHouse contrib, not from gRPC third_party.
set(gRPC_ABSL_PROVIDER "clickhouse" CACHE STRING "" FORCE)

# Choose to build static or shared library for c-ares.
if (USE_STATIC_LIBRARIES)
  set(CARES_STATIC ON CACHE BOOL "" FORCE)
  set(CARES_SHARED OFF CACHE BOOL "" FORCE)
else ()
  set(CARES_STATIC OFF CACHE BOOL "" FORCE)
  set(CARES_SHARED ON CACHE BOOL "" FORCE)
endif ()

# Disable looking for libnsl on a platforms that has gethostbyname in glibc
#
# c-ares searching for gethostbyname in the libnsl library, however in the
# version that shipped with gRPC it doing it wrong [1], since it uses
# CHECK_LIBRARY_EXISTS(), which will return TRUE even if the function exists in
# another dependent library. The upstream already contains correct macro [2],
# but it is not included in gRPC (even upstream gRPC, not the one that is
# shipped with clickhousee).
#
#   [1]: https://github.com/c-ares/c-ares/blob/e982924acee7f7313b4baa4ee5ec000c5e373c30/CMakeLists.txt#L125
#   [2]: https://github.com/c-ares/c-ares/blob/44fbc813685a1fa8aa3f27fcd7544faf612d376a/CMakeLists.txt#L146
#
# And because if you by some reason have libnsl [3] installed, clickhouse will
# reject to start w/o it. While this is completelly different library.
#
#   [3]: https://packages.debian.org/bullseye/libnsl2
if (NOT CMAKE_SYSTEM_NAME STREQUAL "SunOS")
  set(HAVE_LIBNSL OFF CACHE BOOL "" FORCE)
endif()

# We don't want to build C# extensions.
set(gRPC_BUILD_CSHARP_EXT OFF)

add_subdirectory("${_gRPC_SOURCE_DIR}" "${_gRPC_BINARY_DIR}")

# The contrib/grpc/CMakeLists.txt redefined the PROTOBUF_GENERATE_GRPC_CPP() function for its own purposes,
# so we need to redefine it back.
include("${ClickHouse_SOURCE_DIR}/contrib/grpc-cmake/protobuf_generate_grpc.cmake")

set(gRPC_CPP_PLUGIN $<TARGET_FILE:grpc_cpp_plugin>)
set(gRPC_PYTHON_PLUGIN $<TARGET_FILE:grpc_python_plugin>)

set(gRPC_INCLUDE_DIRS "${ClickHouse_SOURCE_DIR}/contrib/grpc/include")
if(gRPC_USE_UNSECURE_LIBRARIES)
  set(gRPC_LIBRARIES grpc_unsecure grpc++_unsecure)
else()
  set(gRPC_LIBRARIES grpc grpc++)
endif()
add_library(_ch_contrib_grpc INTERFACE)
target_link_libraries(_ch_contrib_grpc INTERFACE ${gRPC_LIBRARIES})
target_include_directories(_ch_contrib_grpc SYSTEM INTERFACE ${gRPC_INCLUDE_DIRS})
add_library(ch_contrib::grpc ALIAS _ch_contrib_grpc)
