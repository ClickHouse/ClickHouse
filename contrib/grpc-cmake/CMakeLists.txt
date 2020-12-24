set(_gRPC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/grpc")
set(_gRPC_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/grpc")

if(NOT RE2_INCLUDE_DIR)
  message(FATAL_ERROR " grpc: The location of the \"re2\" library is unknown")
endif()
set(gRPC_RE2_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_RE2_INCLUDE_DIR "${RE2_INCLUDE_DIR}")
set(_gRPC_RE2_LIBRARIES "${RE2_LIBRARY}")

if(NOT ZLIB_INCLUDE_DIRS)
  message(FATAL_ERROR " grpc: The location of the \"zlib\" library is unknown")
endif()
set(gRPC_ZLIB_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_ZLIB_INCLUDE_DIR "${ZLIB_INCLUDE_DIRS}")
set(_gRPC_ZLIB_LIBRARIES "${ZLIB_LIBRARIES}")

if(NOT Protobuf_INCLUDE_DIR OR NOT Protobuf_LIBRARY)
  message(FATAL_ERROR " grpc: The location of the \"protobuf\" library is unknown")
elseif (NOT Protobuf_PROTOC_EXECUTABLE)
  message(FATAL_ERROR " grpc: The location of the protobuf compiler is unknown")
elseif (NOT Protobuf_PROTOC_LIBRARY)
  message(FATAL_ERROR " grpc: The location of the protobuf compiler's library is unknown")
endif()
set(gRPC_PROTOBUF_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_PROTOBUF_WELLKNOWN_INCLUDE_DIR "${Protobuf_INCLUDE_DIR}")
set(_gRPC_PROTOBUF_LIBRARIES "${Protobuf_LIBRARY}")
set(_gRPC_PROTOBUF_PROTOC "protoc")
set(_gRPC_PROTOBUF_PROTOC_EXECUTABLE "${Protobuf_PROTOC_EXECUTABLE}")
set(_gRPC_PROTOBUF_PROTOC_LIBRARIES "${Protobuf_PROTOC_LIBRARY}")

set(gRPC_SSL_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_SSL_INCLUDE_DIR ${OPENSSL_INCLUDE_DIR})
set(_gRPC_SSL_LIBRARIES ${OPENSSL_LIBRARIES})

# We don't want to build C# extensions.
set(gRPC_BUILD_CSHARP_EXT OFF)

# We don't want to build abseil tests, so we temporarily switch BUILD_TESTING off.
set(_gRPC_ORIG_BUILD_TESTING ${BUILD_TESTING})
set(BUILD_TESTING OFF)

add_subdirectory("${_gRPC_SOURCE_DIR}" "${_gRPC_BINARY_DIR}")

set(BUILD_TESTING ${_gRPC_ORIG_BUILD_TESTING})

# The contrib/grpc/CMakeLists.txt redefined the PROTOBUF_GENERATE_GRPC_CPP() function for its own purposes,
# so we need to redefine it back.
include("${ClickHouse_SOURCE_DIR}/contrib/grpc-cmake/protobuf_generate_grpc.cmake")
