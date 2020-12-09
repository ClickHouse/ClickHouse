set(_gRPC_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/grpc")
set(_gRPC_BINARY_DIR "${ClickHouse_BINARY_DIR}/contrib/grpc")

# Use re2 from ClickHouse contrib, not from gRPC third_party.
if(NOT RE2_INCLUDE_DIR)
  message(FATAL_ERROR " grpc: The location of the \"re2\" library is unknown")
endif()
set(gRPC_RE2_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_RE2_INCLUDE_DIR "${RE2_INCLUDE_DIR}")
set(_gRPC_RE2_LIBRARIES "${RE2_LIBRARY}")

# Use zlib from ClickHouse contrib, not from gRPC third_party.
if(NOT ZLIB_INCLUDE_DIRS)
  message(FATAL_ERROR " grpc: The location of the \"zlib\" library is unknown")
endif()
set(gRPC_ZLIB_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_ZLIB_INCLUDE_DIR "${ZLIB_INCLUDE_DIRS}")
set(_gRPC_ZLIB_LIBRARIES "${ZLIB_LIBRARIES}")

# Use protobuf from ClickHouse contrib, not from gRPC third_party.
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

# Use OpenSSL from ClickHouse contrib, not from gRPC third_party.
set(gRPC_SSL_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(_gRPC_SSL_INCLUDE_DIR ${OPENSSL_INCLUDE_DIR})
set(_gRPC_SSL_LIBRARIES ${OPENSSL_LIBRARIES})

# Use abseil-cpp from ClickHouse contrib, not from gRPC third_party.
set(gRPC_ABSL_PROVIDER "clickhouse" CACHE STRING "" FORCE)
set(ABSL_ROOT_DIR "${ClickHouse_SOURCE_DIR}/contrib/abseil-cpp")
if(NOT EXISTS "${ABSL_ROOT_DIR}/CMakeLists.txt")
  message(FATAL_ERROR " grpc: submodule third_party/abseil-cpp is missing. To fix try run: \n git submodule update --init --recursive")
endif()
add_subdirectory("${ABSL_ROOT_DIR}" "${ClickHouse_BINARY_DIR}/contrib/abseil-cpp")

# Choose to build static or shared library for c-ares.
if (MAKE_STATIC_LIBRARIES)
  set(CARES_STATIC ON CACHE BOOL "" FORCE)
  set(CARES_SHARED OFF CACHE BOOL "" FORCE)
else ()
  set(CARES_STATIC OFF CACHE BOOL "" FORCE)
  set(CARES_SHARED ON CACHE BOOL "" FORCE)
endif ()

# We don't want to build C# extensions.
set(gRPC_BUILD_CSHARP_EXT OFF)

add_subdirectory("${_gRPC_SOURCE_DIR}" "${_gRPC_BINARY_DIR}")

# The contrib/grpc/CMakeLists.txt redefined the PROTOBUF_GENERATE_GRPC_CPP() function for its own purposes,
# so we need to redefine it back.
include("${ClickHouse_SOURCE_DIR}/contrib/grpc-cmake/protobuf_generate_grpc.cmake")
