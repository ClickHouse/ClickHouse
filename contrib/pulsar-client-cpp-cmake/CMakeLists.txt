option(ENABLE_PULSAR "Enable Pulsar" ${ENABLE_LIBRARIES})

if(NOT ENABLE_PULSAR)
    message(STATUS "Not using Pulsar")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/pulsar-client-cpp")

# Generated files (protobuf code and Version.h) are placed into the binary
# directory to keep the submodule working tree clean.
set(PULSAR_GENERATED_DIR "${CMAKE_CURRENT_BINARY_DIR}/generated")

set(PROTO_DIR "${LIBRARY_DIR}/proto")

# Use the native protoc when cross-compiling or using musl - the freshly built
# protoc targets the target platform and cannot run on the host
# (the same rule as in protobuf_generate.cmake).
if (NOT CMAKE_HOST_SYSTEM_NAME STREQUAL CMAKE_SYSTEM_NAME OR NOT CMAKE_HOST_SYSTEM_PROCESSOR STREQUAL CMAKE_SYSTEM_PROCESSOR OR USE_MUSL)
    set(NATIVE_protoc "${PROJECT_BINARY_DIR}/native/contrib/google-protobuf-cmake/protoc")
else()
    set(NATIVE_protoc $<TARGET_FILE:protoc>)
endif()

add_custom_command(
    OUTPUT "${PULSAR_GENERATED_DIR}/lib/PulsarApi.pb.h" "${PULSAR_GENERATED_DIR}/lib/PulsarApi.pb.cc"
    COMMAND ${CMAKE_COMMAND} -E make_directory "${PULSAR_GENERATED_DIR}/lib"
    COMMAND ${NATIVE_protoc} -I "${PROTO_DIR}" --cpp_out="${PULSAR_GENERATED_DIR}/lib" "${PROTO_DIR}/PulsarApi.proto"
    DEPENDS ${NATIVE_protoc} "${PROTO_DIR}/PulsarApi.proto"
    COMMENT "Generating Pulsar protobuf sources")

file(READ "${LIBRARY_DIR}/version.txt" PULSAR_CLIENT_VERSION)
string(STRIP "${PULSAR_CLIENT_VERSION}" PULSAR_CLIENT_VERSION)
# The same value as produced by build-support/gen-pulsar-version-macro.py: major * 1000000 + minor * 1000 + patch.
string(REGEX MATCH "^([0-9]+)\\.([0-9]+)\\.([0-9]+)" _pulsar_version_match "${PULSAR_CLIENT_VERSION}")
math(EXPR PULSAR_CLIENT_VERSION_MACRO "${CMAKE_MATCH_1} * 1000000 + ${CMAKE_MATCH_2} * 1000 + ${CMAKE_MATCH_3}")
set(PVM_COMMENT "This is generated from Version.h.in by CMAKE. DO NOT EDIT DIRECTLY")
configure_file("${LIBRARY_DIR}/templates/Version.h.in" "${PULSAR_GENERATED_DIR}/include/pulsar/Version.h" @ONLY)

file(GLOB_RECURSE SRCS "${LIBRARY_DIR}/lib/*.cc")
list(APPEND SRCS "${PULSAR_GENERATED_DIR}/lib/PulsarApi.pb.cc")

# The vendored protobuf returns absl::string_view from descriptor accessors,
# so this file is compiled from a patched copy.
list(REMOVE_ITEM SRCS "${LIBRARY_DIR}/lib/ProtobufNativeSchema.cc")
list(APPEND SRCS "${CMAKE_CURRENT_SOURCE_DIR}/patched/ProtobufNativeSchema.cc")

# Use the LZ4 library bundled with ClickHouse instead of the copy vendored
# inside pulsar-client-cpp, so that only one LZ4 implementation is linked into
# the binary. The codec source is compiled from a patched copy that includes
# the contrib <lz4.h>.
list(REMOVE_ITEM SRCS "${LIBRARY_DIR}/lib/lz4/lz4.cc")
list(REMOVE_ITEM SRCS "${LIBRARY_DIR}/lib/CompressionCodecLZ4.cc")
list(APPEND SRCS "${CMAKE_CURRENT_SOURCE_DIR}/patched/CompressionCodecLZ4.cc")

# The SSE4.2/PCLMUL CRC32C implementation is dispatched at runtime via cpuid
# (crc32c_initialize), so it is safe to compile this single file with the
# required target features even when the build target (e.g. the compat build)
# does not enable them globally. Upstream pulsar-client-cpp passes the same
# flags for x86_64.
if (ARCH_AMD64)
    set_source_files_properties("${LIBRARY_DIR}/lib/checksum/crc32c_sse42.cc" PROPERTIES COMPILE_OPTIONS "-msse4.2;-mpclmul")
endif()

# Make sure the protobuf sources are generated before any library source is compiled.
set_property(SOURCE ${SRCS} APPEND PROPERTY OBJECT_DEPENDS "${PULSAR_GENERATED_DIR}/lib/PulsarApi.pb.h")

add_library(_pulsar ${SRCS})

target_compile_definitions(_pulsar PRIVATE PULSAR_USE_BOOST_REGEX)

# Upstream defines these alongside the zstd/snappy dependencies; without them
# CompressionCodecZstd.cc and CompressionCodecSnappy.cc compile throw-only stubs
# and messages produced with these codecs cannot be read.
target_compile_definitions(_pulsar PRIVATE HAS_ZSTD HAS_SNAPPY)

# ClickHouse does not have curl (the submodule was removed), so a minimal stub is
# provided instead. The Pulsar binary protocol (pulsar:// URLs) works normally;
# HTTP lookup (http:// URLs) and OAuth2 authentication fail at runtime.
target_include_directories(_pulsar SYSTEM PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/curl-stub")

# Compatibility shims for the deprecated Boost.Asio APIs that were removed in
# Boost >= 1.87 but are still used by pulsar-client-cpp (io_service, buffer
# typedefs, resolver query/iterator, timer expires_from_now). This directory
# must come before the Boost include directory in the search order.
target_include_directories(_pulsar SYSTEM PRIVATE "${CMAKE_CURRENT_SOURCE_DIR}/asio-compat")

target_include_directories(_pulsar SYSTEM PRIVATE "${PULSAR_GENERATED_DIR}/lib" "${LIBRARY_DIR}/lib" "${LIBRARY_DIR}")
target_include_directories(_pulsar SYSTEM PUBLIC "${PULSAR_GENERATED_DIR}/include" "${LIBRARY_DIR}/include")

target_link_libraries(_pulsar
  PUBLIC
    boost::headers_only
    boost::system
    boost::filesystem
    ch_contrib::zlib
    ch_contrib::zstd
    ch_contrib::lz4
    OpenSSL::Crypto
    OpenSSL::SSL
    ch_contrib::snappy
    ch_contrib::protobuf
)

add_library(ch_contrib::pulsar ALIAS _pulsar)
