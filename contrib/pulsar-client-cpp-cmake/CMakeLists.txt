option(ENABLE_PULSAR "Enable Pulsar" ${ENABLE_LIBRARIES})

if(NOT ENABLE_PULSAR)
    message(STATUS "Not using Pulsar")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/pulsar-client-cpp")

set(PROTOBUF_EXECUTABLE $<TARGET_FILE:protoc>)
set(PROTO_DIR "${LIBRARY_DIR}/proto")

add_custom_command(OUTPUT ${LIBRARY_DIR}/lib/PulsarApi.pb.h ${LIBRARY_DIR}/lib/PulsarApi.pb.cc
        COMMAND ${PROTOBUF_EXECUTABLE}
        -I ${PROTO_DIR}
        --cpp_out="${LIBRARY_DIR}/lib"
        "${PROTO_DIR}/PulsarApi.proto")


configure_file(${LIBRARY_DIR}/templates/Version.h.in ${LIBRARY_DIR}/include/pulsar/Version.h @ONLY)

add_definitions(-DPULSAR_USE_BOOST_REGEX)

file(GLOB_RECURSE SRCS 
    "${LIBRARY_DIR}/lib/*.cc"
    "${LIBRARY_DIR}/lib/*.h"
    DEPENDS ${LIBRARY_DIR}/lib/PulsarApi.pb.h ${LIBRARY_DIR}/lib/PulsarApi.pb.cc
)

add_library(_pulsar ${SRCS})

add_dependencies(_pulsar protoc)

target_include_directories(_pulsar SYSTEM PRIVATE "${LIBRARY_DIR}/lib" "${LIBRARY_DIR}")
target_include_directories(_pulsar SYSTEM PUBLIC "${LIBRARY_DIR}/include")

target_link_libraries(_pulsar
  PUBLIC
    boost::headers_only
    boost::system
    boost::filesystem
    ch_contrib::zlib
    ch_contrib::zstd
    OpenSSL::Crypto
    OpenSSL::SSL
    ch_contrib::curl
    ch_contrib::snappy
    ch_contrib::protobuf
)

add_library(ch_contrib::pulsar ALIAS _pulsar)