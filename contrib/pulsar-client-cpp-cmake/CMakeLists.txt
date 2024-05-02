option(ENABLE_PULSAR "Enable Pulsar" ${ENABLE_LIBRARIES})

if(NOT ENABLE_PULSAR)
    message(STATUS "Not using Pulsar")
    return()
endif()

set(LIBRARY_DIR "${ClickHouse_SOURCE_DIR}/contrib/pulsar-client-cpp")

file(GLOB_RECURSE SRCS 
    "${LIBRARY_DIR}/lib/*.cc"
    "${LIBRARY_DIR}/lib/*.h"
)

add_library(_pulsar ${SRCS})

target_include_directories(_pulsar SYSTEM PUBLIC "${LIBRARY_DIR}/include" "${LIBRARY_DIR}/lib" "${LIBRARY_DIR}")

target_link_libraries(_pulsar
  PUBLIC
    boost::headers_only
    boost::system
    boost::filesystem
    ch_contrib::zlib
    ch_contrib::zstd
    OpenSSL::Crypto OpenSSL::SSL
    ch_contrib::curl
    ch_contrib::snappy
    ch_contrib::protobuf
)

add_library(ch_contrib::pulsar ALIAS _pulsar)