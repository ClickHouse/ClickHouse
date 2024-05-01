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
add_library(ch_contrib::pulsar ALIAS _pulsar)

target_link_libraries(_pulsar
  PRIVATE
    boost::system
    boost:filesystem
    ch_contrib::zlib
    ch_contrib::zstd
    OpenSSL::Crypto OpenSSL::SSL
    ch_contrib::curl
    ch_contrib::snappy
    
)

target_include_directories(_pulsar PRIVATE "${LIBRARY_DIR}/include")
target_include_directories(_pulsar SYSTEM BEFORE PUBLIC "${LIBRARY_DIR}/include/pulsar")
