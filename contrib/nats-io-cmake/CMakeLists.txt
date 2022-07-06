option (ENABLE_NATS "Enable NATS" ${ENABLE_LIBRARIES})

if (OS_FREEBSD)
    set(ENABLE_NATS OFF)
    message (STATUS "Using internal nats-io library on FreeBSD is not supported")
endif()

if (NOT ENABLE_NATS)
    message(STATUS "Not using nats-io")
    return()
endif()

set(NATS_IO_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/nats-io/src")

if(UNIX)
    set(NATS_PLATFORM_INCLUDE "unix")
elseif(WIN32)
    set(NATS_PLATFORM_INCLUDE "apple")
endif()

file(GLOB PS_SOURCES "${NATS_IO_SOURCE_DIR}/${NATS_PLATFORM_INCLUDE}/*.c")
set(SRCS
    "${NATS_IO_SOURCE_DIR}/asynccb.c"
    "${NATS_IO_SOURCE_DIR}/buf.c"
    "${NATS_IO_SOURCE_DIR}/comsock.c"
    "${NATS_IO_SOURCE_DIR}/conn.c"
    "${NATS_IO_SOURCE_DIR}/crypto.c"
    "${NATS_IO_SOURCE_DIR}/hash.c"
    "${NATS_IO_SOURCE_DIR}/js.c"
    "${NATS_IO_SOURCE_DIR}/jsm.c"
    "${NATS_IO_SOURCE_DIR}/kv.c"
    "${NATS_IO_SOURCE_DIR}/msg.c"
    "${NATS_IO_SOURCE_DIR}/nats.c"
    "${NATS_IO_SOURCE_DIR}/natstime.c"
    "${NATS_IO_SOURCE_DIR}/nkeys.c"
    "${NATS_IO_SOURCE_DIR}/nuid.c"
    "${NATS_IO_SOURCE_DIR}/opts.c"
    "${NATS_IO_SOURCE_DIR}/parser.c"
    "${NATS_IO_SOURCE_DIR}/pub.c"
    "${NATS_IO_SOURCE_DIR}/srvpool.c"
    "${NATS_IO_SOURCE_DIR}/stats.c"
    "${NATS_IO_SOURCE_DIR}/status.c"
    "${NATS_IO_SOURCE_DIR}/sub.c"
    "${NATS_IO_SOURCE_DIR}/timer.c"
    "${NATS_IO_SOURCE_DIR}/url.c"
    "${NATS_IO_SOURCE_DIR}/util.c"
)

add_library(_nats_io ${SRCS} ${PS_SOURCES})
add_library(ch_contrib::nats_io ALIAS _nats_io)

target_include_directories(_nats_io SYSTEM PUBLIC ${NATS_IO_SOURCE_DIR})
target_include_directories(_nats_io SYSTEM PUBLIC ${NATS_IO_SOURCE_DIR}/adapters)
target_include_directories(_nats_io SYSTEM PUBLIC ${NATS_IO_SOURCE_DIR}/include)
target_include_directories(_nats_io SYSTEM PUBLIC ${NATS_IO_SOURCE_DIR}/${NATS_PLATFORM_INCLUDE})

target_link_libraries(_nats_io
        PRIVATE OpenSSL::Crypto OpenSSL::SSL ch_contrib::uv
)
