option (ENABLE_KAFKA "Enable kafka" ${ENABLE_LIBRARIES})

if (NOT ENABLE_KAFKA)
    message(STATUS "Not using librdkafka")
    return()
endif()

set(RDKAFKA_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/librdkafka/src")

set(SRCS
  "${RDKAFKA_SOURCE_DIR}/crc32c.c"
#  "${RDKAFKA_SOURCE_DIR}/lz4.c"
#  "${RDKAFKA_SOURCE_DIR}/lz4frame.c"
#  "${RDKAFKA_SOURCE_DIR}/lz4hc.c"
  "${RDKAFKA_SOURCE_DIR}/rdaddr.c"
  "${RDKAFKA_SOURCE_DIR}/rdavl.c"
  "${RDKAFKA_SOURCE_DIR}/rdbuf.c"
  "${RDKAFKA_SOURCE_DIR}/rdcrc32.c"
  "${RDKAFKA_SOURCE_DIR}/rddl.c"
  "${RDKAFKA_SOURCE_DIR}/rdfnv1a.c"
  "${RDKAFKA_SOURCE_DIR}/rdgz.c"
  "${RDKAFKA_SOURCE_DIR}/rdhdrhistogram.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_admin.c" # looks optional
  "${RDKAFKA_SOURCE_DIR}/rdkafka_assignment.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_assignor.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_aux.c" # looks optional
  "${RDKAFKA_SOURCE_DIR}/rdkafka_background.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_broker.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_buf.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_cert.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_cgrp.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_conf.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_coord.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_error.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_event.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_feature.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_header.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_idempotence.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_interceptor.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_lz4.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_metadata.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_metadata_cache.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_mock.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_mock_cgrp.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_mock_handlers.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_msg.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_msgset_reader.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_msgset_writer.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_offset.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_op.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_partition.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_pattern.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_plugin.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_queue.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_range_assignor.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_request.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_roundrobin_assignor.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl.c"
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_cyrus.c"        # optionally included below
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_oauthbearer.c"  # optionally included below
  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_plain.c"
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_scram.c"        # optionally included below
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_win32.c"
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_ssl.c"               # optionally included below
  "${RDKAFKA_SOURCE_DIR}/rdkafka_sticky_assignor.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_subscription.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_timer.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_topic.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_transport.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_txnmgr.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_zstd.c"
  "${RDKAFKA_SOURCE_DIR}/rdlist.c"
  "${RDKAFKA_SOURCE_DIR}/rdlog.c"
  "${RDKAFKA_SOURCE_DIR}/rdmap.c"
  "${RDKAFKA_SOURCE_DIR}/rdmurmur2.c"
  "${RDKAFKA_SOURCE_DIR}/rdports.c"
  "${RDKAFKA_SOURCE_DIR}/rdrand.c"
  "${RDKAFKA_SOURCE_DIR}/rdregex.c"
  "${RDKAFKA_SOURCE_DIR}/rdstring.c"
  "${RDKAFKA_SOURCE_DIR}/rdunittest.c"
  "${RDKAFKA_SOURCE_DIR}/rdvarint.c"
  "${RDKAFKA_SOURCE_DIR}/rdxxhash.c"
  # "${RDKAFKA_SOURCE_DIR}/regexp.c"
  "${RDKAFKA_SOURCE_DIR}/snappy.c"
  "${RDKAFKA_SOURCE_DIR}/tinycthread.c"
  "${RDKAFKA_SOURCE_DIR}/tinycthread_extra.c"
)

if(TARGET ch_contrib::sasl2)
    message (STATUS "librdkafka with SASL support")
    set(WITH_SASL_CYRUS 1)
endif()

message (STATUS "librdkafka with SSL support")
set(WITH_SSL 1)
if(WITH_SASL_CYRUS)
   set(WITH_SASL_SCRAM 1)
   set(WITH_SASL_OAUTHBEARER 1)
endif()
list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/rdkafka_ssl.c")

if(WITH_SASL_CYRUS)
  list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_cyrus.c") # needed to support Kerberos, requires cyrus-sasl
endif()

if(WITH_SASL_SCRAM)
  list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_scram.c")
endif()

if(WITH_SASL_OAUTHBEARER)
  list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_oauthbearer.c")
endif()

add_library(_rdkafka ${SRCS})
add_library(ch_contrib::rdkafka ALIAS _rdkafka)

target_compile_options(_rdkafka PRIVATE -fno-sanitize=undefined)
# target_include_directories(_rdkafka SYSTEM PUBLIC include)
target_include_directories(_rdkafka SYSTEM PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/include") # for "librdkafka/rdkafka.h"
target_include_directories(_rdkafka SYSTEM PUBLIC ${RDKAFKA_SOURCE_DIR})                 # Because weird logic with "include_next" is used.
target_include_directories(_rdkafka SYSTEM PUBLIC "${CMAKE_CURRENT_BINARY_DIR}/auxdir")  # for "../config.h"
target_link_libraries(_rdkafka
  PRIVATE
    ch_contrib::lz4
    ch_contrib::zlib
    ch_contrib::zstd
    OpenSSL::Crypto OpenSSL::SSL
)
if(WITH_SASL_CYRUS)
    target_link_libraries(_rdkafka PRIVATE ch_contrib::sasl2)
endif()

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/auxdir")

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/config.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/config.h"
  IMMEDIATE @ONLY
)
