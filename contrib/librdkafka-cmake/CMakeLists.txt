option (ENABLE_KAFKA "Enable kafka" ${ENABLE_LIBRARIES})

if (NOT ENABLE_KAFKA)
    message(STATUS "Not using librdkafka")
    return()
endif()

set(RDKAFKA_SOURCE_DIR "${ClickHouse_SOURCE_DIR}/contrib/librdkafka/src")

set(SRCS
#  "${RDKAFKA_SOURCE_DIR}/cJSON.c"    # see below
  "${RDKAFKA_SOURCE_DIR}/crc32c.c"
#  "${RDKAFKA_SOURCE_DIR}/lz4.c"      # WITH_LZ4_EXT
#  "${RDKAFKA_SOURCE_DIR}/lz4frame.c" # WITH_LZ4_EXT
#  "${RDKAFKA_SOURCE_DIR}/lz4hc.c"    # WITH_LZ4_EXT
  "${RDKAFKA_SOURCE_DIR}/rdaddr.c"
  "${RDKAFKA_SOURCE_DIR}/rdavl.c"
  "${RDKAFKA_SOURCE_DIR}/rdbase64.c"
  "${RDKAFKA_SOURCE_DIR}/rdbuf.c"
  "${RDKAFKA_SOURCE_DIR}/rdcrc32.c"
  #"${RDKAFKA_SOURCE_DIR}/rddl.c" #  WITH_LIBDL OR WIN32 - we don't want to support dynamic loading
  "${RDKAFKA_SOURCE_DIR}/rdfnv1a.c"
  "${RDKAFKA_SOURCE_DIR}/rdgz.c" # WITH_ZLIB
  #"${RDKAFKA_SOURCE_DIR}/rdhdrhistogram.c" # WITH_HDRHISTOGRAM - allows to collect some better stats (not used so far)
  "${RDKAFKA_SOURCE_DIR}/rdkafka_admin.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_assignment.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_assignor.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_aux.c"
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
  "${RDKAFKA_SOURCE_DIR}/rdkafka_fetcher.c"
  # "${RDKAFKA_SOURCE_DIR}/rdhttp.c" #  see WITH_CURL below
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
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_plugin.c" # WITH_PLUGINS, we don't support plugins (dynamic loading)
  "${RDKAFKA_SOURCE_DIR}/rdkafka_queue.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_range_assignor.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_request.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_roundrobin_assignor.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl.c"
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_cyrus.c"            # WITH_SASL_CYRUS, see below
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_oauthbearer.c"      # WITH_SASL_OAUTHBEARER, see below
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_oauthbearer_oidc.c" # WITH_OAUTHBEARER_OIDC, see below
  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_plain.c"
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_scram.c"     # WITH_SASL_SCRAM, see below
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_win32.c"     # WIN32
#  "${RDKAFKA_SOURCE_DIR}/rdkafka_ssl.c"            # WITH_SSL, see below
  "${RDKAFKA_SOURCE_DIR}/rdkafka_sticky_assignor.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_subscription.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_telemetry.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_telemetry_decode.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_telemetry_encode.c"
  "${RDKAFKA_SOURCE_DIR}/nanopb/pb_encode.c"
  "${RDKAFKA_SOURCE_DIR}/nanopb/pb_decode.c"
  "${RDKAFKA_SOURCE_DIR}/nanopb/pb_common.c"
  "${RDKAFKA_SOURCE_DIR}/opentelemetry/metrics.pb.c"
  "${RDKAFKA_SOURCE_DIR}/opentelemetry/common.pb.c"
  "${RDKAFKA_SOURCE_DIR}/opentelemetry/resource.pb.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_timer.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_topic.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_transport.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_txnmgr.c"
  "${RDKAFKA_SOURCE_DIR}/rdkafka_zstd.c" # WITH_ZSTD
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
  # "${RDKAFKA_SOURCE_DIR}/regexp.c" # NOT HAVE_REGEX
  "${RDKAFKA_SOURCE_DIR}/snappy.c"
  "${RDKAFKA_SOURCE_DIR}/tinycthread.c"
  "${RDKAFKA_SOURCE_DIR}/tinycthread_extra.c"
)

if(TARGET ch_contrib::curl)
    message (STATUS "librdkafka with curl")
    set(WITH_CURL 1)
endif()

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
add_definitions(-DOPENSSL_NO_ENGINE)
list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/rdkafka_ssl.c")

if(WITH_SSL AND WITH_CURL)
  set(WITH_OAUTHBEARER_OIDC 1)
  list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/rdhttp.c") # needed for WITH_OAUTHBEARER_OIDC
  list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_oauthbearer_oidc.c")
endif()

if(WITH_SASL_CYRUS)
  list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_cyrus.c") # needed to support Kerberos, requires cyrus-sasl
endif()

if(WITH_SASL_SCRAM)
  list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_scram.c")
endif()

if(WITH_SASL_OAUTHBEARER)
  list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_oauthbearer.c")
endif()

## there is a conflict with the cJSON library from aws-s3
# so normally we link cJSON lirary from contrib/aws (which have some extra patches to be thread safe)
# but if we don't use aws-s3, we need to link the cJSON library from librdkafka (hacky)
# which is (potentially) not thread safe (see https://github.com/confluentinc/librdkafka/issues/4159 ),
if(NOT ENABLE_AWS_S3)
   list(APPEND SRCS "${RDKAFKA_SOURCE_DIR}/cJSON.c")
endif()

add_library(_rdkafka ${SRCS})
add_library(ch_contrib::rdkafka ALIAS _rdkafka)
target_compile_options(_rdkafka PRIVATE -fno-sanitize=undefined)
target_compile_definitions(_rdkafka PRIVATE -DCJSON_HIDE_SYMBOLS)

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

if(WITH_CURL)
  target_link_libraries(_rdkafka PRIVATE ch_contrib::curl)
endif()

file(MAKE_DIRECTORY "${CMAKE_CURRENT_BINARY_DIR}/auxdir")

configure_file(
  "${CMAKE_CURRENT_SOURCE_DIR}/config.h.in"
  "${CMAKE_CURRENT_BINARY_DIR}/config.h"
  IMMEDIATE @ONLY
)

# -----------------------------------------------------------------------------
# Adjust to avoid the ClickHouse runtime trap for harmful functions:
#
# librdkafka uses the tinycthreads library for threading primitives.
# Note that tinycthreads is not actually using the C11 threads API,
# yet it defines and uses functions with the same names (e.g. mtx_lock,
# thrd_create, etc.). ClickHouse’s runtime trap monitors for these names
# and will immediately terminate the program if they are present.
#
# To avoid triggering this runtime trap, we replace these symbols via compile
# definitions. That is, we redefine each harmful function to an alias prefixed
# with "rd_kafka_". This ensures that none of the original harmful symbol
# names appear in the final binary.
target_compile_definitions(_rdkafka PRIVATE
  thrd_create=rd_kafka_thrd_create
  thrd_equal=rd_kafka_thrd_equal
  thrd_current=rd_kafka_thrd_current
  thrd_sleep=rd_kafka_thrd_sleep
  thrd_yield=rd_kafka_thrd_yield
  thrd_exit=rd_kafka_thrd_exit
  thrd_detach=rd_kafka_thrd_detach
  thrd_join=rd_kafka_thrd_join

  mtx_init=rd_kafka_mtx_init
  mtx_lock=rd_kafka_mtx_lock
  mtx_timedlock=rd_kafka_mtx_timedlock
  mtx_trylock=rd_kafka_mtx_trylock
  mtx_unlock=rd_kafka_mtx_unlock
  mtx_destroy=rd_kafka_mtx_destroy
  call_once=rd_kafka_call_once

  cnd_init=rd_kafka_cnd_init
  cnd_signal=rd_kafka_cnd_signal
  cnd_broadcast=rd_kafka_cnd_broadcast
  cnd_wait=rd_kafka_cnd_wait
  cnd_timedwait=rd_kafka_cnd_timedwait
  cnd_destroy=rd_kafka_cnd_destroy

  tss_create=rd_kafka_tss_create
  tss_get=rd_kafka_tss_get
  tss_set=rd_kafka_tss_set
  tss_delete=rd_kafka_tss_delete
)
