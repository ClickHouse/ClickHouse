set(RDKAFKA_SOURCE_DIR ${CMAKE_SOURCE_DIR}/contrib/librdkafka/src)

set(SRCS
${RDKAFKA_SOURCE_DIR}/crc32c.c
${RDKAFKA_SOURCE_DIR}/rdaddr.c
${RDKAFKA_SOURCE_DIR}/rdavl.c
${RDKAFKA_SOURCE_DIR}/rdbuf.c
${RDKAFKA_SOURCE_DIR}/rdcrc32.c
${RDKAFKA_SOURCE_DIR}/rdkafka.c
${RDKAFKA_SOURCE_DIR}/rdkafka_assignor.c
${RDKAFKA_SOURCE_DIR}/rdkafka_broker.c
${RDKAFKA_SOURCE_DIR}/rdkafka_buf.c
${RDKAFKA_SOURCE_DIR}/rdkafka_cgrp.c
${RDKAFKA_SOURCE_DIR}/rdkafka_conf.c
${RDKAFKA_SOURCE_DIR}/rdkafka_event.c
${RDKAFKA_SOURCE_DIR}/rdkafka_feature.c
${RDKAFKA_SOURCE_DIR}/rdkafka_lz4.c
${RDKAFKA_SOURCE_DIR}/rdkafka_metadata.c
${RDKAFKA_SOURCE_DIR}/rdkafka_metadata_cache.c
${RDKAFKA_SOURCE_DIR}/rdkafka_msg.c
${RDKAFKA_SOURCE_DIR}/rdkafka_msgset_reader.c
${RDKAFKA_SOURCE_DIR}/rdkafka_msgset_writer.c
${RDKAFKA_SOURCE_DIR}/rdkafka_offset.c
${RDKAFKA_SOURCE_DIR}/rdkafka_op.c
${RDKAFKA_SOURCE_DIR}/rdkafka_partition.c
${RDKAFKA_SOURCE_DIR}/rdkafka_pattern.c
${RDKAFKA_SOURCE_DIR}/rdkafka_queue.c
${RDKAFKA_SOURCE_DIR}/rdkafka_range_assignor.c
${RDKAFKA_SOURCE_DIR}/rdkafka_request.c
${RDKAFKA_SOURCE_DIR}/rdkafka_roundrobin_assignor.c
${RDKAFKA_SOURCE_DIR}/rdkafka_sasl.c
${RDKAFKA_SOURCE_DIR}/rdkafka_sasl_plain.c
${RDKAFKA_SOURCE_DIR}/rdkafka_subscription.c
${RDKAFKA_SOURCE_DIR}/rdkafka_timer.c
${RDKAFKA_SOURCE_DIR}/rdkafka_topic.c
${RDKAFKA_SOURCE_DIR}/rdkafka_transport.c
${RDKAFKA_SOURCE_DIR}/rdkafka_interceptor.c
${RDKAFKA_SOURCE_DIR}/rdkafka_header.c
${RDKAFKA_SOURCE_DIR}/rdlist.c
${RDKAFKA_SOURCE_DIR}/rdlog.c
${RDKAFKA_SOURCE_DIR}/rdmurmur2.c
${RDKAFKA_SOURCE_DIR}/rdports.c
${RDKAFKA_SOURCE_DIR}/rdrand.c
${RDKAFKA_SOURCE_DIR}/rdregex.c
${RDKAFKA_SOURCE_DIR}/rdstring.c
${RDKAFKA_SOURCE_DIR}/rdunittest.c
${RDKAFKA_SOURCE_DIR}/rdvarint.c
${RDKAFKA_SOURCE_DIR}/snappy.c
${RDKAFKA_SOURCE_DIR}/tinycthread.c
${RDKAFKA_SOURCE_DIR}/xxhash.c
${RDKAFKA_SOURCE_DIR}/lz4.c
${RDKAFKA_SOURCE_DIR}/lz4frame.c
${RDKAFKA_SOURCE_DIR}/lz4hc.c
${RDKAFKA_SOURCE_DIR}/rdgz.c
)

add_library(rdkafka STATIC ${SRCS})
target_include_directories(rdkafka PRIVATE include)
target_include_directories(rdkafka SYSTEM PUBLIC ${RDKAFKA_SOURCE_DIR})
target_link_libraries(rdkafka PUBLIC ${ZLIB_LIBRARIES} ${OPENSSL_SSL_LIBRARY} ${OPENSSL_CRYPTO_LIBRARY})
