set (SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/redis-plus-plus/src/sw")

set(SRCS
        "${SRC_DIR}/redis++/async_connection.cpp"
        "${SRC_DIR}/redis++/async_connection_pool.cpp"
        "${SRC_DIR}/redis++/async_redis.cpp"
        "${SRC_DIR}/redis++/async_redis_cluster.cpp"
        "${SRC_DIR}/redis++/async_sentinel.cpp"
        "${SRC_DIR}/redis++/async_shards_pool.cpp"
        "${SRC_DIR}/redis++/command.cpp"
        "${SRC_DIR}/redis++/command_options.cpp"
        "${SRC_DIR}/redis++/connection.cpp"
        "${SRC_DIR}/redis++/connection_pool.cpp"
        "${SRC_DIR}/redis++/crc16.cpp"
        "${SRC_DIR}/redis++/errors.cpp"
        "${SRC_DIR}/redis++/event_loop.cpp"
        "${SRC_DIR}/redis++/pipeline.cpp"
        "${SRC_DIR}/redis++/redis.cpp"
        "${SRC_DIR}/redis++/redis_cluster.cpp"
        "${SRC_DIR}/redis++/reply.cpp"
        "${SRC_DIR}/redis++/sentinel.cpp"
        "${SRC_DIR}/redis++/shards.cpp"
        "${SRC_DIR}/redis++/shards_pool.cpp"
        "${SRC_DIR}/redis++/subscriber.cpp"
        "${SRC_DIR}/redis++/connection.cpp"
        "${SRC_DIR}/redis++/tls/tls.cpp"
        )

add_library(sw ${SRCS})
target_include_directories(sw SYSTEM PUBLIC ${SRC_DIR})
target_link_libraries(sw PRIVATE hiredis)