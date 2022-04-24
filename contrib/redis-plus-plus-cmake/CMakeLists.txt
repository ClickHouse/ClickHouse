set (SRC_DIR "${ClickHouse_SOURCE_DIR}/contrib/redis-plus-plus/src/sw")

set(SRCS
        "${SRC_DIR}/redis++/command.cpp"
        "${SRC_DIR}/redis++/command_options.cpp"
        "${SRC_DIR}/redis++/connection.cpp"
        "${SRC_DIR}/redis++/connection_pool.cpp"
        "${SRC_DIR}/redis++/crc16.cpp"
        "${SRC_DIR}/redis++/errors.cpp"
        "${SRC_DIR}/redis++/pipeline.cpp"
        "${SRC_DIR}/redis++/redis.cpp"
        "${SRC_DIR}/redis++/redis_cluster.cpp"
        "${SRC_DIR}/redis++/reply.cpp"
        "${SRC_DIR}/redis++/sentinel.cpp"
        "${SRC_DIR}/redis++/shards.cpp"
        "${SRC_DIR}/redis++/shards_pool.cpp"
        "${SRC_DIR}/redis++/subscriber.cpp"
        )

add_library(_sw ${SRCS})
add_library(ch_contrib::sw ALIAS _sw)
target_include_directories(_sw SYSTEM BEFORE PUBLIC ${SRC_DIR})
target_include_directories(_sw SYSTEM BEFORE PUBLIC ${SRC_DIR}/redis++/cxx17)
target_include_directories(_sw SYSTEM BEFORE PUBLIC ${SRC_DIR}/redis++/no_tls)
target_link_libraries(_sw PRIVATE ch_contrib::hiredis boost::headers_only)