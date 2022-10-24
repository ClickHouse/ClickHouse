clickhouse_add_executable(tcp_protocol_fuzzer tcp_protocol_fuzzer.cpp  ../Server.cpp ../MetricsTransmitter.cpp)

set (TCP_PROTOCOL_FUZZER_LINK
    PRIVATE
        daemon
        clickhouse_aggregate_functions
        clickhouse_functions
        clickhouse_table_functions
        ch_contrib::fuzzer
        "-Wl,${WHOLE_ARCHIVE} $<TARGET_FILE:clickhouse_server_configs> -Wl,${NO_WHOLE_ARCHIVE}"
)

if (TARGET ch_contrib::jemalloc)
    list(APPEND TCP_PROTOCOL_FUZZER_LINK PRIVATE ch_contrib::jemalloc)
endif()

target_link_libraries(tcp_protocol_fuzzer ${TCP_PROTOCOL_FUZZER_LINK})

add_dependencies(tcp_protocol_fuzzer clickhouse_server_configs)
