clickhouse_add_executable(tcp_protocol_fuzzer tcp_protocol_fuzzer.cpp  ../Server.cpp ../MetricsTransmitter.cpp)
target_include_directories (tcp_protocol_fuzzer PRIVATE ..)

set (TCP_PROTOCOL_FUZZER_LINK
    PRIVATE
        daemon
        clickhouse_aggregate_functions
        clickhouse_functions
        clickhouse_table_functions
)

if (TARGET ch_contrib::jemalloc)
    list(APPEND TCP_PROTOCOL_FUZZER_LINK PRIVATE ch_contrib::jemalloc)
endif()

target_link_libraries(tcp_protocol_fuzzer ${TCP_PROTOCOL_FUZZER_LINK})
