add_executable(fdb_meta_store fdb_meta_store.cpp)
target_link_libraries(fdb_meta_store
    PRIVATE
        clickhouse_common_foundationdb
        clickhouse_common_config
        clickhouse_common_access
        dbms
        ch_contrib::fmt
        loggers_no_text_log
)

add_executable(fdb_network_test fdb_network_test.cpp)
target_link_libraries(fdb_network_test
    PRIVATE
        clickhouse_common_foundationdb
        clickhouse_common_config
        ch_contrib::gtest_all
        ch_contrib::fmt
        loggers_no_text_log
        fdb_c
)