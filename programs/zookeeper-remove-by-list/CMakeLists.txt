set (CLICKHOUSE_ZOOKEEPER_REMOVE_BY_LIST_SOURCES ZooKeeperRemoveByList.cpp)

set (CLICKHOUSE_ZOOKEEPER_REMOVE_BY_LIST_LINK
    PRIVATE
        clickhouse_common_zookeeper_no_log
        clickhouse_common_io
        dbms
        clickhouse_functions
        boost::program_options
)

clickhouse_program_add(zookeeper-remove-by-list)
