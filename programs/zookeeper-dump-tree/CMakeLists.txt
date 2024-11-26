set (CLICKHOUSE_ZOOKEEPER_DUMP_TREE_SOURCES ZooKeeperDumpTree.cpp)

set (CLICKHOUSE_ZOOKEEPER_DUMP_TREE_LINK
    PRIVATE
        clickhouse_common_zookeeper_no_log
        clickhouse_common_io
        dbms
        clickhouse_functions
        boost::program_options
)

clickhouse_program_add(zookeeper-dump-tree)
