set (CLICKHOUSE_FST_DUMP_TREE_SOURCES FstDumpTree.cpp)

set (CLICKHOUSE_FST_DUMP_TREE_LINK
    PRIVATE
        dbms
        clickhouse_functions
        boost::program_options
)

clickhouse_program_add(fst-dump-tree)
