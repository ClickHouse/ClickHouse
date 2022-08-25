set (CLICKHOUSE_DISKS_SOURCES DisksApp.cpp ICommand.cpp)

set (CLICKHOUSE_DISKS_LINK
    PRIVATE
        boost::program_options
        clickhouse_aggregate_functions
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_functions
        clickhouse_parsers
        clickhouse_table_functions
        )

clickhouse_program_add(disks)
