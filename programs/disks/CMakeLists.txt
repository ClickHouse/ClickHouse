set (CLICKHOUSE_DISKS_SOURCES
    DisksApp.cpp
    ICommand.cpp
    CommandCopy.cpp
    CommandLink.cpp
    CommandList.cpp
    CommandListDisks.cpp
    CommandMkDir.cpp
    CommandMove.cpp
    CommandRead.cpp
    CommandRemove.cpp
    CommandWrite.cpp)

if (CLICKHOUSE_CLOUD)
    set (CLICKHOUSE_DISKS_SOURCES ${CLICKHOUSE_DISKS_SOURCES} CommandPackedIO.cpp)
endif ()

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
