set (CLICKHOUSE_DISKS_SOURCES
    ICommand.cpp
    DisksClient.cpp
    DisksApp.cpp
    CommandCopy.cpp
    CommandListDisks.cpp
    CommandList.cpp
    CommandLink.cpp
    CommandChangeDirectory.cpp
    CommandMkDir.cpp
    CommandMove.cpp
    CommandRead.cpp
    CommandRemove.cpp
    CommandWrite.cpp
    CommandSwitchDisk.cpp)

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
