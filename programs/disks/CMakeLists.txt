set (CLICKHOUSE_DISKS_SOURCES
    DisksApp.cpp
    DisksClient.cpp
    ICommand.cpp
    CommandChangeDirectory.cpp
    CommandCopy.cpp
    CommandLink.cpp
    CommandList.cpp
    CommandListDisks.cpp
    CommandMkDir.cpp
    CommandMove.cpp
    CommandRead.cpp
    CommandRemove.cpp
    CommandSwitchDisk.cpp
    CommandWrite.cpp
    CommandHelp.cpp
    CommandTouch.cpp
    CommandGetCurrentDiskAndPath.cpp)

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
