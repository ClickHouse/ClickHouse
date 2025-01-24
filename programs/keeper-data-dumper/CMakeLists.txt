if (NOT TARGET ch_contrib::nuraft)
    message (WARNING "Not building keeper-data-dumper due to nuraft is disabled")
    return ()
endif ()

set (CLICKHOUSE_KEEPER_DATA_DUMPER_SOURCES KeeperDataDumper.cpp)

set (CLICKHOUSE_KEEPER_DATA_DUMPER_LINK
    PRIVATE
        clickhouse_functions
        dbms
)

clickhouse_program_add(keeper-data-dumper)
