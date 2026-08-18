if (NOT TARGET ch_contrib::rapidjson OR NOT TARGET ch_contrib::nuraft)
    message (STATUS "Not building keeper-bench because RapidJSON or NuRaft is disabled")
    return()
endif ()

set (CLICKHOUSE_KEEPER_BENCH_SOURCES
    Generator.cpp
    Runner.cpp
    Stats.cpp
    KeeperBench.cpp
)

set (CLICKHOUSE_KEEPER_BENCH_LINK
    PRIVATE
        dbms
        clickhouse_functions
        ch_contrib::rapidjson
)

clickhouse_program_add(keeper-bench)
