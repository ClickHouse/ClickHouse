if (NOT TARGET ch_contrib::rapidjson)
    message (${RECONFIGURE_MESSAGE_LEVEL} "Not building keeper-bench due to rapidjson is disabled")
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
