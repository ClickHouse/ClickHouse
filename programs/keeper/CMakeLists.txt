set(CLICKHOUSE_KEEPER_SOURCES
    Keeper.cpp
)

set (CLICKHOUSE_KEEPER_LINK
    PRIVATE
        clickhouse_common_config
        clickhouse_common_io
        clickhouse_common_zookeeper
        daemon
        dbms
)

clickhouse_program_add(keeper)
