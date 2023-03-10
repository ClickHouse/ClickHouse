set (CLICKHOUSE_KEEPER_CLIENT_SOURCES KeeperClient.cpp)

set (CLICKHOUSE_KEEPER_CLIENT_LINK
    PRIVATE
    boost::program_options
    dbms
)

clickhouse_program_add(keeper-client)
