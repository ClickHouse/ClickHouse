set (CLICKHOUSE_KEEPER_CONVERTER_SOURCES KeeperConverter.cpp)

set (CLICKHOUSE_KEEPER_CONVERTER_LINK
    PRIVATE
        boost::program_options
        clickhouse_dictionaries
        dbms
)

clickhouse_program_add(keeper-converter)
