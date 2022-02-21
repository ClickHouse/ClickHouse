set (CLICKHOUSE_KEEPER_CONVERTER_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/KeeperConverter.cpp)

set (CLICKHOUSE_KEEPER_CONVERTER_LINK
    PRIVATE
        boost::program_options
        dbms
)

clickhouse_program_add(keeper-converter)
