set (SRCS
    ClickHouseLexer.cpp
    ClickHouseLexer.h
    ClickHouseParser.cpp
    ClickHouseParser.h
    ClickHouseParserBaseVisitor.cpp
    ClickHouseParserBaseVisitor.h
    ClickHouseParserVisitor.cpp
    ClickHouseParserVisitor.h
)

add_library (clickhouse_parsers_new ${SRCS})

target_link_libraries (clickhouse_parsers_new PUBLIC antlr4-runtime)
