set (SRCS
    ClickHouseLexer.cpp
    ClickHouseParser.cpp
    ClickHouseParserBaseVisitor.cpp
    ClickHouseParserVisitor.cpp
    parseQuery.cpp
)

add_library (clickhouse_parsers_new ${SRCS})

target_compile_options (clickhouse_parsers_new
    PUBLIC
        -Wno-documentation-html
        -Wno-documentation
        -Wno-documentation-deprecated-sync
        -Wno-inconsistent-missing-destructor-override
        -Wno-extra-semi
)
target_link_libraries (clickhouse_parsers_new PUBLIC antlr4-runtime clickhouse_common_io)
