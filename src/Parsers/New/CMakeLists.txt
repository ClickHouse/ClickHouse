set (SRCS
    ClickHouseLexer.cpp
    ClickHouseParser.cpp
    ClickHouseParserBaseVisitor.cpp
    ClickHouseParserVisitor.cpp
    parseQuery.cpp
)

add_library (clickhouse_parsers_new ${SRCS})

target_compile_options (clickhouse_parsers_new
    PRIVATE
        -Wno-c++20-compat
        -Wno-deprecated-this-capture
        -Wno-documentation-html
        -Wno-documentation
        -Wno-documentation-deprecated-sync
        -Wno-shadow-field
        -Wno-unused-parameter

    PUBLIC
        -Wno-extra-semi
        -Wno-inconsistent-missing-destructor-override
)
target_link_libraries (clickhouse_parsers_new PUBLIC antlr4-runtime clickhouse_common_io)

# ANTLR generates u8 string literals, which are incompatible with |std::string| in C++20.
# See https://github.com/antlr/antlr4/issues/2683
set_source_files_properties(
        ClickHouseLexer.cpp
        ClickHouseParser.cpp
    PROPERTIES COMPILE_FLAGS -std=c++17
)
