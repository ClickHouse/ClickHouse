set (SRCS
    AST/AlterTableQuery.cpp
    AST/AttachQuery.cpp
    AST/CheckQuery.cpp
    AST/ColumnExpr.cpp
    AST/ColumnTypeExpr.cpp
    AST/CreateDatabaseQuery.cpp
    AST/CreateDictionaryQuery.cpp
    AST/CreateLiveViewQuery.cpp
    AST/CreateMaterializedViewQuery.cpp
    AST/CreateTableQuery.cpp
    AST/CreateViewQuery.cpp
    AST/DDLQuery.cpp
    AST/DescribeQuery.cpp
    AST/DropQuery.cpp
    AST/EngineExpr.cpp
    AST/ExistsQuery.cpp
    AST/ExplainQuery.cpp
    AST/Identifier.cpp
    AST/InsertQuery.cpp
    AST/JoinExpr.cpp
    AST/KillQuery.cpp
    AST/LimitExpr.cpp
    AST/Literal.cpp
    AST/OptimizeQuery.cpp
    AST/OrderExpr.cpp
    AST/Query.cpp
    AST/RatioExpr.cpp
    AST/RenameQuery.cpp
    AST/SelectUnionQuery.cpp
    AST/SetQuery.cpp
    AST/SettingExpr.cpp
    AST/ShowCreateQuery.cpp
    AST/ShowQuery.cpp
    AST/SystemQuery.cpp
    AST/TableElementExpr.cpp
    AST/TableExpr.cpp
    AST/TruncateQuery.cpp
    AST/UseQuery.cpp
    AST/WatchQuery.cpp
    CharInputStream.cpp
    ClickHouseLexer.cpp
    ClickHouseParser.cpp
    ClickHouseParserVisitor.cpp
    LexerErrorListener.cpp
    parseQuery.cpp
    ParserErrorListener.cpp
    ParseTreeVisitor.cpp
)

add_library (clickhouse_parsers_new ${SRCS})

# XXX: hack for old clang-10!
check_cxx_compiler_flag("-Wsuggest-destructor-override" HAS_SUGGEST_DESTRUCTOR_OVERRIDE)

# XXX: hack for old gcc-10!
check_cxx_compiler_flag("-Wshadow" HAS_SHADOW)

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
if (HAS_SUGGEST_DESTRUCTOR_OVERRIDE)
    target_compile_options (clickhouse_parsers_new
        PRIVATE
            -Wno-suggest-destructor-override
    )
endif ()
if (HAS_SHADOW)
    target_compile_options (clickhouse_parsers_new
        PRIVATE
            -Wno-shadow
    )
endif ()

target_link_libraries (clickhouse_parsers_new PUBLIC antlr4-runtime clickhouse_common_io clickhouse_parsers)

# ANTLR generates u8 string literals, which are incompatible with |std::string| in C++20.
# See https://github.com/antlr/antlr4/issues/2683
set_source_files_properties(
        ClickHouseLexer.cpp
        ClickHouseParser.cpp
    PROPERTIES COMPILE_FLAGS -std=c++17
)

# Disable clang-tidy for whole target.
set_target_properties(clickhouse_parsers_new PROPERTIES CXX_CLANG_TIDY "")
