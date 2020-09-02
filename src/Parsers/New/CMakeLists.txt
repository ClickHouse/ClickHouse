set (SRCS
    AST/AlterPartitionQuery.cpp
    AST/AlterTableQuery.cpp
    AST/AnalyzeQuery.cpp
    AST/CheckQuery.cpp
    AST/ColumnExpr.cpp
    AST/ColumnTypeExpr.cpp
    AST/CreateDatabaseQuery.cpp
    AST/CreateMaterializedViewQuery.cpp
    AST/CreateTableQuery.cpp
    AST/CreateViewQuery.cpp
    AST/DDLQuery.cpp
    AST/DescribeQuery.cpp
    AST/DropQuery.cpp
    AST/EngineExpr.cpp
    AST/ExistsQuery.cpp
    AST/Identifier.cpp
    AST/InsertQuery.cpp
    AST/JoinExpr.cpp
    AST/LimitExpr.cpp
    AST/Literal.cpp
    AST/OptimizeQuery.cpp
    AST/OrderExpr.cpp
    AST/Query.cpp
    AST/RatioExpr.cpp
    AST/RenameQuery.cpp
    AST/SelectStmt.cpp
    AST/SelectUnionQuery.cpp
    AST/SetQuery.cpp
    AST/SettingExpr.cpp
    AST/ShowQuery.cpp
    AST/SystemQuery.cpp
    AST/TableElementExpr.cpp
    AST/TableExpr.cpp
    AST/UseQuery.cpp
    ClickHouseLexer.cpp
    ClickHouseParser.cpp
    ClickHouseParserBaseVisitor.cpp
    ClickHouseParserVisitor.cpp
    LexerErrorListener.cpp
    parseQuery.cpp
    ParserErrorListener.cpp
    ParseTreeVisitor.cpp
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
target_link_libraries (clickhouse_parsers_new PUBLIC antlr4-runtime clickhouse_common_io clickhouse_parsers)

# ANTLR generates u8 string literals, which are incompatible with |std::string| in C++20.
# See https://github.com/antlr/antlr4/issues/2683
set_source_files_properties(
        ClickHouseLexer.cpp
        ClickHouseParser.cpp
    PROPERTIES COMPILE_FLAGS -std=c++17
)
