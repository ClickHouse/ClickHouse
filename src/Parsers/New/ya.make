LIBRARY()

PEERDIR(
    clickhouse/src/Common
    clickhouse/src/Parsers
)

CFLAGS(-g0)

SRCS(
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
    AST/SelectUnionQuery.cpp
    AST/SetQuery.cpp
    AST/SettingExpr.cpp
    AST/ShowCreateQuery.cpp
    AST/SystemQuery.cpp
    AST/TableElementExpr.cpp
    AST/TableExpr.cpp
    AST/TruncateQuery.cpp
    AST/UseQuery.cpp
    CharInputStream.cpp
    ClickHouseLexer.cpp
    ClickHouseParser.cpp
    ClickHouseParserVisitor.cpp
    LexerErrorListener.cpp
    parseQuery.cpp
    ParserErrorListener.cpp
    ParseTreeVisitor.cpp
)

END ()
