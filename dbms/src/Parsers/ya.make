LIBRARY()

PEERDIR(
    clickhouse/dbms/src/Common
)

SRCS(
    ASTCreateQuotaQuery.cpp
    ASTCreateRoleQuery.cpp
    ASTCreateRowPolicyQuery.cpp
    ASTCreateUserQuery.cpp
    ASTDictionaryAttributeDeclaration.cpp
    ASTExpressionList.cpp
    ASTFunction.cpp
    ASTFunctionWithKeyValueArguments.cpp
    ASTGenericRoleSet.cpp
    ASTGrantQuery.cpp
    ASTIdentifier.cpp
    ASTLiteral.cpp
    ASTWithAlias.cpp
    ExpressionElementParsers.cpp
    ExpressionListParsers.cpp
    formatAST.cpp
    IAST.cpp
    IParserBase.cpp
    parseQuery.cpp
    ParserCreateQuotaQuery.cpp
    ParserCreateRoleQuery.cpp
    ParserCreateRowPolicyQuery.cpp
    ParserCreateUserQuery.cpp
    ParserGrantQuery.cpp
    queryToString.cpp
)

END()
