#pragma once

#include "ClickHouseParserBaseVisitor.h"

#include <Parsers/New/AST/Query.h>
#include <Parsers/New/ClickHouseParser.h>


namespace DB {

class ParserTreeVisitor : public ClickHouseParserBaseVisitor
{
public:
    virtual ~ParserTreeVisitor() = default;

    antlrcpp::Any /* QueryList */ visitQueryList(ClickHouseParser::QueryListContext *ctx) override;
    antlrcpp::Any /* SelectUnionQuery */ visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx) override;
    antlrcpp::Any /* SelectStmt */ visitSelectStmt(ClickHouseParser::SelectStmtContext *ctx) override;

private:
    void visitQueryStmtAsParent(AST::Query *query, ClickHouseParser::QueryStmtContext *ctx);
};

ASTPtr parseQuery(const std::string& query);

}
