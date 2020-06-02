#pragma once

#include "ClickHouseParserBaseVisitor.h"

namespace DB {

class ParserTreeVisitor : public ClickHouseParserBaseVisitor
{
public:
    virtual ~ParserTreeVisitor() = default;

    antlrcpp::Any visitQueryList(ClickHouseParser::QueryListContext *ctx) override;
    antlrcpp::Any visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx) override;
    antlrcpp::Any visitSelectStmt(ClickHouseParser::SelectStmtContext *ctx) override;
};

}
