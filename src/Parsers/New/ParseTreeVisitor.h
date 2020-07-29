#pragma once

#include "ClickHouseParserBaseVisitor.h"

#include <Parsers/New/AST/Query.h>
#include <Parsers/New/ClickHouseParser.h>


namespace DB {

class ParseTreeVisitor : public ClickHouseParserBaseVisitor
{
public:
    virtual ~ParseTreeVisitor() = default;

    // Top-level statements

    antlrcpp::Any visitQueryList(ClickHouseParser::QueryListContext *ctx) override;
    antlrcpp::Any visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx) override;
    antlrcpp::Any visitSelectStmt(ClickHouseParser::SelectStmtContext *ctx) override;

    // SELECT clauses

    antlrcpp::Any visitWithClause(ClickHouseParser::WithClauseContext *ctx) override;
    antlrcpp::Any visitFromClause(ClickHouseParser::FromClauseContext *ctx) override;
    antlrcpp::Any visitSampleClause(ClickHouseParser::SampleClauseContext *ctx) override;
    antlrcpp::Any visitArrayJoinClause(ClickHouseParser::ArrayJoinClauseContext *ctx) override;
    antlrcpp::Any visitPrewhereClause(ClickHouseParser::PrewhereClauseContext *ctx) override;
    antlrcpp::Any visitWhereClause(ClickHouseParser::WhereClauseContext *ctx) override;
    antlrcpp::Any visitGroupByClause(ClickHouseParser::GroupByClauseContext *ctx) override;
    antlrcpp::Any visitHavingClause(ClickHouseParser::HavingClauseContext *ctx) override;
    antlrcpp::Any visitOrderByClause(ClickHouseParser::OrderByClauseContext *ctx) override;
    antlrcpp::Any visitLimitByClause(ClickHouseParser::LimitByClauseContext *ctx) override;
    antlrcpp::Any visitLimitClause(ClickHouseParser::LimitClauseContext *ctx) override;
    antlrcpp::Any visitSettingsClause(ClickHouseParser::SettingsClauseContext *ctx) override;

    // SELECT expressions

    antlrcpp::Any visitRatioExpr(ClickHouseParser::RatioExprContext *ctx) override;
    antlrcpp::Any visitOrderExprList(ClickHouseParser::OrderExprListContext *ctx) override;
    antlrcpp::Any visitOrderExpr(ClickHouseParser::OrderExprContext *ctx) override;
    antlrcpp::Any visitLimitExpr(ClickHouseParser::LimitExprContext *ctx) override;
    antlrcpp::Any visitSettingExprList(ClickHouseParser::SettingExprListContext *ctx) override;
    antlrcpp::Any visitSettingExpr(ClickHouseParser::SettingExprContext *ctx) override;

    // Join expressions

    antlrcpp::Any visitJoinExprTable(ClickHouseParser::JoinExprTableContext *ctx) override;

    // Column expressions

    antlrcpp::Any visitColumnExprList(ClickHouseParser::ColumnExprListContext *ctx) override;
    antlrcpp::Any visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext *ctx) override;

    // Table expressions

    antlrcpp::Any visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *ctx) override;
    antlrcpp::Any visitTableIdentifier(ClickHouseParser::TableIdentifierContext *ctx) override;

    // Basic expressions

    antlrcpp::Any visitIdentifier(ClickHouseParser::IdentifierContext *ctx) override;
    antlrcpp::Any visitLiteralNull(ClickHouseParser::LiteralNullContext *ctx) override;
    antlrcpp::Any visitLiteralNumber(ClickHouseParser::LiteralNumberContext *ctx) override;
    antlrcpp::Any visitLiteralString(ClickHouseParser::LiteralStringContext *ctx) override;

private:
    void visitQueryStmtAsParent(AST::Query *query, ClickHouseParser::QueryStmtContext *ctx);
};

}
