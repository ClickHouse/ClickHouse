#pragma once

#include "ClickHouseParserVisitor.h"

#include <Parsers/New/AST/Query.h>
#include <Parsers/New/ClickHouseParser.h>


namespace DB {

class ParseTreeVisitor : public ClickHouseParserVisitor
{
public:
    virtual ~ParseTreeVisitor() = default;

    // Top-level statements

    antlrcpp::Any visitQueryList(ClickHouseParser::QueryListContext *ctx) override;
    antlrcpp::Any visitQueryStmt(ClickHouseParser::QueryStmtContext *ctx) override;
    antlrcpp::Any visitQuery(ClickHouseParser::QueryContext *ctx) override;
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

    // Join expressions (alphabetically)

    antlrcpp::Any visitJoinConstraintClause(ClickHouseParser::JoinConstraintClauseContext *ctx) override;
    antlrcpp::Any visitJoinExprCrossOp(ClickHouseParser::JoinExprCrossOpContext *ctx) override;
    antlrcpp::Any visitJoinExprOp(ClickHouseParser::JoinExprOpContext *ctx) override;
    antlrcpp::Any visitJoinExprParens(ClickHouseParser::JoinExprParensContext *ctx) override;
    antlrcpp::Any visitJoinExprTable(ClickHouseParser::JoinExprTableContext *ctx) override;
    antlrcpp::Any visitJoinOpCross(ClickHouseParser::JoinOpCrossContext *ctx) override;
    antlrcpp::Any visitJoinOpFull(ClickHouseParser::JoinOpFullContext *ctx) override;
    antlrcpp::Any visitJoinOpInner(ClickHouseParser::JoinOpInnerContext *ctx) override;
    antlrcpp::Any visitJoinOpLeftRight(ClickHouseParser::JoinOpLeftRightContext *ctx) override;

    // Column expressions (alphabetically)

    antlrcpp::Any visitColumnArgExpr(ClickHouseParser::ColumnArgExprContext *ctx) override;
    antlrcpp::Any visitColumnArgList(ClickHouseParser::ColumnArgListContext *ctx) override;
    antlrcpp::Any visitColumnExprAlias(ClickHouseParser::ColumnExprAliasContext *ctx) override;
    antlrcpp::Any visitColumnExprArray(ClickHouseParser::ColumnExprArrayContext *ctx) override;
    antlrcpp::Any visitColumnExprArrayAccess(ClickHouseParser::ColumnExprArrayAccessContext *ctx) override;
    antlrcpp::Any visitColumnExprAsterisk(ClickHouseParser::ColumnExprAsteriskContext *ctx) override;
    antlrcpp::Any visitColumnExprBetween(ClickHouseParser::ColumnExprBetweenContext *ctx) override;
    antlrcpp::Any visitColumnExprBinaryOp(ClickHouseParser::ColumnExprBinaryOpContext *ctx) override;
    antlrcpp::Any visitColumnExprCase(ClickHouseParser::ColumnExprCaseContext *ctx) override;
    // TODO: antlrcpp::Any visitColumnExprCast(ClickHouseParser::ColumnExprCastContext *ctx) override;
    antlrcpp::Any visitColumnExprExtract(ClickHouseParser::ColumnExprExtractContext *ctx) override;
    antlrcpp::Any visitColumnExprFunction(ClickHouseParser::ColumnExprFunctionContext *ctx) override;
    antlrcpp::Any visitColumnExprIdentifier(ClickHouseParser::ColumnExprIdentifierContext *ctx) override;
    antlrcpp::Any visitColumnExprInterval(ClickHouseParser::ColumnExprIntervalContext *ctx) override;
    antlrcpp::Any visitColumnExprIsNull(ClickHouseParser::ColumnExprIsNullContext *ctx) override;
    antlrcpp::Any visitColumnExprList(ClickHouseParser::ColumnExprListContext *ctx) override;
    antlrcpp::Any visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext *ctx) override;
    antlrcpp::Any visitColumnExprTernaryOp(ClickHouseParser::ColumnExprTernaryOpContext *ctx) override;
    antlrcpp::Any visitColumnExprTrim(ClickHouseParser::ColumnExprTrimContext *ctx) override;
    antlrcpp::Any visitColumnExprTuple(ClickHouseParser::ColumnExprTupleContext *ctx) override;
    antlrcpp::Any visitColumnExprTupleAccess(ClickHouseParser::ColumnExprTupleAccessContext *ctx) override;
    antlrcpp::Any visitColumnExprUnaryOp(ClickHouseParser::ColumnExprUnaryOpContext *ctx) override;
    antlrcpp::Any visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *ctx) override;
    antlrcpp::Any visitColumnLambdaExpr(ClickHouseParser::ColumnLambdaExprContext *ctx) override;
    antlrcpp::Any visitColumnParamList(ClickHouseParser::ColumnParamListContext *ctx) override;

    // Table expressions (alphabetically)

    antlrcpp::Any visitTableArgExpr(ClickHouseParser::TableArgExprContext *ctx) override;
    antlrcpp::Any visitTableArgList(ClickHouseParser::TableArgListContext *ctx) override;
    antlrcpp::Any visitTableExprAlias(ClickHouseParser::TableExprAliasContext *ctx) override;
    antlrcpp::Any visitTableExprFunction(ClickHouseParser::TableExprFunctionContext *ctx) override;
    antlrcpp::Any visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *ctx) override;
    antlrcpp::Any visitTableExprSubquery(ClickHouseParser::TableExprSubqueryContext *ctx) override;
    antlrcpp::Any visitTableFunctionExpr(ClickHouseParser::TableFunctionExprContext *ctx) override;
    antlrcpp::Any visitTableIdentifier(ClickHouseParser::TableIdentifierContext *ctx) override;

    // Database expressions

    antlrcpp::Any visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *ctx) override;

    // Basic expressions (alphabetically)

    antlrcpp::Any visitBinaryOp(ClickHouseParser::BinaryOpContext *ctx) override;  // returns |AST::ColumnExpr::BinaryOpType|
    antlrcpp::Any visitIdentifier(ClickHouseParser::IdentifierContext *ctx) override;
    antlrcpp::Any visitKeyword(ClickHouseParser::KeywordContext *ctx) override;
    antlrcpp::Any visitLiteral(ClickHouseParser::LiteralContext *ctx) override;
    antlrcpp::Any visitUnaryOp(ClickHouseParser::UnaryOpContext *ctx) override;  // returns |AST::ColumnExpr::UnaryOpType|
};

}
