
// Generated from ClickHouseParser.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"
#include "ClickHouseParser.h"


namespace DB {

/**
 * This class defines an abstract visitor for a parse tree
 * produced by ClickHouseParser.
 */
class  ClickHouseParserVisitor : public antlr4::tree::AbstractParseTreeVisitor {
public:

  /**
   * Visit parse trees produced by ClickHouseParser.
   */
    virtual antlrcpp::Any visitQueryList(ClickHouseParser::QueryListContext *context) = 0;

    virtual antlrcpp::Any visitQueryStmt(ClickHouseParser::QueryStmtContext *context) = 0;

    virtual antlrcpp::Any visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *context) = 0;

    virtual antlrcpp::Any visitSelectStmt(ClickHouseParser::SelectStmtContext *context) = 0;

    virtual antlrcpp::Any visitWithClause(ClickHouseParser::WithClauseContext *context) = 0;

    virtual antlrcpp::Any visitFromClause(ClickHouseParser::FromClauseContext *context) = 0;

    virtual antlrcpp::Any visitSampleClause(ClickHouseParser::SampleClauseContext *context) = 0;

    virtual antlrcpp::Any visitArrayJoinClause(ClickHouseParser::ArrayJoinClauseContext *context) = 0;

    virtual antlrcpp::Any visitPrewhereClause(ClickHouseParser::PrewhereClauseContext *context) = 0;

    virtual antlrcpp::Any visitWhereClause(ClickHouseParser::WhereClauseContext *context) = 0;

    virtual antlrcpp::Any visitGroupByClause(ClickHouseParser::GroupByClauseContext *context) = 0;

    virtual antlrcpp::Any visitHavingClause(ClickHouseParser::HavingClauseContext *context) = 0;

    virtual antlrcpp::Any visitOrderByClause(ClickHouseParser::OrderByClauseContext *context) = 0;

    virtual antlrcpp::Any visitLimitByClause(ClickHouseParser::LimitByClauseContext *context) = 0;

    virtual antlrcpp::Any visitLimitClause(ClickHouseParser::LimitClauseContext *context) = 0;

    virtual antlrcpp::Any visitSettingsClause(ClickHouseParser::SettingsClauseContext *context) = 0;

    virtual antlrcpp::Any visitJoinExprOp(ClickHouseParser::JoinExprOpContext *context) = 0;

    virtual antlrcpp::Any visitJoinExprTable(ClickHouseParser::JoinExprTableContext *context) = 0;

    virtual antlrcpp::Any visitJoinExprParens(ClickHouseParser::JoinExprParensContext *context) = 0;

    virtual antlrcpp::Any visitJoinExprCrossOp(ClickHouseParser::JoinExprCrossOpContext *context) = 0;

    virtual antlrcpp::Any visitJoinOpInner(ClickHouseParser::JoinOpInnerContext *context) = 0;

    virtual antlrcpp::Any visitJoinOpLeftRight(ClickHouseParser::JoinOpLeftRightContext *context) = 0;

    virtual antlrcpp::Any visitJoinOpFull(ClickHouseParser::JoinOpFullContext *context) = 0;

    virtual antlrcpp::Any visitJoinConstraintClause(ClickHouseParser::JoinConstraintClauseContext *context) = 0;

    virtual antlrcpp::Any visitJoinCrossOp(ClickHouseParser::JoinCrossOpContext *context) = 0;

    virtual antlrcpp::Any visitLimitExpr(ClickHouseParser::LimitExprContext *context) = 0;

    virtual antlrcpp::Any visitOrderExprList(ClickHouseParser::OrderExprListContext *context) = 0;

    virtual antlrcpp::Any visitOrderExpr(ClickHouseParser::OrderExprContext *context) = 0;

    virtual antlrcpp::Any visitRatioExpr(ClickHouseParser::RatioExprContext *context) = 0;

    virtual antlrcpp::Any visitSettingExprList(ClickHouseParser::SettingExprListContext *context) = 0;

    virtual antlrcpp::Any visitSettingExpr(ClickHouseParser::SettingExprContext *context) = 0;

    virtual antlrcpp::Any visitInsertStmt(ClickHouseParser::InsertStmtContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprList(ClickHouseParser::ColumnExprListContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTernaryOp(ClickHouseParser::ColumnExprTernaryOpContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprAlias(ClickHouseParser::ColumnExprAliasContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprArray(ClickHouseParser::ColumnExprArrayContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTuple(ClickHouseParser::ColumnExprTupleContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprArrayAccess(ClickHouseParser::ColumnExprArrayAccessContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprBetween(ClickHouseParser::ColumnExprBetweenContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprParens(ClickHouseParser::ColumnExprParensContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprUnaryOp(ClickHouseParser::ColumnExprUnaryOpContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTupleAccess(ClickHouseParser::ColumnExprTupleAccessContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprCase(ClickHouseParser::ColumnExprCaseContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprInterval(ClickHouseParser::ColumnExprIntervalContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprIsNull(ClickHouseParser::ColumnExprIsNullContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprIdentifier(ClickHouseParser::ColumnExprIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprBinaryOp(ClickHouseParser::ColumnExprBinaryOpContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprAsterisk(ClickHouseParser::ColumnExprAsteriskContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprFunction(ClickHouseParser::ColumnExprFunctionContext *context) = 0;

    virtual antlrcpp::Any visitColumnFunctionExpr(ClickHouseParser::ColumnFunctionExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnArgList(ClickHouseParser::ColumnArgListContext *context) = 0;

    virtual antlrcpp::Any visitColumnArgExpr(ClickHouseParser::ColumnArgExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnLambdaExpr(ClickHouseParser::ColumnLambdaExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitTableExprSubquery(ClickHouseParser::TableExprSubqueryContext *context) = 0;

    virtual antlrcpp::Any visitTableExprAlias(ClickHouseParser::TableExprAliasContext *context) = 0;

    virtual antlrcpp::Any visitTableExprFunction(ClickHouseParser::TableExprFunctionContext *context) = 0;

    virtual antlrcpp::Any visitTableIdentifier(ClickHouseParser::TableIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitTableFunctionExpr(ClickHouseParser::TableFunctionExprContext *context) = 0;

    virtual antlrcpp::Any visitTableArgList(ClickHouseParser::TableArgListContext *context) = 0;

    virtual antlrcpp::Any visitTableArgExpr(ClickHouseParser::TableArgExprContext *context) = 0;

    virtual antlrcpp::Any visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitLiteral(ClickHouseParser::LiteralContext *context) = 0;

    virtual antlrcpp::Any visitIdentifier(ClickHouseParser::IdentifierContext *context) = 0;

    virtual antlrcpp::Any visitUnaryOp(ClickHouseParser::UnaryOpContext *context) = 0;

    virtual antlrcpp::Any visitBinaryOp(ClickHouseParser::BinaryOpContext *context) = 0;


};

}  // namespace DB
