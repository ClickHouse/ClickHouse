
// Generated from ClickHouseParser.g4 by ANTLR 4.8

#pragma once


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

    virtual antlrcpp::Any visitJoinExpr(ClickHouseParser::JoinExprContext *context) = 0;

    virtual antlrcpp::Any visitLimitExpr(ClickHouseParser::LimitExprContext *context) = 0;

    virtual antlrcpp::Any visitOrderExprList(ClickHouseParser::OrderExprListContext *context) = 0;

    virtual antlrcpp::Any visitOrderExpr(ClickHouseParser::OrderExprContext *context) = 0;

    virtual antlrcpp::Any visitRatioExpr(ClickHouseParser::RatioExprContext *context) = 0;

    virtual antlrcpp::Any visitSettingExprList(ClickHouseParser::SettingExprListContext *context) = 0;

    virtual antlrcpp::Any visitSettingExpr(ClickHouseParser::SettingExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprList(ClickHouseParser::ColumnExprListContext *context) = 0;

    virtual antlrcpp::Any visitColumnExpr(ClickHouseParser::ColumnExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnFunctionExpr(ClickHouseParser::ColumnFunctionExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnArgList(ClickHouseParser::ColumnArgListContext *context) = 0;

    virtual antlrcpp::Any visitColumnArgExpr(ClickHouseParser::ColumnArgExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnLambdaExpr(ClickHouseParser::ColumnLambdaExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitTableIdentifier(ClickHouseParser::TableIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitIdentifier(ClickHouseParser::IdentifierContext *context) = 0;

    virtual antlrcpp::Any visitUnaryOp(ClickHouseParser::UnaryOpContext *context) = 0;

    virtual antlrcpp::Any visitBinaryOp(ClickHouseParser::BinaryOpContext *context) = 0;


};

}  // namespace DB
