
// Generated from ClickHouseParser.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"
#include "ClickHouseParserVisitor.h"


namespace DB {

/**
 * This class provides an empty implementation of ClickHouseParserVisitor, which can be
 * extended to create a visitor which only needs to handle a subset of the available methods.
 */
class  ClickHouseParserBaseVisitor : public ClickHouseParserVisitor {
public:

  virtual antlrcpp::Any visitQueryList(ClickHouseParser::QueryListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitQueryStmt(ClickHouseParser::QueryStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSelectStmt(ClickHouseParser::SelectStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitWithClause(ClickHouseParser::WithClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitFromClause(ClickHouseParser::FromClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSampleClause(ClickHouseParser::SampleClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitArrayJoinClause(ClickHouseParser::ArrayJoinClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPrewhereClause(ClickHouseParser::PrewhereClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitWhereClause(ClickHouseParser::WhereClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitGroupByClause(ClickHouseParser::GroupByClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitHavingClause(ClickHouseParser::HavingClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitOrderByClause(ClickHouseParser::OrderByClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLimitByClause(ClickHouseParser::LimitByClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLimitClause(ClickHouseParser::LimitClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSettingsClause(ClickHouseParser::SettingsClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinExpr(ClickHouseParser::JoinExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLimitExpr(ClickHouseParser::LimitExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitOrderExprList(ClickHouseParser::OrderExprListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitOrderExpr(ClickHouseParser::OrderExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRatioExpr(ClickHouseParser::RatioExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSettingExprList(ClickHouseParser::SettingExprListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSettingExpr(ClickHouseParser::SettingExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInsertStmt(ClickHouseParser::InsertStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprList(ClickHouseParser::ColumnExprListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTupleAccess(ClickHouseParser::TupleAccessContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTernary(ClickHouseParser::TernaryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBetween(ClickHouseParser::BetweenContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSubquery(ClickHouseParser::SubqueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnary(ClickHouseParser::UnaryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCase(ClickHouseParser::CaseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitArray(ClickHouseParser::ArrayContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitArrayAccess(ClickHouseParser::ArrayAccessContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlias(ClickHouseParser::AliasContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLiteral(ClickHouseParser::LiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIsNull(ClickHouseParser::IsNullContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitId(ClickHouseParser::IdContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitFunctionCall(ClickHouseParser::FunctionCallContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBinary(ClickHouseParser::BinaryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAsterisk(ClickHouseParser::AsteriskContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTuple(ClickHouseParser::TupleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInterval(ClickHouseParser::IntervalContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnFunctionExpr(ClickHouseParser::ColumnFunctionExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnArgList(ClickHouseParser::ColumnArgListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnArgExpr(ClickHouseParser::ColumnArgExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnLambdaExpr(ClickHouseParser::ColumnLambdaExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableIdentifier(ClickHouseParser::TableIdentifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIdentifier(ClickHouseParser::IdentifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnaryOp(ClickHouseParser::UnaryOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBinaryOp(ClickHouseParser::BinaryOpContext *ctx) override {
    return visitChildren(ctx);
  }


};

}  // namespace DB
