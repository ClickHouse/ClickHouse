
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

  virtual antlrcpp::Any visitQuery(ClickHouseParser::QueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterTableStmt(ClickHouseParser::AlterTableStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterPartitionStmt(ClickHouseParser::AlterPartitionStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterTableAddClause(ClickHouseParser::AlterTableAddClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterTableClearClause(ClickHouseParser::AlterTableClearClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterTableCommentClause(ClickHouseParser::AlterTableCommentClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterTableDropClause(ClickHouseParser::AlterTableDropClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterTableModifyClause(ClickHouseParser::AlterTableModifyClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitAlterPartitionDropClause(ClickHouseParser::AlterPartitionDropClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCheckStmt(ClickHouseParser::CheckStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateDatabaseStmt(ClickHouseParser::CreateDatabaseStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateMaterializedViewStmt(ClickHouseParser::CreateMaterializedViewStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateTableStmt(ClickHouseParser::CreateTableStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitCreateViewStmt(ClickHouseParser::CreateViewStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSubqueryClause(ClickHouseParser::SubqueryClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSchemaDescriptionClause(ClickHouseParser::SchemaDescriptionClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSchemaAsTableClause(ClickHouseParser::SchemaAsTableClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSchemaAsFunctionClause(ClickHouseParser::SchemaAsFunctionClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitEngineClause(ClickHouseParser::EngineClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPartitionByClause(ClickHouseParser::PartitionByClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPrimaryKeyClause(ClickHouseParser::PrimaryKeyClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSampleByClause(ClickHouseParser::SampleByClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTtlClause(ClickHouseParser::TtlClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitEngineExpr(ClickHouseParser::EngineExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableElementExprColumn(ClickHouseParser::TableElementExprColumnContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableColumnDfnt(ClickHouseParser::TableColumnDfntContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableColumnPropertyExpr(ClickHouseParser::TableColumnPropertyExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTtlExpr(ClickHouseParser::TtlExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDescribeStmt(ClickHouseParser::DescribeStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDropDatabaseStmt(ClickHouseParser::DropDatabaseStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDropTableStmt(ClickHouseParser::DropTableStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitExistsStmt(ClickHouseParser::ExistsStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitInsertStmt(ClickHouseParser::InsertStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitValuesClause(ClickHouseParser::ValuesClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitValueTupleExpr(ClickHouseParser::ValueTupleExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitOptimizeStmt(ClickHouseParser::OptimizeStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitPartitionClause(ClickHouseParser::PartitionClauseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitRenameStmt(ClickHouseParser::RenameStmtContext *ctx) override {
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

  virtual antlrcpp::Any visitJoinExprOp(ClickHouseParser::JoinExprOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinExprTable(ClickHouseParser::JoinExprTableContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinExprParens(ClickHouseParser::JoinExprParensContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinExprCrossOp(ClickHouseParser::JoinExprCrossOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinOpInner(ClickHouseParser::JoinOpInnerContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinOpLeftRight(ClickHouseParser::JoinOpLeftRightContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinOpFull(ClickHouseParser::JoinOpFullContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinOpCross(ClickHouseParser::JoinOpCrossContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitJoinConstraintClause(ClickHouseParser::JoinConstraintClauseContext *ctx) override {
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

  virtual antlrcpp::Any visitSetStmt(ClickHouseParser::SetStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowCreateTableStmt(ClickHouseParser::ShowCreateTableStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitShowTablesStmt(ClickHouseParser::ShowTablesStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitSystemSyncStmt(ClickHouseParser::SystemSyncStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUseStmt(ClickHouseParser::UseStmtContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnTypeExprSimple(ClickHouseParser::ColumnTypeExprSimpleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnTypeExprParam(ClickHouseParser::ColumnTypeExprParamContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnTypeExprEnum(ClickHouseParser::ColumnTypeExprEnumContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnTypeExprComplex(ClickHouseParser::ColumnTypeExprComplexContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnTypeExprNested(ClickHouseParser::ColumnTypeExprNestedContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprList(ClickHouseParser::ColumnExprListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnsExprAsterisk(ClickHouseParser::ColumnsExprAsteriskContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnsExprSubquery(ClickHouseParser::ColumnsExprSubqueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnsExprColumn(ClickHouseParser::ColumnsExprColumnContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprTernaryOp(ClickHouseParser::ColumnExprTernaryOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprAlias(ClickHouseParser::ColumnExprAliasContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprExtract(ClickHouseParser::ColumnExprExtractContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprTrim(ClickHouseParser::ColumnExprTrimContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprSubquery(ClickHouseParser::ColumnExprSubqueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprArray(ClickHouseParser::ColumnExprArrayContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprTuple(ClickHouseParser::ColumnExprTupleContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprArrayAccess(ClickHouseParser::ColumnExprArrayAccessContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprBetween(ClickHouseParser::ColumnExprBetweenContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprCast(ClickHouseParser::ColumnExprCastContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprParens(ClickHouseParser::ColumnExprParensContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprUnaryOp(ClickHouseParser::ColumnExprUnaryOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprTupleAccess(ClickHouseParser::ColumnExprTupleAccessContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprCase(ClickHouseParser::ColumnExprCaseContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprInterval(ClickHouseParser::ColumnExprIntervalContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprIsNull(ClickHouseParser::ColumnExprIsNullContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprIdentifier(ClickHouseParser::ColumnExprIdentifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprBinaryOp(ClickHouseParser::ColumnExprBinaryOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprFunction(ClickHouseParser::ColumnExprFunctionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitColumnExprAsterisk(ClickHouseParser::ColumnExprAsteriskContext *ctx) override {
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

  virtual antlrcpp::Any visitNestedIdentifier(ClickHouseParser::NestedIdentifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableExprSubquery(ClickHouseParser::TableExprSubqueryContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableExprAlias(ClickHouseParser::TableExprAliasContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableExprFunction(ClickHouseParser::TableExprFunctionContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableIdentifier(ClickHouseParser::TableIdentifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableArgList(ClickHouseParser::TableArgListContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitTableArgExpr(ClickHouseParser::TableArgExprContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitFloatingLiteral(ClickHouseParser::FloatingLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitNumberLiteral(ClickHouseParser::NumberLiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitLiteral(ClickHouseParser::LiteralContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitKeyword(ClickHouseParser::KeywordContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIdentifier(ClickHouseParser::IdentifierContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitIdentifierOrNull(ClickHouseParser::IdentifierOrNullContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitUnaryOp(ClickHouseParser::UnaryOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitBinaryOp(ClickHouseParser::BinaryOpContext *ctx) override {
    return visitChildren(ctx);
  }

  virtual antlrcpp::Any visitEnumValue(ClickHouseParser::EnumValueContext *ctx) override {
    return visitChildren(ctx);
  }


};

}  // namespace DB
