
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

    virtual antlrcpp::Any visitQuery(ClickHouseParser::QueryContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableStmt(ClickHouseParser::AlterTableStmtContext *context) = 0;

    virtual antlrcpp::Any visitAlterPartitionStmt(ClickHouseParser::AlterPartitionStmtContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableAddClause(ClickHouseParser::AlterTableAddClauseContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableCommentClause(ClickHouseParser::AlterTableCommentClauseContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableDropClause(ClickHouseParser::AlterTableDropClauseContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableModifyClause(ClickHouseParser::AlterTableModifyClauseContext *context) = 0;

    virtual antlrcpp::Any visitAlterPartitionDropClause(ClickHouseParser::AlterPartitionDropClauseContext *context) = 0;

    virtual antlrcpp::Any visitCheckStmt(ClickHouseParser::CheckStmtContext *context) = 0;

    virtual antlrcpp::Any visitCreateDatabaseStmt(ClickHouseParser::CreateDatabaseStmtContext *context) = 0;

    virtual antlrcpp::Any visitCreateMaterializedViewStmt(ClickHouseParser::CreateMaterializedViewStmtContext *context) = 0;

    virtual antlrcpp::Any visitCreateTableStmt(ClickHouseParser::CreateTableStmtContext *context) = 0;

    virtual antlrcpp::Any visitCreateViewStmt(ClickHouseParser::CreateViewStmtContext *context) = 0;

    virtual antlrcpp::Any visitSubqueryClause(ClickHouseParser::SubqueryClauseContext *context) = 0;

    virtual antlrcpp::Any visitSchemaDescriptionClause(ClickHouseParser::SchemaDescriptionClauseContext *context) = 0;

    virtual antlrcpp::Any visitSchemaAsTableClause(ClickHouseParser::SchemaAsTableClauseContext *context) = 0;

    virtual antlrcpp::Any visitSchemaAsFunctionClause(ClickHouseParser::SchemaAsFunctionClauseContext *context) = 0;

    virtual antlrcpp::Any visitEngineClause(ClickHouseParser::EngineClauseContext *context) = 0;

    virtual antlrcpp::Any visitPartitionByClause(ClickHouseParser::PartitionByClauseContext *context) = 0;

    virtual antlrcpp::Any visitPrimaryKeyClause(ClickHouseParser::PrimaryKeyClauseContext *context) = 0;

    virtual antlrcpp::Any visitSampleByClause(ClickHouseParser::SampleByClauseContext *context) = 0;

    virtual antlrcpp::Any visitTtlClause(ClickHouseParser::TtlClauseContext *context) = 0;

    virtual antlrcpp::Any visitEngineExpr(ClickHouseParser::EngineExprContext *context) = 0;

    virtual antlrcpp::Any visitTableElementExprColumn(ClickHouseParser::TableElementExprColumnContext *context) = 0;

    virtual antlrcpp::Any visitTableColumnDfnt(ClickHouseParser::TableColumnDfntContext *context) = 0;

    virtual antlrcpp::Any visitTableColumnPropertyExpr(ClickHouseParser::TableColumnPropertyExprContext *context) = 0;

    virtual antlrcpp::Any visitTtlExpr(ClickHouseParser::TtlExprContext *context) = 0;

    virtual antlrcpp::Any visitDescribeStmt(ClickHouseParser::DescribeStmtContext *context) = 0;

    virtual antlrcpp::Any visitDropDatabaseStmt(ClickHouseParser::DropDatabaseStmtContext *context) = 0;

    virtual antlrcpp::Any visitDropTableStmt(ClickHouseParser::DropTableStmtContext *context) = 0;

    virtual antlrcpp::Any visitExistsStmt(ClickHouseParser::ExistsStmtContext *context) = 0;

    virtual antlrcpp::Any visitInsertStmt(ClickHouseParser::InsertStmtContext *context) = 0;

    virtual antlrcpp::Any visitValuesClause(ClickHouseParser::ValuesClauseContext *context) = 0;

    virtual antlrcpp::Any visitValueTupleExpr(ClickHouseParser::ValueTupleExprContext *context) = 0;

    virtual antlrcpp::Any visitOptimizeStmt(ClickHouseParser::OptimizeStmtContext *context) = 0;

    virtual antlrcpp::Any visitPartitionClause(ClickHouseParser::PartitionClauseContext *context) = 0;

    virtual antlrcpp::Any visitRenameStmt(ClickHouseParser::RenameStmtContext *context) = 0;

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

    virtual antlrcpp::Any visitJoinOpCross(ClickHouseParser::JoinOpCrossContext *context) = 0;

    virtual antlrcpp::Any visitJoinConstraintClause(ClickHouseParser::JoinConstraintClauseContext *context) = 0;

    virtual antlrcpp::Any visitLimitExpr(ClickHouseParser::LimitExprContext *context) = 0;

    virtual antlrcpp::Any visitOrderExprList(ClickHouseParser::OrderExprListContext *context) = 0;

    virtual antlrcpp::Any visitOrderExpr(ClickHouseParser::OrderExprContext *context) = 0;

    virtual antlrcpp::Any visitRatioExpr(ClickHouseParser::RatioExprContext *context) = 0;

    virtual antlrcpp::Any visitSettingExprList(ClickHouseParser::SettingExprListContext *context) = 0;

    virtual antlrcpp::Any visitSettingExpr(ClickHouseParser::SettingExprContext *context) = 0;

    virtual antlrcpp::Any visitSetStmt(ClickHouseParser::SetStmtContext *context) = 0;

    virtual antlrcpp::Any visitShowCreateTableStmt(ClickHouseParser::ShowCreateTableStmtContext *context) = 0;

    virtual antlrcpp::Any visitShowTablesStmt(ClickHouseParser::ShowTablesStmtContext *context) = 0;

    virtual antlrcpp::Any visitUseStmt(ClickHouseParser::UseStmtContext *context) = 0;

    virtual antlrcpp::Any visitValueExprList(ClickHouseParser::ValueExprListContext *context) = 0;

    virtual antlrcpp::Any visitValueExprLiteral(ClickHouseParser::ValueExprLiteralContext *context) = 0;

    virtual antlrcpp::Any visitValueExprTuple(ClickHouseParser::ValueExprTupleContext *context) = 0;

    virtual antlrcpp::Any visitValueExprArray(ClickHouseParser::ValueExprArrayContext *context) = 0;

    virtual antlrcpp::Any visitColumnTypeExprSimple(ClickHouseParser::ColumnTypeExprSimpleContext *context) = 0;

    virtual antlrcpp::Any visitColumnTypeExprParam(ClickHouseParser::ColumnTypeExprParamContext *context) = 0;

    virtual antlrcpp::Any visitColumnTypeExprEnum(ClickHouseParser::ColumnTypeExprEnumContext *context) = 0;

    virtual antlrcpp::Any visitColumnTypeExprComplex(ClickHouseParser::ColumnTypeExprComplexContext *context) = 0;

    virtual antlrcpp::Any visitColumnTypeExprNested(ClickHouseParser::ColumnTypeExprNestedContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprList(ClickHouseParser::ColumnExprListContext *context) = 0;

    virtual antlrcpp::Any visitColumnsExprAsterisk(ClickHouseParser::ColumnsExprAsteriskContext *context) = 0;

    virtual antlrcpp::Any visitColumnsExprSubquery(ClickHouseParser::ColumnsExprSubqueryContext *context) = 0;

    virtual antlrcpp::Any visitColumnsExprColumn(ClickHouseParser::ColumnsExprColumnContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTernaryOp(ClickHouseParser::ColumnExprTernaryOpContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprAlias(ClickHouseParser::ColumnExprAliasContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprExtract(ClickHouseParser::ColumnExprExtractContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTrim(ClickHouseParser::ColumnExprTrimContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprSubquery(ClickHouseParser::ColumnExprSubqueryContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprArray(ClickHouseParser::ColumnExprArrayContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTuple(ClickHouseParser::ColumnExprTupleContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprArrayAccess(ClickHouseParser::ColumnExprArrayAccessContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprBetween(ClickHouseParser::ColumnExprBetweenContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprCast(ClickHouseParser::ColumnExprCastContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprParens(ClickHouseParser::ColumnExprParensContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprUnaryOp(ClickHouseParser::ColumnExprUnaryOpContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTupleAccess(ClickHouseParser::ColumnExprTupleAccessContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprCase(ClickHouseParser::ColumnExprCaseContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprInterval(ClickHouseParser::ColumnExprIntervalContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprIsNull(ClickHouseParser::ColumnExprIsNullContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprIdentifier(ClickHouseParser::ColumnExprIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprBinaryOp(ClickHouseParser::ColumnExprBinaryOpContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprFunction(ClickHouseParser::ColumnExprFunctionContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprAsterisk(ClickHouseParser::ColumnExprAsteriskContext *context) = 0;

    virtual antlrcpp::Any visitColumnParamList(ClickHouseParser::ColumnParamListContext *context) = 0;

    virtual antlrcpp::Any visitColumnArgList(ClickHouseParser::ColumnArgListContext *context) = 0;

    virtual antlrcpp::Any visitColumnArgExpr(ClickHouseParser::ColumnArgExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnLambdaExpr(ClickHouseParser::ColumnLambdaExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitNestedIdentifier(ClickHouseParser::NestedIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitTableExprSubquery(ClickHouseParser::TableExprSubqueryContext *context) = 0;

    virtual antlrcpp::Any visitTableExprAlias(ClickHouseParser::TableExprAliasContext *context) = 0;

    virtual antlrcpp::Any visitTableExprFunction(ClickHouseParser::TableExprFunctionContext *context) = 0;

    virtual antlrcpp::Any visitTableIdentifier(ClickHouseParser::TableIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitTableArgList(ClickHouseParser::TableArgListContext *context) = 0;

    virtual antlrcpp::Any visitTableArgExpr(ClickHouseParser::TableArgExprContext *context) = 0;

    virtual antlrcpp::Any visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitFloatingLiteral(ClickHouseParser::FloatingLiteralContext *context) = 0;

    virtual antlrcpp::Any visitNumberLiteral(ClickHouseParser::NumberLiteralContext *context) = 0;

    virtual antlrcpp::Any visitLiteral(ClickHouseParser::LiteralContext *context) = 0;

    virtual antlrcpp::Any visitKeyword(ClickHouseParser::KeywordContext *context) = 0;

    virtual antlrcpp::Any visitIdentifier(ClickHouseParser::IdentifierContext *context) = 0;

    virtual antlrcpp::Any visitIdentifierOrNull(ClickHouseParser::IdentifierOrNullContext *context) = 0;

    virtual antlrcpp::Any visitUnaryOp(ClickHouseParser::UnaryOpContext *context) = 0;

    virtual antlrcpp::Any visitBinaryOp(ClickHouseParser::BinaryOpContext *context) = 0;

    virtual antlrcpp::Any visitEnumValue(ClickHouseParser::EnumValueContext *context) = 0;


};

}  // namespace DB
