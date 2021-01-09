
// Generated from ClickHouseParser.g4 by ANTLR 4.7.2

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
    virtual antlrcpp::Any visitQueryStmt(ClickHouseParser::QueryStmtContext *context) = 0;

    virtual antlrcpp::Any visitQuery(ClickHouseParser::QueryContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableStmt(ClickHouseParser::AlterTableStmtContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseAddColumn(ClickHouseParser::AlterTableClauseAddColumnContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseAddIndex(ClickHouseParser::AlterTableClauseAddIndexContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseAttach(ClickHouseParser::AlterTableClauseAttachContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseClear(ClickHouseParser::AlterTableClauseClearContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseComment(ClickHouseParser::AlterTableClauseCommentContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseDelete(ClickHouseParser::AlterTableClauseDeleteContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseDetach(ClickHouseParser::AlterTableClauseDetachContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseDropColumn(ClickHouseParser::AlterTableClauseDropColumnContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseDropIndex(ClickHouseParser::AlterTableClauseDropIndexContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseDropPartition(ClickHouseParser::AlterTableClauseDropPartitionContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseFreezePartition(ClickHouseParser::AlterTableClauseFreezePartitionContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseModifyCodec(ClickHouseParser::AlterTableClauseModifyCodecContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseModifyComment(ClickHouseParser::AlterTableClauseModifyCommentContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseModifyRemove(ClickHouseParser::AlterTableClauseModifyRemoveContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseModify(ClickHouseParser::AlterTableClauseModifyContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseModifyOrderBy(ClickHouseParser::AlterTableClauseModifyOrderByContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseModifyTTL(ClickHouseParser::AlterTableClauseModifyTTLContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseMovePartition(ClickHouseParser::AlterTableClauseMovePartitionContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseRemoveTTL(ClickHouseParser::AlterTableClauseRemoveTTLContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseRename(ClickHouseParser::AlterTableClauseRenameContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseReplace(ClickHouseParser::AlterTableClauseReplaceContext *context) = 0;

    virtual antlrcpp::Any visitAlterTableClauseUpdate(ClickHouseParser::AlterTableClauseUpdateContext *context) = 0;

    virtual antlrcpp::Any visitAssignmentExprList(ClickHouseParser::AssignmentExprListContext *context) = 0;

    virtual antlrcpp::Any visitAssignmentExpr(ClickHouseParser::AssignmentExprContext *context) = 0;

    virtual antlrcpp::Any visitTableColumnPropertyType(ClickHouseParser::TableColumnPropertyTypeContext *context) = 0;

    virtual antlrcpp::Any visitPartitionClause(ClickHouseParser::PartitionClauseContext *context) = 0;

    virtual antlrcpp::Any visitAttachDictionaryStmt(ClickHouseParser::AttachDictionaryStmtContext *context) = 0;

    virtual antlrcpp::Any visitCheckStmt(ClickHouseParser::CheckStmtContext *context) = 0;

    virtual antlrcpp::Any visitCreateDatabaseStmt(ClickHouseParser::CreateDatabaseStmtContext *context) = 0;

    virtual antlrcpp::Any visitCreateDictionaryStmt(ClickHouseParser::CreateDictionaryStmtContext *context) = 0;

    virtual antlrcpp::Any visitCreateLiveViewStmt(ClickHouseParser::CreateLiveViewStmtContext *context) = 0;

    virtual antlrcpp::Any visitCreateMaterializedViewStmt(ClickHouseParser::CreateMaterializedViewStmtContext *context) = 0;

    virtual antlrcpp::Any visitCreateTableStmt(ClickHouseParser::CreateTableStmtContext *context) = 0;

    virtual antlrcpp::Any visitCreateViewStmt(ClickHouseParser::CreateViewStmtContext *context) = 0;

    virtual antlrcpp::Any visitDictionarySchemaClause(ClickHouseParser::DictionarySchemaClauseContext *context) = 0;

    virtual antlrcpp::Any visitDictionaryAttrDfnt(ClickHouseParser::DictionaryAttrDfntContext *context) = 0;

    virtual antlrcpp::Any visitDictionaryEngineClause(ClickHouseParser::DictionaryEngineClauseContext *context) = 0;

    virtual antlrcpp::Any visitDictionaryPrimaryKeyClause(ClickHouseParser::DictionaryPrimaryKeyClauseContext *context) = 0;

    virtual antlrcpp::Any visitDictionaryArgExpr(ClickHouseParser::DictionaryArgExprContext *context) = 0;

    virtual antlrcpp::Any visitSourceClause(ClickHouseParser::SourceClauseContext *context) = 0;

    virtual antlrcpp::Any visitLifetimeClause(ClickHouseParser::LifetimeClauseContext *context) = 0;

    virtual antlrcpp::Any visitLayoutClause(ClickHouseParser::LayoutClauseContext *context) = 0;

    virtual antlrcpp::Any visitRangeClause(ClickHouseParser::RangeClauseContext *context) = 0;

    virtual antlrcpp::Any visitDictionarySettingsClause(ClickHouseParser::DictionarySettingsClauseContext *context) = 0;

    virtual antlrcpp::Any visitClusterClause(ClickHouseParser::ClusterClauseContext *context) = 0;

    virtual antlrcpp::Any visitUuidClause(ClickHouseParser::UuidClauseContext *context) = 0;

    virtual antlrcpp::Any visitDestinationClause(ClickHouseParser::DestinationClauseContext *context) = 0;

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

    virtual antlrcpp::Any visitTableElementExprConstraint(ClickHouseParser::TableElementExprConstraintContext *context) = 0;

    virtual antlrcpp::Any visitTableElementExprIndex(ClickHouseParser::TableElementExprIndexContext *context) = 0;

    virtual antlrcpp::Any visitTableColumnDfnt(ClickHouseParser::TableColumnDfntContext *context) = 0;

    virtual antlrcpp::Any visitTableColumnPropertyExpr(ClickHouseParser::TableColumnPropertyExprContext *context) = 0;

    virtual antlrcpp::Any visitTableIndexDfnt(ClickHouseParser::TableIndexDfntContext *context) = 0;

    virtual antlrcpp::Any visitCodecExpr(ClickHouseParser::CodecExprContext *context) = 0;

    virtual antlrcpp::Any visitCodecArgExpr(ClickHouseParser::CodecArgExprContext *context) = 0;

    virtual antlrcpp::Any visitTtlExpr(ClickHouseParser::TtlExprContext *context) = 0;

    virtual antlrcpp::Any visitDescribeStmt(ClickHouseParser::DescribeStmtContext *context) = 0;

    virtual antlrcpp::Any visitDropDatabaseStmt(ClickHouseParser::DropDatabaseStmtContext *context) = 0;

    virtual antlrcpp::Any visitDropTableStmt(ClickHouseParser::DropTableStmtContext *context) = 0;

    virtual antlrcpp::Any visitExistsStmt(ClickHouseParser::ExistsStmtContext *context) = 0;

    virtual antlrcpp::Any visitExplainStmt(ClickHouseParser::ExplainStmtContext *context) = 0;

    virtual antlrcpp::Any visitInsertStmt(ClickHouseParser::InsertStmtContext *context) = 0;

    virtual antlrcpp::Any visitColumnsClause(ClickHouseParser::ColumnsClauseContext *context) = 0;

    virtual antlrcpp::Any visitDataClauseFormat(ClickHouseParser::DataClauseFormatContext *context) = 0;

    virtual antlrcpp::Any visitDataClauseValues(ClickHouseParser::DataClauseValuesContext *context) = 0;

    virtual antlrcpp::Any visitDataClauseSelect(ClickHouseParser::DataClauseSelectContext *context) = 0;

    virtual antlrcpp::Any visitKillMutationStmt(ClickHouseParser::KillMutationStmtContext *context) = 0;

    virtual antlrcpp::Any visitOptimizeStmt(ClickHouseParser::OptimizeStmtContext *context) = 0;

    virtual antlrcpp::Any visitRenameStmt(ClickHouseParser::RenameStmtContext *context) = 0;

    virtual antlrcpp::Any visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext *context) = 0;

    virtual antlrcpp::Any visitSelectStmtWithParens(ClickHouseParser::SelectStmtWithParensContext *context) = 0;

    virtual antlrcpp::Any visitSelectStmt(ClickHouseParser::SelectStmtContext *context) = 0;

    virtual antlrcpp::Any visitWithClause(ClickHouseParser::WithClauseContext *context) = 0;

    virtual antlrcpp::Any visitTopClause(ClickHouseParser::TopClauseContext *context) = 0;

    virtual antlrcpp::Any visitFromClause(ClickHouseParser::FromClauseContext *context) = 0;

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

    virtual antlrcpp::Any visitSampleClause(ClickHouseParser::SampleClauseContext *context) = 0;

    virtual antlrcpp::Any visitLimitExpr(ClickHouseParser::LimitExprContext *context) = 0;

    virtual antlrcpp::Any visitOrderExprList(ClickHouseParser::OrderExprListContext *context) = 0;

    virtual antlrcpp::Any visitOrderExpr(ClickHouseParser::OrderExprContext *context) = 0;

    virtual antlrcpp::Any visitRatioExpr(ClickHouseParser::RatioExprContext *context) = 0;

    virtual antlrcpp::Any visitSettingExprList(ClickHouseParser::SettingExprListContext *context) = 0;

    virtual antlrcpp::Any visitSettingExpr(ClickHouseParser::SettingExprContext *context) = 0;

    virtual antlrcpp::Any visitSetStmt(ClickHouseParser::SetStmtContext *context) = 0;

    virtual antlrcpp::Any visitShowCreateDatabaseStmt(ClickHouseParser::ShowCreateDatabaseStmtContext *context) = 0;

    virtual antlrcpp::Any visitShowCreateDictionaryStmt(ClickHouseParser::ShowCreateDictionaryStmtContext *context) = 0;

    virtual antlrcpp::Any visitShowCreateTableStmt(ClickHouseParser::ShowCreateTableStmtContext *context) = 0;

    virtual antlrcpp::Any visitShowDatabasesStmt(ClickHouseParser::ShowDatabasesStmtContext *context) = 0;

    virtual antlrcpp::Any visitShowDictionariesStmt(ClickHouseParser::ShowDictionariesStmtContext *context) = 0;

    virtual antlrcpp::Any visitShowTablesStmt(ClickHouseParser::ShowTablesStmtContext *context) = 0;

    virtual antlrcpp::Any visitSystemStmt(ClickHouseParser::SystemStmtContext *context) = 0;

    virtual antlrcpp::Any visitTruncateStmt(ClickHouseParser::TruncateStmtContext *context) = 0;

    virtual antlrcpp::Any visitUseStmt(ClickHouseParser::UseStmtContext *context) = 0;

    virtual antlrcpp::Any visitWatchStmt(ClickHouseParser::WatchStmtContext *context) = 0;

    virtual antlrcpp::Any visitColumnTypeExprSimple(ClickHouseParser::ColumnTypeExprSimpleContext *context) = 0;

    virtual antlrcpp::Any visitColumnTypeExprNested(ClickHouseParser::ColumnTypeExprNestedContext *context) = 0;

    virtual antlrcpp::Any visitColumnTypeExprEnum(ClickHouseParser::ColumnTypeExprEnumContext *context) = 0;

    virtual antlrcpp::Any visitColumnTypeExprComplex(ClickHouseParser::ColumnTypeExprComplexContext *context) = 0;

    virtual antlrcpp::Any visitColumnTypeExprParam(ClickHouseParser::ColumnTypeExprParamContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprList(ClickHouseParser::ColumnExprListContext *context) = 0;

    virtual antlrcpp::Any visitColumnsExprAsterisk(ClickHouseParser::ColumnsExprAsteriskContext *context) = 0;

    virtual antlrcpp::Any visitColumnsExprSubquery(ClickHouseParser::ColumnsExprSubqueryContext *context) = 0;

    virtual antlrcpp::Any visitColumnsExprColumn(ClickHouseParser::ColumnsExprColumnContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTernaryOp(ClickHouseParser::ColumnExprTernaryOpContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprAlias(ClickHouseParser::ColumnExprAliasContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprExtract(ClickHouseParser::ColumnExprExtractContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprNegate(ClickHouseParser::ColumnExprNegateContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprSubquery(ClickHouseParser::ColumnExprSubqueryContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprArray(ClickHouseParser::ColumnExprArrayContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprSubstring(ClickHouseParser::ColumnExprSubstringContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprCast(ClickHouseParser::ColumnExprCastContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprOr(ClickHouseParser::ColumnExprOrContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprPrecedence1(ClickHouseParser::ColumnExprPrecedence1Context *context) = 0;

    virtual antlrcpp::Any visitColumnExprPrecedence2(ClickHouseParser::ColumnExprPrecedence2Context *context) = 0;

    virtual antlrcpp::Any visitColumnExprPrecedence3(ClickHouseParser::ColumnExprPrecedence3Context *context) = 0;

    virtual antlrcpp::Any visitColumnExprInterval(ClickHouseParser::ColumnExprIntervalContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprIsNull(ClickHouseParser::ColumnExprIsNullContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTrim(ClickHouseParser::ColumnExprTrimContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTuple(ClickHouseParser::ColumnExprTupleContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprArrayAccess(ClickHouseParser::ColumnExprArrayAccessContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprBetween(ClickHouseParser::ColumnExprBetweenContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprParens(ClickHouseParser::ColumnExprParensContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTimestamp(ClickHouseParser::ColumnExprTimestampContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprAnd(ClickHouseParser::ColumnExprAndContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprTupleAccess(ClickHouseParser::ColumnExprTupleAccessContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprCase(ClickHouseParser::ColumnExprCaseContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprDate(ClickHouseParser::ColumnExprDateContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprNot(ClickHouseParser::ColumnExprNotContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprIdentifier(ClickHouseParser::ColumnExprIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprFunction(ClickHouseParser::ColumnExprFunctionContext *context) = 0;

    virtual antlrcpp::Any visitColumnExprAsterisk(ClickHouseParser::ColumnExprAsteriskContext *context) = 0;

    virtual antlrcpp::Any visitColumnArgList(ClickHouseParser::ColumnArgListContext *context) = 0;

    virtual antlrcpp::Any visitColumnArgExpr(ClickHouseParser::ColumnArgExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnLambdaExpr(ClickHouseParser::ColumnLambdaExprContext *context) = 0;

    virtual antlrcpp::Any visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitNestedIdentifier(ClickHouseParser::NestedIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitTableExprSubquery(ClickHouseParser::TableExprSubqueryContext *context) = 0;

    virtual antlrcpp::Any visitTableExprAlias(ClickHouseParser::TableExprAliasContext *context) = 0;

    virtual antlrcpp::Any visitTableExprFunction(ClickHouseParser::TableExprFunctionContext *context) = 0;

    virtual antlrcpp::Any visitTableFunctionExpr(ClickHouseParser::TableFunctionExprContext *context) = 0;

    virtual antlrcpp::Any visitTableIdentifier(ClickHouseParser::TableIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitTableArgList(ClickHouseParser::TableArgListContext *context) = 0;

    virtual antlrcpp::Any visitTableArgExpr(ClickHouseParser::TableArgExprContext *context) = 0;

    virtual antlrcpp::Any visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *context) = 0;

    virtual antlrcpp::Any visitFloatingLiteral(ClickHouseParser::FloatingLiteralContext *context) = 0;

    virtual antlrcpp::Any visitNumberLiteral(ClickHouseParser::NumberLiteralContext *context) = 0;

    virtual antlrcpp::Any visitLiteral(ClickHouseParser::LiteralContext *context) = 0;

    virtual antlrcpp::Any visitInterval(ClickHouseParser::IntervalContext *context) = 0;

    virtual antlrcpp::Any visitKeyword(ClickHouseParser::KeywordContext *context) = 0;

    virtual antlrcpp::Any visitKeywordForAlias(ClickHouseParser::KeywordForAliasContext *context) = 0;

    virtual antlrcpp::Any visitAlias(ClickHouseParser::AliasContext *context) = 0;

    virtual antlrcpp::Any visitIdentifier(ClickHouseParser::IdentifierContext *context) = 0;

    virtual antlrcpp::Any visitIdentifierOrNull(ClickHouseParser::IdentifierOrNullContext *context) = 0;

    virtual antlrcpp::Any visitEnumValue(ClickHouseParser::EnumValueContext *context) = 0;


};

}  // namespace DB
