#pragma once

#include <Parsers/New/ClickHouseParserVisitor.h>


namespace DB {

class ParseTreeVisitor : public ClickHouseParserVisitor
{
public:
    virtual ~ParseTreeVisitor() override = default;

    // Top-level statements
    antlrcpp::Any visitQueryStmt(ClickHouseParser::QueryStmtContext * ctx) override;
    antlrcpp::Any visitQuery(ClickHouseParser::QueryContext * ctx) override;

    // AlterTableQuery
    antlrcpp::Any visitAlterTableClauseAddColumn(ClickHouseParser::AlterTableClauseAddColumnContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseAddIndex(ClickHouseParser::AlterTableClauseAddIndexContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseAttach(ClickHouseParser::AlterTableClauseAttachContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseClear(ClickHouseParser::AlterTableClauseClearContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseComment(ClickHouseParser::AlterTableClauseCommentContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseDelete(ClickHouseParser::AlterTableClauseDeleteContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseDetach(ClickHouseParser::AlterTableClauseDetachContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseDropColumn(ClickHouseParser::AlterTableClauseDropColumnContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseDropIndex(ClickHouseParser::AlterTableClauseDropIndexContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseDropPartition(ClickHouseParser::AlterTableClauseDropPartitionContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseFreezePartition(ClickHouseParser::AlterTableClauseFreezePartitionContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseModify(ClickHouseParser::AlterTableClauseModifyContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseModifyCodec(ClickHouseParser::AlterTableClauseModifyCodecContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseModifyComment(ClickHouseParser::AlterTableClauseModifyCommentContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseModifyOrderBy(ClickHouseParser::AlterTableClauseModifyOrderByContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseModifyRemove(ClickHouseParser::AlterTableClauseModifyRemoveContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseModifyTTL(ClickHouseParser::AlterTableClauseModifyTTLContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseMovePartition(ClickHouseParser::AlterTableClauseMovePartitionContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseRemoveTTL(ClickHouseParser::AlterTableClauseRemoveTTLContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseRename(ClickHouseParser::AlterTableClauseRenameContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseReplace(ClickHouseParser::AlterTableClauseReplaceContext * ctx) override;
    antlrcpp::Any visitAlterTableClauseUpdate(ClickHouseParser::AlterTableClauseUpdateContext * ctx) override;
    antlrcpp::Any visitAlterTableStmt(ClickHouseParser::AlterTableStmtContext * ctx) override;
    antlrcpp::Any visitAssignmentExpr(ClickHouseParser::AssignmentExprContext * ctx) override;
    antlrcpp::Any visitAssignmentExprList(ClickHouseParser::AssignmentExprListContext * ctx) override;
    antlrcpp::Any visitTableColumnPropertyType(ClickHouseParser::TableColumnPropertyTypeContext * ctx) override;

    // AttachQuery
    antlrcpp::Any visitAttachDictionaryStmt(ClickHouseParser::AttachDictionaryStmtContext * ctx) override;

    // CheckQuery
    antlrcpp::Any visitCheckStmt(ClickHouseParser::CheckStmtContext * ctx) override;

    // ColumnExpr
    antlrcpp::Any visitColumnExprAlias(ClickHouseParser::ColumnExprAliasContext * ctx) override;
    antlrcpp::Any visitColumnExprAnd(ClickHouseParser::ColumnExprAndContext * ctx) override;
    antlrcpp::Any visitColumnExprArray(ClickHouseParser::ColumnExprArrayContext * ctx) override;
    antlrcpp::Any visitColumnExprArrayAccess(ClickHouseParser::ColumnExprArrayAccessContext * ctx) override;
    antlrcpp::Any visitColumnExprAsterisk(ClickHouseParser::ColumnExprAsteriskContext * ctx) override;
    antlrcpp::Any visitColumnExprBetween(ClickHouseParser::ColumnExprBetweenContext * ctx) override;
    antlrcpp::Any visitColumnExprCase(ClickHouseParser::ColumnExprCaseContext * ctx) override;
    antlrcpp::Any visitColumnExprCast(ClickHouseParser::ColumnExprCastContext * ctx) override;
    antlrcpp::Any visitColumnExprDate(ClickHouseParser::ColumnExprDateContext * ctx) override;
    antlrcpp::Any visitColumnExprExtract(ClickHouseParser::ColumnExprExtractContext * ctx) override;
    antlrcpp::Any visitColumnExprFunction(ClickHouseParser::ColumnExprFunctionContext * ctx) override;
    antlrcpp::Any visitColumnExprIdentifier(ClickHouseParser::ColumnExprIdentifierContext * ctx) override;
    antlrcpp::Any visitColumnExprInterval(ClickHouseParser::ColumnExprIntervalContext * ctx) override;
    antlrcpp::Any visitColumnExprIsNull(ClickHouseParser::ColumnExprIsNullContext * ctx) override;
    antlrcpp::Any visitColumnExprList(ClickHouseParser::ColumnExprListContext * ctx) override;
    antlrcpp::Any visitColumnExprLiteral(ClickHouseParser::ColumnExprLiteralContext * ctx) override;
    antlrcpp::Any visitColumnExprNegate(ClickHouseParser::ColumnExprNegateContext * ctx) override;
    antlrcpp::Any visitColumnExprNot(ClickHouseParser::ColumnExprNotContext * ctx) override;
    antlrcpp::Any visitColumnExprOr(ClickHouseParser::ColumnExprOrContext * ctx) override;
    antlrcpp::Any visitColumnExprParens(ClickHouseParser::ColumnExprParensContext * ctx) override;
    antlrcpp::Any visitColumnExprPrecedence1(ClickHouseParser::ColumnExprPrecedence1Context * ctx) override;
    antlrcpp::Any visitColumnExprPrecedence2(ClickHouseParser::ColumnExprPrecedence2Context * ctx) override;
    antlrcpp::Any visitColumnExprPrecedence3(ClickHouseParser::ColumnExprPrecedence3Context * ctx) override;
    antlrcpp::Any visitColumnExprSubquery(ClickHouseParser::ColumnExprSubqueryContext * ctx) override;
    antlrcpp::Any visitColumnExprSubstring(ClickHouseParser::ColumnExprSubstringContext * ctx) override;
    antlrcpp::Any visitColumnExprTernaryOp(ClickHouseParser::ColumnExprTernaryOpContext * ctx) override;
    antlrcpp::Any visitColumnExprTimestamp(ClickHouseParser::ColumnExprTimestampContext * ctx) override;
    antlrcpp::Any visitColumnExprTrim(ClickHouseParser::ColumnExprTrimContext * ctx) override;
    antlrcpp::Any visitColumnExprTuple(ClickHouseParser::ColumnExprTupleContext * ctx) override;
    antlrcpp::Any visitColumnExprTupleAccess(ClickHouseParser::ColumnExprTupleAccessContext * ctx) override;

    // ColumnTypeExpr
    antlrcpp::Any visitColumnTypeExprSimple(ClickHouseParser::ColumnTypeExprSimpleContext * ctx) override;
    antlrcpp::Any visitColumnTypeExprParam(ClickHouseParser::ColumnTypeExprParamContext * ctx) override;
    antlrcpp::Any visitColumnTypeExprEnum(ClickHouseParser::ColumnTypeExprEnumContext * ctx) override;
    antlrcpp::Any visitColumnTypeExprComplex(ClickHouseParser::ColumnTypeExprComplexContext * ctx) override;
    antlrcpp::Any visitColumnTypeExprNested(ClickHouseParser::ColumnTypeExprNestedContext * ctx) override;

    // CreateDatabaseQuery
    antlrcpp::Any visitCreateDatabaseStmt(ClickHouseParser::CreateDatabaseStmtContext * ctx) override;

    // CreateDictionaryQuery
    antlrcpp::Any visitCreateDictionaryStmt(ClickHouseParser::CreateDictionaryStmtContext * ctx) override;
    antlrcpp::Any visitDictionaryArgExpr(ClickHouseParser::DictionaryArgExprContext * ctx) override;
    antlrcpp::Any visitDictionaryAttrDfnt(ClickHouseParser::DictionaryAttrDfntContext * ctx) override;
    antlrcpp::Any visitDictionaryEngineClause(ClickHouseParser::DictionaryEngineClauseContext * ctx) override;
    antlrcpp::Any visitDictionaryPrimaryKeyClause(ClickHouseParser::DictionaryPrimaryKeyClauseContext * ctx) override;
    antlrcpp::Any visitDictionarySchemaClause(ClickHouseParser::DictionarySchemaClauseContext * ctx) override;
    antlrcpp::Any visitDictionarySettingsClause(ClickHouseParser::DictionarySettingsClauseContext * ctx) override;
    antlrcpp::Any visitLayoutClause(ClickHouseParser::LayoutClauseContext * ctx) override;
    antlrcpp::Any visitLifetimeClause(ClickHouseParser::LifetimeClauseContext * ctx) override;
    antlrcpp::Any visitRangeClause(ClickHouseParser::RangeClauseContext * ctx) override;
    antlrcpp::Any visitSourceClause(ClickHouseParser::SourceClauseContext * ctx) override;

    // CreateLiveViewQuery
    antlrcpp::Any visitCreateLiveViewStmt(ClickHouseParser::CreateLiveViewStmtContext * ctx) override;

    // CreateMaterializedViewQuery
    antlrcpp::Any visitCreateMaterializedViewStmt(ClickHouseParser::CreateMaterializedViewStmtContext * ctx) override;

    // CreateTableQuery
    antlrcpp::Any visitClusterClause(ClickHouseParser::ClusterClauseContext * ctx) override;
    antlrcpp::Any visitCreateTableStmt(ClickHouseParser::CreateTableStmtContext * ctx) override;
    antlrcpp::Any visitUuidClause(ClickHouseParser::UuidClauseContext * ctx) override;

    // CreateViewQuery
    antlrcpp::Any visitCreateViewStmt(ClickHouseParser::CreateViewStmtContext * ctx) override;

    // DescribeQuery
    antlrcpp::Any visitDescribeStmt(ClickHouseParser::DescribeStmtContext * ctx) override;

    // DropQuery
    antlrcpp::Any visitDropDatabaseStmt(ClickHouseParser::DropDatabaseStmtContext * ctx) override;
    antlrcpp::Any visitDropTableStmt(ClickHouseParser::DropTableStmtContext * ctx) override;

    // EngineExpr
    antlrcpp::Any visitEngineClause(ClickHouseParser::EngineClauseContext * ctx) override;
    antlrcpp::Any visitEngineExpr(ClickHouseParser::EngineExprContext * ctx) override;
    antlrcpp::Any visitPartitionByClause(ClickHouseParser::PartitionByClauseContext * ctx) override;
    antlrcpp::Any visitPrimaryKeyClause(ClickHouseParser::PrimaryKeyClauseContext * ctx) override;
    antlrcpp::Any visitSampleByClause(ClickHouseParser::SampleByClauseContext * ctx) override;
    antlrcpp::Any visitTtlClause(ClickHouseParser::TtlClauseContext * ctx) override;
    antlrcpp::Any visitTtlExpr(ClickHouseParser::TtlExprContext * ctx) override;

    // ExistsQuery
    antlrcpp::Any visitExistsStmt(ClickHouseParser::ExistsStmtContext * ctx) override;

    // ExplainQuery
    antlrcpp::Any visitExplainStmt(ClickHouseParser::ExplainStmtContext * ctx) override;

    // Identifier
    antlrcpp::Any visitTableIdentifier(ClickHouseParser::TableIdentifierContext * ctx) override;

    // InsertQuery
    antlrcpp::Any visitColumnsClause(ClickHouseParser::ColumnsClauseContext * ctx) override;
    antlrcpp::Any visitDataClauseFormat(ClickHouseParser::DataClauseFormatContext * ctx) override;
    antlrcpp::Any visitDataClauseSelect(ClickHouseParser::DataClauseSelectContext * ctx) override;
    antlrcpp::Any visitDataClauseValues(ClickHouseParser::DataClauseValuesContext * ctx) override;
    antlrcpp::Any visitInsertStmt(ClickHouseParser::InsertStmtContext * ctx) override;

    // KillQuery
    antlrcpp::Any visitKillMutationStmt(ClickHouseParser::KillMutationStmtContext * ctx) override;

    // OptimizeQuery
    antlrcpp::Any visitOptimizeStmt(ClickHouseParser::OptimizeStmtContext * ctx) override;

    // RenameQuery
    antlrcpp::Any visitRenameStmt(ClickHouseParser::RenameStmtContext * ctx) override;

    // SelectUnionQuery
    antlrcpp::Any visitSelectStmt(ClickHouseParser::SelectStmtContext * ctx) override;
    antlrcpp::Any visitSelectStmtWithParens(ClickHouseParser::SelectStmtWithParensContext * ctx) override;
    antlrcpp::Any visitSelectUnionStmt(ClickHouseParser::SelectUnionStmtContext * ctx) override;

    // SetQuery
    antlrcpp::Any visitSetStmt(ClickHouseParser::SetStmtContext * ctx) override;

    // ShowCreateQuery
    antlrcpp::Any visitShowCreateDatabaseStmt(ClickHouseParser::ShowCreateDatabaseStmtContext * ctx) override;
    antlrcpp::Any visitShowCreateDictionaryStmt(ClickHouseParser::ShowCreateDictionaryStmtContext * ctx) override;
    antlrcpp::Any visitShowCreateTableStmt(ClickHouseParser::ShowCreateTableStmtContext * ctx) override;

    // ShowQuery
    antlrcpp::Any visitShowDatabasesStmt(ClickHouseParser::ShowDatabasesStmtContext * ctx) override;
    antlrcpp::Any visitShowDictionariesStmt(ClickHouseParser::ShowDictionariesStmtContext * ctx) override;
    antlrcpp::Any visitShowTablesStmt(ClickHouseParser::ShowTablesStmtContext * ctx) override;

    // SystemQuery
    antlrcpp::Any visitSystemStmt(ClickHouseParser::SystemStmtContext * ctx) override;

    // TableElementExpr
    antlrcpp::Any visitCodecArgExpr(ClickHouseParser::CodecArgExprContext * ctx) override;
    antlrcpp::Any visitCodecExpr(ClickHouseParser::CodecExprContext * ctx) override;
    antlrcpp::Any visitTableColumnDfnt(ClickHouseParser::TableColumnDfntContext * ctx) override;
    antlrcpp::Any visitTableColumnPropertyExpr(ClickHouseParser::TableColumnPropertyExprContext * ctx) override;
    antlrcpp::Any visitTableElementExprColumn(ClickHouseParser::TableElementExprColumnContext * ctx) override;
    antlrcpp::Any visitTableElementExprConstraint(ClickHouseParser::TableElementExprConstraintContext * ctx) override;
    antlrcpp::Any visitTableElementExprIndex(ClickHouseParser::TableElementExprIndexContext * ctx) override;
    antlrcpp::Any visitTableIndexDfnt(ClickHouseParser::TableIndexDfntContext * ctx) override;

    // TableExpr
    antlrcpp::Any visitTableArgExpr(ClickHouseParser::TableArgExprContext * ctx) override;
    antlrcpp::Any visitTableArgList(ClickHouseParser::TableArgListContext * ctx) override;
    antlrcpp::Any visitTableExprAlias(ClickHouseParser::TableExprAliasContext * ctx) override;
    antlrcpp::Any visitTableExprFunction(ClickHouseParser::TableExprFunctionContext * ctx) override;
    antlrcpp::Any visitTableExprIdentifier(ClickHouseParser::TableExprIdentifierContext * ctx) override;
    antlrcpp::Any visitTableExprSubquery(ClickHouseParser::TableExprSubqueryContext * ctx) override;
    antlrcpp::Any visitTableFunctionExpr(ClickHouseParser::TableFunctionExprContext * ctx) override;

    // TruncateQuery
    antlrcpp::Any visitTruncateStmt(ClickHouseParser::TruncateStmtContext * ctx) override;

    // UseQuery
    antlrcpp::Any visitUseStmt(ClickHouseParser::UseStmtContext * ctx) override;

    // WatchQuery
    antlrcpp::Any visitWatchStmt(ClickHouseParser::WatchStmtContext * ctx) override;

    // TODO: sort methods below this comment.

    // CREATE clauses

    antlrcpp::Any visitDestinationClause(ClickHouseParser::DestinationClauseContext *ctx) override;
    antlrcpp::Any visitSchemaDescriptionClause(ClickHouseParser::SchemaDescriptionClauseContext *ctx) override;
    antlrcpp::Any visitSchemaAsTableClause(ClickHouseParser::SchemaAsTableClauseContext *ctx) override;
    antlrcpp::Any visitSchemaAsFunctionClause(ClickHouseParser::SchemaAsFunctionClauseContext *ctx) override;
    antlrcpp::Any visitSubqueryClause(ClickHouseParser::SubqueryClauseContext *ctx) override;

    // OPTIMIZE clauses

    antlrcpp::Any visitPartitionClause(ClickHouseParser::PartitionClauseContext *ctx) override;  // returns |PtrTo<PartitionExprList>|

    // SELECT clauses

    antlrcpp::Any visitWithClause(ClickHouseParser::WithClauseContext *ctx) override;
    antlrcpp::Any visitTopClause(ClickHouseParser::TopClauseContext * ctx) override;
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
    antlrcpp::Any visitColumnIdentifier(ClickHouseParser::ColumnIdentifierContext *ctx) override;
    antlrcpp::Any visitColumnLambdaExpr(ClickHouseParser::ColumnLambdaExprContext *ctx) override;
    antlrcpp::Any visitColumnsExprAsterisk(ClickHouseParser::ColumnsExprAsteriskContext *ctx) override;
    antlrcpp::Any visitColumnsExprColumn(ClickHouseParser::ColumnsExprColumnContext *ctx) override;
    antlrcpp::Any visitColumnsExprSubquery(ClickHouseParser::ColumnsExprSubqueryContext *ctx) override;
    antlrcpp::Any visitNestedIdentifier(ClickHouseParser::NestedIdentifierContext *ctx) override;

    // Database expressions

    antlrcpp::Any visitDatabaseIdentifier(ClickHouseParser::DatabaseIdentifierContext *ctx) override;

    // Basic expressions (alphabetically)

    antlrcpp::Any visitAlias(ClickHouseParser::AliasContext * ctx) override;
    antlrcpp::Any visitEnumValue(ClickHouseParser::EnumValueContext *ctx) override;
    antlrcpp::Any visitFloatingLiteral(ClickHouseParser::FloatingLiteralContext *ctx) override;
    antlrcpp::Any visitIdentifier(ClickHouseParser::IdentifierContext *ctx) override;
    antlrcpp::Any visitIdentifierOrNull(ClickHouseParser::IdentifierOrNullContext *ctx) override;
    antlrcpp::Any visitInterval(ClickHouseParser::IntervalContext * ctx) override;
    antlrcpp::Any visitKeyword(ClickHouseParser::KeywordContext *ctx) override;
    antlrcpp::Any visitKeywordForAlias(ClickHouseParser::KeywordForAliasContext * ctx) override;
    antlrcpp::Any visitLiteral(ClickHouseParser::LiteralContext *ctx) override;
    antlrcpp::Any visitNumberLiteral(ClickHouseParser::NumberLiteralContext *ctx) override;
};

}
