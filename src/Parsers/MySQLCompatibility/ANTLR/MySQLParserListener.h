
#include "SqlMode.h"
#include "MySQLBaseParser.h"
  // import { MySQLBaseParser } from './MySQLBaseParser'
  // import { SqlMode } from './common'


// Generated from MySQLParser.g4 by ANTLR 4.8

#pragma once


#include "antlr4-runtime.h"
#include "MySQLParser.h"


/**
 * This interface defines an abstract listener for a parse tree produced by MySQLParser.
 */
class  MySQLParserListener : public antlr4::tree::ParseTreeListener {
public:

  virtual void enterQuery(MySQLParser::QueryContext *ctx) = 0;
  virtual void exitQuery(MySQLParser::QueryContext *ctx) = 0;

  virtual void enterSimpleStatement(MySQLParser::SimpleStatementContext *ctx) = 0;
  virtual void exitSimpleStatement(MySQLParser::SimpleStatementContext *ctx) = 0;

  virtual void enterAlterStatement(MySQLParser::AlterStatementContext *ctx) = 0;
  virtual void exitAlterStatement(MySQLParser::AlterStatementContext *ctx) = 0;

  virtual void enterAlterDatabase(MySQLParser::AlterDatabaseContext *ctx) = 0;
  virtual void exitAlterDatabase(MySQLParser::AlterDatabaseContext *ctx) = 0;

  virtual void enterAlterEvent(MySQLParser::AlterEventContext *ctx) = 0;
  virtual void exitAlterEvent(MySQLParser::AlterEventContext *ctx) = 0;

  virtual void enterAlterLogfileGroup(MySQLParser::AlterLogfileGroupContext *ctx) = 0;
  virtual void exitAlterLogfileGroup(MySQLParser::AlterLogfileGroupContext *ctx) = 0;

  virtual void enterAlterLogfileGroupOptions(MySQLParser::AlterLogfileGroupOptionsContext *ctx) = 0;
  virtual void exitAlterLogfileGroupOptions(MySQLParser::AlterLogfileGroupOptionsContext *ctx) = 0;

  virtual void enterAlterLogfileGroupOption(MySQLParser::AlterLogfileGroupOptionContext *ctx) = 0;
  virtual void exitAlterLogfileGroupOption(MySQLParser::AlterLogfileGroupOptionContext *ctx) = 0;

  virtual void enterAlterServer(MySQLParser::AlterServerContext *ctx) = 0;
  virtual void exitAlterServer(MySQLParser::AlterServerContext *ctx) = 0;

  virtual void enterAlterTable(MySQLParser::AlterTableContext *ctx) = 0;
  virtual void exitAlterTable(MySQLParser::AlterTableContext *ctx) = 0;

  virtual void enterAlterTableActions(MySQLParser::AlterTableActionsContext *ctx) = 0;
  virtual void exitAlterTableActions(MySQLParser::AlterTableActionsContext *ctx) = 0;

  virtual void enterAlterCommandList(MySQLParser::AlterCommandListContext *ctx) = 0;
  virtual void exitAlterCommandList(MySQLParser::AlterCommandListContext *ctx) = 0;

  virtual void enterAlterCommandsModifierList(MySQLParser::AlterCommandsModifierListContext *ctx) = 0;
  virtual void exitAlterCommandsModifierList(MySQLParser::AlterCommandsModifierListContext *ctx) = 0;

  virtual void enterStandaloneAlterCommands(MySQLParser::StandaloneAlterCommandsContext *ctx) = 0;
  virtual void exitStandaloneAlterCommands(MySQLParser::StandaloneAlterCommandsContext *ctx) = 0;

  virtual void enterAlterPartition(MySQLParser::AlterPartitionContext *ctx) = 0;
  virtual void exitAlterPartition(MySQLParser::AlterPartitionContext *ctx) = 0;

  virtual void enterAlterList(MySQLParser::AlterListContext *ctx) = 0;
  virtual void exitAlterList(MySQLParser::AlterListContext *ctx) = 0;

  virtual void enterAlterCommandsModifier(MySQLParser::AlterCommandsModifierContext *ctx) = 0;
  virtual void exitAlterCommandsModifier(MySQLParser::AlterCommandsModifierContext *ctx) = 0;

  virtual void enterAlterListItem(MySQLParser::AlterListItemContext *ctx) = 0;
  virtual void exitAlterListItem(MySQLParser::AlterListItemContext *ctx) = 0;

  virtual void enterPlace(MySQLParser::PlaceContext *ctx) = 0;
  virtual void exitPlace(MySQLParser::PlaceContext *ctx) = 0;

  virtual void enterRestrict(MySQLParser::RestrictContext *ctx) = 0;
  virtual void exitRestrict(MySQLParser::RestrictContext *ctx) = 0;

  virtual void enterAlterOrderList(MySQLParser::AlterOrderListContext *ctx) = 0;
  virtual void exitAlterOrderList(MySQLParser::AlterOrderListContext *ctx) = 0;

  virtual void enterAlterAlgorithmOption(MySQLParser::AlterAlgorithmOptionContext *ctx) = 0;
  virtual void exitAlterAlgorithmOption(MySQLParser::AlterAlgorithmOptionContext *ctx) = 0;

  virtual void enterAlterLockOption(MySQLParser::AlterLockOptionContext *ctx) = 0;
  virtual void exitAlterLockOption(MySQLParser::AlterLockOptionContext *ctx) = 0;

  virtual void enterIndexLockAndAlgorithm(MySQLParser::IndexLockAndAlgorithmContext *ctx) = 0;
  virtual void exitIndexLockAndAlgorithm(MySQLParser::IndexLockAndAlgorithmContext *ctx) = 0;

  virtual void enterWithValidation(MySQLParser::WithValidationContext *ctx) = 0;
  virtual void exitWithValidation(MySQLParser::WithValidationContext *ctx) = 0;

  virtual void enterRemovePartitioning(MySQLParser::RemovePartitioningContext *ctx) = 0;
  virtual void exitRemovePartitioning(MySQLParser::RemovePartitioningContext *ctx) = 0;

  virtual void enterAllOrPartitionNameList(MySQLParser::AllOrPartitionNameListContext *ctx) = 0;
  virtual void exitAllOrPartitionNameList(MySQLParser::AllOrPartitionNameListContext *ctx) = 0;

  virtual void enterReorgPartitionRule(MySQLParser::ReorgPartitionRuleContext *ctx) = 0;
  virtual void exitReorgPartitionRule(MySQLParser::ReorgPartitionRuleContext *ctx) = 0;

  virtual void enterAlterTablespace(MySQLParser::AlterTablespaceContext *ctx) = 0;
  virtual void exitAlterTablespace(MySQLParser::AlterTablespaceContext *ctx) = 0;

  virtual void enterAlterUndoTablespace(MySQLParser::AlterUndoTablespaceContext *ctx) = 0;
  virtual void exitAlterUndoTablespace(MySQLParser::AlterUndoTablespaceContext *ctx) = 0;

  virtual void enterUndoTableSpaceOptions(MySQLParser::UndoTableSpaceOptionsContext *ctx) = 0;
  virtual void exitUndoTableSpaceOptions(MySQLParser::UndoTableSpaceOptionsContext *ctx) = 0;

  virtual void enterUndoTableSpaceOption(MySQLParser::UndoTableSpaceOptionContext *ctx) = 0;
  virtual void exitUndoTableSpaceOption(MySQLParser::UndoTableSpaceOptionContext *ctx) = 0;

  virtual void enterAlterTablespaceOptions(MySQLParser::AlterTablespaceOptionsContext *ctx) = 0;
  virtual void exitAlterTablespaceOptions(MySQLParser::AlterTablespaceOptionsContext *ctx) = 0;

  virtual void enterAlterTablespaceOption(MySQLParser::AlterTablespaceOptionContext *ctx) = 0;
  virtual void exitAlterTablespaceOption(MySQLParser::AlterTablespaceOptionContext *ctx) = 0;

  virtual void enterChangeTablespaceOption(MySQLParser::ChangeTablespaceOptionContext *ctx) = 0;
  virtual void exitChangeTablespaceOption(MySQLParser::ChangeTablespaceOptionContext *ctx) = 0;

  virtual void enterAlterView(MySQLParser::AlterViewContext *ctx) = 0;
  virtual void exitAlterView(MySQLParser::AlterViewContext *ctx) = 0;

  virtual void enterViewTail(MySQLParser::ViewTailContext *ctx) = 0;
  virtual void exitViewTail(MySQLParser::ViewTailContext *ctx) = 0;

  virtual void enterViewSelect(MySQLParser::ViewSelectContext *ctx) = 0;
  virtual void exitViewSelect(MySQLParser::ViewSelectContext *ctx) = 0;

  virtual void enterViewCheckOption(MySQLParser::ViewCheckOptionContext *ctx) = 0;
  virtual void exitViewCheckOption(MySQLParser::ViewCheckOptionContext *ctx) = 0;

  virtual void enterCreateStatement(MySQLParser::CreateStatementContext *ctx) = 0;
  virtual void exitCreateStatement(MySQLParser::CreateStatementContext *ctx) = 0;

  virtual void enterCreateDatabase(MySQLParser::CreateDatabaseContext *ctx) = 0;
  virtual void exitCreateDatabase(MySQLParser::CreateDatabaseContext *ctx) = 0;

  virtual void enterCreateDatabaseOption(MySQLParser::CreateDatabaseOptionContext *ctx) = 0;
  virtual void exitCreateDatabaseOption(MySQLParser::CreateDatabaseOptionContext *ctx) = 0;

  virtual void enterCreateTable(MySQLParser::CreateTableContext *ctx) = 0;
  virtual void exitCreateTable(MySQLParser::CreateTableContext *ctx) = 0;

  virtual void enterTableElementList(MySQLParser::TableElementListContext *ctx) = 0;
  virtual void exitTableElementList(MySQLParser::TableElementListContext *ctx) = 0;

  virtual void enterTableElement(MySQLParser::TableElementContext *ctx) = 0;
  virtual void exitTableElement(MySQLParser::TableElementContext *ctx) = 0;

  virtual void enterDuplicateAsQueryExpression(MySQLParser::DuplicateAsQueryExpressionContext *ctx) = 0;
  virtual void exitDuplicateAsQueryExpression(MySQLParser::DuplicateAsQueryExpressionContext *ctx) = 0;

  virtual void enterQueryExpressionOrParens(MySQLParser::QueryExpressionOrParensContext *ctx) = 0;
  virtual void exitQueryExpressionOrParens(MySQLParser::QueryExpressionOrParensContext *ctx) = 0;

  virtual void enterCreateRoutine(MySQLParser::CreateRoutineContext *ctx) = 0;
  virtual void exitCreateRoutine(MySQLParser::CreateRoutineContext *ctx) = 0;

  virtual void enterCreateProcedure(MySQLParser::CreateProcedureContext *ctx) = 0;
  virtual void exitCreateProcedure(MySQLParser::CreateProcedureContext *ctx) = 0;

  virtual void enterCreateFunction(MySQLParser::CreateFunctionContext *ctx) = 0;
  virtual void exitCreateFunction(MySQLParser::CreateFunctionContext *ctx) = 0;

  virtual void enterCreateUdf(MySQLParser::CreateUdfContext *ctx) = 0;
  virtual void exitCreateUdf(MySQLParser::CreateUdfContext *ctx) = 0;

  virtual void enterRoutineCreateOption(MySQLParser::RoutineCreateOptionContext *ctx) = 0;
  virtual void exitRoutineCreateOption(MySQLParser::RoutineCreateOptionContext *ctx) = 0;

  virtual void enterRoutineAlterOptions(MySQLParser::RoutineAlterOptionsContext *ctx) = 0;
  virtual void exitRoutineAlterOptions(MySQLParser::RoutineAlterOptionsContext *ctx) = 0;

  virtual void enterRoutineOption(MySQLParser::RoutineOptionContext *ctx) = 0;
  virtual void exitRoutineOption(MySQLParser::RoutineOptionContext *ctx) = 0;

  virtual void enterCreateIndex(MySQLParser::CreateIndexContext *ctx) = 0;
  virtual void exitCreateIndex(MySQLParser::CreateIndexContext *ctx) = 0;

  virtual void enterIndexNameAndType(MySQLParser::IndexNameAndTypeContext *ctx) = 0;
  virtual void exitIndexNameAndType(MySQLParser::IndexNameAndTypeContext *ctx) = 0;

  virtual void enterCreateIndexTarget(MySQLParser::CreateIndexTargetContext *ctx) = 0;
  virtual void exitCreateIndexTarget(MySQLParser::CreateIndexTargetContext *ctx) = 0;

  virtual void enterCreateLogfileGroup(MySQLParser::CreateLogfileGroupContext *ctx) = 0;
  virtual void exitCreateLogfileGroup(MySQLParser::CreateLogfileGroupContext *ctx) = 0;

  virtual void enterLogfileGroupOptions(MySQLParser::LogfileGroupOptionsContext *ctx) = 0;
  virtual void exitLogfileGroupOptions(MySQLParser::LogfileGroupOptionsContext *ctx) = 0;

  virtual void enterLogfileGroupOption(MySQLParser::LogfileGroupOptionContext *ctx) = 0;
  virtual void exitLogfileGroupOption(MySQLParser::LogfileGroupOptionContext *ctx) = 0;

  virtual void enterCreateServer(MySQLParser::CreateServerContext *ctx) = 0;
  virtual void exitCreateServer(MySQLParser::CreateServerContext *ctx) = 0;

  virtual void enterServerOptions(MySQLParser::ServerOptionsContext *ctx) = 0;
  virtual void exitServerOptions(MySQLParser::ServerOptionsContext *ctx) = 0;

  virtual void enterServerOption(MySQLParser::ServerOptionContext *ctx) = 0;
  virtual void exitServerOption(MySQLParser::ServerOptionContext *ctx) = 0;

  virtual void enterCreateTablespace(MySQLParser::CreateTablespaceContext *ctx) = 0;
  virtual void exitCreateTablespace(MySQLParser::CreateTablespaceContext *ctx) = 0;

  virtual void enterCreateUndoTablespace(MySQLParser::CreateUndoTablespaceContext *ctx) = 0;
  virtual void exitCreateUndoTablespace(MySQLParser::CreateUndoTablespaceContext *ctx) = 0;

  virtual void enterTsDataFileName(MySQLParser::TsDataFileNameContext *ctx) = 0;
  virtual void exitTsDataFileName(MySQLParser::TsDataFileNameContext *ctx) = 0;

  virtual void enterTsDataFile(MySQLParser::TsDataFileContext *ctx) = 0;
  virtual void exitTsDataFile(MySQLParser::TsDataFileContext *ctx) = 0;

  virtual void enterTablespaceOptions(MySQLParser::TablespaceOptionsContext *ctx) = 0;
  virtual void exitTablespaceOptions(MySQLParser::TablespaceOptionsContext *ctx) = 0;

  virtual void enterTablespaceOption(MySQLParser::TablespaceOptionContext *ctx) = 0;
  virtual void exitTablespaceOption(MySQLParser::TablespaceOptionContext *ctx) = 0;

  virtual void enterTsOptionInitialSize(MySQLParser::TsOptionInitialSizeContext *ctx) = 0;
  virtual void exitTsOptionInitialSize(MySQLParser::TsOptionInitialSizeContext *ctx) = 0;

  virtual void enterTsOptionUndoRedoBufferSize(MySQLParser::TsOptionUndoRedoBufferSizeContext *ctx) = 0;
  virtual void exitTsOptionUndoRedoBufferSize(MySQLParser::TsOptionUndoRedoBufferSizeContext *ctx) = 0;

  virtual void enterTsOptionAutoextendSize(MySQLParser::TsOptionAutoextendSizeContext *ctx) = 0;
  virtual void exitTsOptionAutoextendSize(MySQLParser::TsOptionAutoextendSizeContext *ctx) = 0;

  virtual void enterTsOptionMaxSize(MySQLParser::TsOptionMaxSizeContext *ctx) = 0;
  virtual void exitTsOptionMaxSize(MySQLParser::TsOptionMaxSizeContext *ctx) = 0;

  virtual void enterTsOptionExtentSize(MySQLParser::TsOptionExtentSizeContext *ctx) = 0;
  virtual void exitTsOptionExtentSize(MySQLParser::TsOptionExtentSizeContext *ctx) = 0;

  virtual void enterTsOptionNodegroup(MySQLParser::TsOptionNodegroupContext *ctx) = 0;
  virtual void exitTsOptionNodegroup(MySQLParser::TsOptionNodegroupContext *ctx) = 0;

  virtual void enterTsOptionEngine(MySQLParser::TsOptionEngineContext *ctx) = 0;
  virtual void exitTsOptionEngine(MySQLParser::TsOptionEngineContext *ctx) = 0;

  virtual void enterTsOptionWait(MySQLParser::TsOptionWaitContext *ctx) = 0;
  virtual void exitTsOptionWait(MySQLParser::TsOptionWaitContext *ctx) = 0;

  virtual void enterTsOptionComment(MySQLParser::TsOptionCommentContext *ctx) = 0;
  virtual void exitTsOptionComment(MySQLParser::TsOptionCommentContext *ctx) = 0;

  virtual void enterTsOptionFileblockSize(MySQLParser::TsOptionFileblockSizeContext *ctx) = 0;
  virtual void exitTsOptionFileblockSize(MySQLParser::TsOptionFileblockSizeContext *ctx) = 0;

  virtual void enterTsOptionEncryption(MySQLParser::TsOptionEncryptionContext *ctx) = 0;
  virtual void exitTsOptionEncryption(MySQLParser::TsOptionEncryptionContext *ctx) = 0;

  virtual void enterCreateView(MySQLParser::CreateViewContext *ctx) = 0;
  virtual void exitCreateView(MySQLParser::CreateViewContext *ctx) = 0;

  virtual void enterViewReplaceOrAlgorithm(MySQLParser::ViewReplaceOrAlgorithmContext *ctx) = 0;
  virtual void exitViewReplaceOrAlgorithm(MySQLParser::ViewReplaceOrAlgorithmContext *ctx) = 0;

  virtual void enterViewAlgorithm(MySQLParser::ViewAlgorithmContext *ctx) = 0;
  virtual void exitViewAlgorithm(MySQLParser::ViewAlgorithmContext *ctx) = 0;

  virtual void enterViewSuid(MySQLParser::ViewSuidContext *ctx) = 0;
  virtual void exitViewSuid(MySQLParser::ViewSuidContext *ctx) = 0;

  virtual void enterCreateTrigger(MySQLParser::CreateTriggerContext *ctx) = 0;
  virtual void exitCreateTrigger(MySQLParser::CreateTriggerContext *ctx) = 0;

  virtual void enterTriggerFollowsPrecedesClause(MySQLParser::TriggerFollowsPrecedesClauseContext *ctx) = 0;
  virtual void exitTriggerFollowsPrecedesClause(MySQLParser::TriggerFollowsPrecedesClauseContext *ctx) = 0;

  virtual void enterCreateEvent(MySQLParser::CreateEventContext *ctx) = 0;
  virtual void exitCreateEvent(MySQLParser::CreateEventContext *ctx) = 0;

  virtual void enterCreateRole(MySQLParser::CreateRoleContext *ctx) = 0;
  virtual void exitCreateRole(MySQLParser::CreateRoleContext *ctx) = 0;

  virtual void enterCreateSpatialReference(MySQLParser::CreateSpatialReferenceContext *ctx) = 0;
  virtual void exitCreateSpatialReference(MySQLParser::CreateSpatialReferenceContext *ctx) = 0;

  virtual void enterSrsAttribute(MySQLParser::SrsAttributeContext *ctx) = 0;
  virtual void exitSrsAttribute(MySQLParser::SrsAttributeContext *ctx) = 0;

  virtual void enterDropStatement(MySQLParser::DropStatementContext *ctx) = 0;
  virtual void exitDropStatement(MySQLParser::DropStatementContext *ctx) = 0;

  virtual void enterDropDatabase(MySQLParser::DropDatabaseContext *ctx) = 0;
  virtual void exitDropDatabase(MySQLParser::DropDatabaseContext *ctx) = 0;

  virtual void enterDropEvent(MySQLParser::DropEventContext *ctx) = 0;
  virtual void exitDropEvent(MySQLParser::DropEventContext *ctx) = 0;

  virtual void enterDropFunction(MySQLParser::DropFunctionContext *ctx) = 0;
  virtual void exitDropFunction(MySQLParser::DropFunctionContext *ctx) = 0;

  virtual void enterDropProcedure(MySQLParser::DropProcedureContext *ctx) = 0;
  virtual void exitDropProcedure(MySQLParser::DropProcedureContext *ctx) = 0;

  virtual void enterDropIndex(MySQLParser::DropIndexContext *ctx) = 0;
  virtual void exitDropIndex(MySQLParser::DropIndexContext *ctx) = 0;

  virtual void enterDropLogfileGroup(MySQLParser::DropLogfileGroupContext *ctx) = 0;
  virtual void exitDropLogfileGroup(MySQLParser::DropLogfileGroupContext *ctx) = 0;

  virtual void enterDropLogfileGroupOption(MySQLParser::DropLogfileGroupOptionContext *ctx) = 0;
  virtual void exitDropLogfileGroupOption(MySQLParser::DropLogfileGroupOptionContext *ctx) = 0;

  virtual void enterDropServer(MySQLParser::DropServerContext *ctx) = 0;
  virtual void exitDropServer(MySQLParser::DropServerContext *ctx) = 0;

  virtual void enterDropTable(MySQLParser::DropTableContext *ctx) = 0;
  virtual void exitDropTable(MySQLParser::DropTableContext *ctx) = 0;

  virtual void enterDropTableSpace(MySQLParser::DropTableSpaceContext *ctx) = 0;
  virtual void exitDropTableSpace(MySQLParser::DropTableSpaceContext *ctx) = 0;

  virtual void enterDropTrigger(MySQLParser::DropTriggerContext *ctx) = 0;
  virtual void exitDropTrigger(MySQLParser::DropTriggerContext *ctx) = 0;

  virtual void enterDropView(MySQLParser::DropViewContext *ctx) = 0;
  virtual void exitDropView(MySQLParser::DropViewContext *ctx) = 0;

  virtual void enterDropRole(MySQLParser::DropRoleContext *ctx) = 0;
  virtual void exitDropRole(MySQLParser::DropRoleContext *ctx) = 0;

  virtual void enterDropSpatialReference(MySQLParser::DropSpatialReferenceContext *ctx) = 0;
  virtual void exitDropSpatialReference(MySQLParser::DropSpatialReferenceContext *ctx) = 0;

  virtual void enterDropUndoTablespace(MySQLParser::DropUndoTablespaceContext *ctx) = 0;
  virtual void exitDropUndoTablespace(MySQLParser::DropUndoTablespaceContext *ctx) = 0;

  virtual void enterRenameTableStatement(MySQLParser::RenameTableStatementContext *ctx) = 0;
  virtual void exitRenameTableStatement(MySQLParser::RenameTableStatementContext *ctx) = 0;

  virtual void enterRenamePair(MySQLParser::RenamePairContext *ctx) = 0;
  virtual void exitRenamePair(MySQLParser::RenamePairContext *ctx) = 0;

  virtual void enterTruncateTableStatement(MySQLParser::TruncateTableStatementContext *ctx) = 0;
  virtual void exitTruncateTableStatement(MySQLParser::TruncateTableStatementContext *ctx) = 0;

  virtual void enterImportStatement(MySQLParser::ImportStatementContext *ctx) = 0;
  virtual void exitImportStatement(MySQLParser::ImportStatementContext *ctx) = 0;

  virtual void enterCallStatement(MySQLParser::CallStatementContext *ctx) = 0;
  virtual void exitCallStatement(MySQLParser::CallStatementContext *ctx) = 0;

  virtual void enterDeleteStatement(MySQLParser::DeleteStatementContext *ctx) = 0;
  virtual void exitDeleteStatement(MySQLParser::DeleteStatementContext *ctx) = 0;

  virtual void enterPartitionDelete(MySQLParser::PartitionDeleteContext *ctx) = 0;
  virtual void exitPartitionDelete(MySQLParser::PartitionDeleteContext *ctx) = 0;

  virtual void enterDeleteStatementOption(MySQLParser::DeleteStatementOptionContext *ctx) = 0;
  virtual void exitDeleteStatementOption(MySQLParser::DeleteStatementOptionContext *ctx) = 0;

  virtual void enterDoStatement(MySQLParser::DoStatementContext *ctx) = 0;
  virtual void exitDoStatement(MySQLParser::DoStatementContext *ctx) = 0;

  virtual void enterHandlerStatement(MySQLParser::HandlerStatementContext *ctx) = 0;
  virtual void exitHandlerStatement(MySQLParser::HandlerStatementContext *ctx) = 0;

  virtual void enterHandlerReadOrScan(MySQLParser::HandlerReadOrScanContext *ctx) = 0;
  virtual void exitHandlerReadOrScan(MySQLParser::HandlerReadOrScanContext *ctx) = 0;

  virtual void enterInsertStatement(MySQLParser::InsertStatementContext *ctx) = 0;
  virtual void exitInsertStatement(MySQLParser::InsertStatementContext *ctx) = 0;

  virtual void enterInsertLockOption(MySQLParser::InsertLockOptionContext *ctx) = 0;
  virtual void exitInsertLockOption(MySQLParser::InsertLockOptionContext *ctx) = 0;

  virtual void enterInsertFromConstructor(MySQLParser::InsertFromConstructorContext *ctx) = 0;
  virtual void exitInsertFromConstructor(MySQLParser::InsertFromConstructorContext *ctx) = 0;

  virtual void enterFields(MySQLParser::FieldsContext *ctx) = 0;
  virtual void exitFields(MySQLParser::FieldsContext *ctx) = 0;

  virtual void enterInsertValues(MySQLParser::InsertValuesContext *ctx) = 0;
  virtual void exitInsertValues(MySQLParser::InsertValuesContext *ctx) = 0;

  virtual void enterInsertQueryExpression(MySQLParser::InsertQueryExpressionContext *ctx) = 0;
  virtual void exitInsertQueryExpression(MySQLParser::InsertQueryExpressionContext *ctx) = 0;

  virtual void enterValueList(MySQLParser::ValueListContext *ctx) = 0;
  virtual void exitValueList(MySQLParser::ValueListContext *ctx) = 0;

  virtual void enterValues(MySQLParser::ValuesContext *ctx) = 0;
  virtual void exitValues(MySQLParser::ValuesContext *ctx) = 0;

  virtual void enterValuesReference(MySQLParser::ValuesReferenceContext *ctx) = 0;
  virtual void exitValuesReference(MySQLParser::ValuesReferenceContext *ctx) = 0;

  virtual void enterInsertUpdateList(MySQLParser::InsertUpdateListContext *ctx) = 0;
  virtual void exitInsertUpdateList(MySQLParser::InsertUpdateListContext *ctx) = 0;

  virtual void enterLoadStatement(MySQLParser::LoadStatementContext *ctx) = 0;
  virtual void exitLoadStatement(MySQLParser::LoadStatementContext *ctx) = 0;

  virtual void enterDataOrXml(MySQLParser::DataOrXmlContext *ctx) = 0;
  virtual void exitDataOrXml(MySQLParser::DataOrXmlContext *ctx) = 0;

  virtual void enterXmlRowsIdentifiedBy(MySQLParser::XmlRowsIdentifiedByContext *ctx) = 0;
  virtual void exitXmlRowsIdentifiedBy(MySQLParser::XmlRowsIdentifiedByContext *ctx) = 0;

  virtual void enterLoadDataFileTail(MySQLParser::LoadDataFileTailContext *ctx) = 0;
  virtual void exitLoadDataFileTail(MySQLParser::LoadDataFileTailContext *ctx) = 0;

  virtual void enterLoadDataFileTargetList(MySQLParser::LoadDataFileTargetListContext *ctx) = 0;
  virtual void exitLoadDataFileTargetList(MySQLParser::LoadDataFileTargetListContext *ctx) = 0;

  virtual void enterFieldOrVariableList(MySQLParser::FieldOrVariableListContext *ctx) = 0;
  virtual void exitFieldOrVariableList(MySQLParser::FieldOrVariableListContext *ctx) = 0;

  virtual void enterReplaceStatement(MySQLParser::ReplaceStatementContext *ctx) = 0;
  virtual void exitReplaceStatement(MySQLParser::ReplaceStatementContext *ctx) = 0;

  virtual void enterSelectStatement(MySQLParser::SelectStatementContext *ctx) = 0;
  virtual void exitSelectStatement(MySQLParser::SelectStatementContext *ctx) = 0;

  virtual void enterSelectStatementWithInto(MySQLParser::SelectStatementWithIntoContext *ctx) = 0;
  virtual void exitSelectStatementWithInto(MySQLParser::SelectStatementWithIntoContext *ctx) = 0;

  virtual void enterQueryExpression(MySQLParser::QueryExpressionContext *ctx) = 0;
  virtual void exitQueryExpression(MySQLParser::QueryExpressionContext *ctx) = 0;

  virtual void enterQueryExpressionBody(MySQLParser::QueryExpressionBodyContext *ctx) = 0;
  virtual void exitQueryExpressionBody(MySQLParser::QueryExpressionBodyContext *ctx) = 0;

  virtual void enterQueryExpressionParens(MySQLParser::QueryExpressionParensContext *ctx) = 0;
  virtual void exitQueryExpressionParens(MySQLParser::QueryExpressionParensContext *ctx) = 0;

  virtual void enterQuerySpecification(MySQLParser::QuerySpecificationContext *ctx) = 0;
  virtual void exitQuerySpecification(MySQLParser::QuerySpecificationContext *ctx) = 0;

  virtual void enterSubquery(MySQLParser::SubqueryContext *ctx) = 0;
  virtual void exitSubquery(MySQLParser::SubqueryContext *ctx) = 0;

  virtual void enterQuerySpecOption(MySQLParser::QuerySpecOptionContext *ctx) = 0;
  virtual void exitQuerySpecOption(MySQLParser::QuerySpecOptionContext *ctx) = 0;

  virtual void enterLimitClause(MySQLParser::LimitClauseContext *ctx) = 0;
  virtual void exitLimitClause(MySQLParser::LimitClauseContext *ctx) = 0;

  virtual void enterSimpleLimitClause(MySQLParser::SimpleLimitClauseContext *ctx) = 0;
  virtual void exitSimpleLimitClause(MySQLParser::SimpleLimitClauseContext *ctx) = 0;

  virtual void enterLimitOptions(MySQLParser::LimitOptionsContext *ctx) = 0;
  virtual void exitLimitOptions(MySQLParser::LimitOptionsContext *ctx) = 0;

  virtual void enterLimitOption(MySQLParser::LimitOptionContext *ctx) = 0;
  virtual void exitLimitOption(MySQLParser::LimitOptionContext *ctx) = 0;

  virtual void enterIntoClause(MySQLParser::IntoClauseContext *ctx) = 0;
  virtual void exitIntoClause(MySQLParser::IntoClauseContext *ctx) = 0;

  virtual void enterProcedureAnalyseClause(MySQLParser::ProcedureAnalyseClauseContext *ctx) = 0;
  virtual void exitProcedureAnalyseClause(MySQLParser::ProcedureAnalyseClauseContext *ctx) = 0;

  virtual void enterHavingClause(MySQLParser::HavingClauseContext *ctx) = 0;
  virtual void exitHavingClause(MySQLParser::HavingClauseContext *ctx) = 0;

  virtual void enterWindowClause(MySQLParser::WindowClauseContext *ctx) = 0;
  virtual void exitWindowClause(MySQLParser::WindowClauseContext *ctx) = 0;

  virtual void enterWindowDefinition(MySQLParser::WindowDefinitionContext *ctx) = 0;
  virtual void exitWindowDefinition(MySQLParser::WindowDefinitionContext *ctx) = 0;

  virtual void enterWindowSpec(MySQLParser::WindowSpecContext *ctx) = 0;
  virtual void exitWindowSpec(MySQLParser::WindowSpecContext *ctx) = 0;

  virtual void enterWindowSpecDetails(MySQLParser::WindowSpecDetailsContext *ctx) = 0;
  virtual void exitWindowSpecDetails(MySQLParser::WindowSpecDetailsContext *ctx) = 0;

  virtual void enterWindowFrameClause(MySQLParser::WindowFrameClauseContext *ctx) = 0;
  virtual void exitWindowFrameClause(MySQLParser::WindowFrameClauseContext *ctx) = 0;

  virtual void enterWindowFrameUnits(MySQLParser::WindowFrameUnitsContext *ctx) = 0;
  virtual void exitWindowFrameUnits(MySQLParser::WindowFrameUnitsContext *ctx) = 0;

  virtual void enterWindowFrameExtent(MySQLParser::WindowFrameExtentContext *ctx) = 0;
  virtual void exitWindowFrameExtent(MySQLParser::WindowFrameExtentContext *ctx) = 0;

  virtual void enterWindowFrameStart(MySQLParser::WindowFrameStartContext *ctx) = 0;
  virtual void exitWindowFrameStart(MySQLParser::WindowFrameStartContext *ctx) = 0;

  virtual void enterWindowFrameBetween(MySQLParser::WindowFrameBetweenContext *ctx) = 0;
  virtual void exitWindowFrameBetween(MySQLParser::WindowFrameBetweenContext *ctx) = 0;

  virtual void enterWindowFrameBound(MySQLParser::WindowFrameBoundContext *ctx) = 0;
  virtual void exitWindowFrameBound(MySQLParser::WindowFrameBoundContext *ctx) = 0;

  virtual void enterWindowFrameExclusion(MySQLParser::WindowFrameExclusionContext *ctx) = 0;
  virtual void exitWindowFrameExclusion(MySQLParser::WindowFrameExclusionContext *ctx) = 0;

  virtual void enterWithClause(MySQLParser::WithClauseContext *ctx) = 0;
  virtual void exitWithClause(MySQLParser::WithClauseContext *ctx) = 0;

  virtual void enterCommonTableExpression(MySQLParser::CommonTableExpressionContext *ctx) = 0;
  virtual void exitCommonTableExpression(MySQLParser::CommonTableExpressionContext *ctx) = 0;

  virtual void enterGroupByClause(MySQLParser::GroupByClauseContext *ctx) = 0;
  virtual void exitGroupByClause(MySQLParser::GroupByClauseContext *ctx) = 0;

  virtual void enterOlapOption(MySQLParser::OlapOptionContext *ctx) = 0;
  virtual void exitOlapOption(MySQLParser::OlapOptionContext *ctx) = 0;

  virtual void enterOrderClause(MySQLParser::OrderClauseContext *ctx) = 0;
  virtual void exitOrderClause(MySQLParser::OrderClauseContext *ctx) = 0;

  virtual void enterDirection(MySQLParser::DirectionContext *ctx) = 0;
  virtual void exitDirection(MySQLParser::DirectionContext *ctx) = 0;

  virtual void enterFromClause(MySQLParser::FromClauseContext *ctx) = 0;
  virtual void exitFromClause(MySQLParser::FromClauseContext *ctx) = 0;

  virtual void enterTableReferenceList(MySQLParser::TableReferenceListContext *ctx) = 0;
  virtual void exitTableReferenceList(MySQLParser::TableReferenceListContext *ctx) = 0;

  virtual void enterSelectOption(MySQLParser::SelectOptionContext *ctx) = 0;
  virtual void exitSelectOption(MySQLParser::SelectOptionContext *ctx) = 0;

  virtual void enterLockingClause(MySQLParser::LockingClauseContext *ctx) = 0;
  virtual void exitLockingClause(MySQLParser::LockingClauseContext *ctx) = 0;

  virtual void enterLockStrengh(MySQLParser::LockStrenghContext *ctx) = 0;
  virtual void exitLockStrengh(MySQLParser::LockStrenghContext *ctx) = 0;

  virtual void enterLockedRowAction(MySQLParser::LockedRowActionContext *ctx) = 0;
  virtual void exitLockedRowAction(MySQLParser::LockedRowActionContext *ctx) = 0;

  virtual void enterSelectItemList(MySQLParser::SelectItemListContext *ctx) = 0;
  virtual void exitSelectItemList(MySQLParser::SelectItemListContext *ctx) = 0;

  virtual void enterSelectItem(MySQLParser::SelectItemContext *ctx) = 0;
  virtual void exitSelectItem(MySQLParser::SelectItemContext *ctx) = 0;

  virtual void enterSelectAlias(MySQLParser::SelectAliasContext *ctx) = 0;
  virtual void exitSelectAlias(MySQLParser::SelectAliasContext *ctx) = 0;

  virtual void enterWhereClause(MySQLParser::WhereClauseContext *ctx) = 0;
  virtual void exitWhereClause(MySQLParser::WhereClauseContext *ctx) = 0;

  virtual void enterTableReference(MySQLParser::TableReferenceContext *ctx) = 0;
  virtual void exitTableReference(MySQLParser::TableReferenceContext *ctx) = 0;

  virtual void enterEscapedTableReference(MySQLParser::EscapedTableReferenceContext *ctx) = 0;
  virtual void exitEscapedTableReference(MySQLParser::EscapedTableReferenceContext *ctx) = 0;

  virtual void enterJoinedTable(MySQLParser::JoinedTableContext *ctx) = 0;
  virtual void exitJoinedTable(MySQLParser::JoinedTableContext *ctx) = 0;

  virtual void enterNaturalJoinType(MySQLParser::NaturalJoinTypeContext *ctx) = 0;
  virtual void exitNaturalJoinType(MySQLParser::NaturalJoinTypeContext *ctx) = 0;

  virtual void enterInnerJoinType(MySQLParser::InnerJoinTypeContext *ctx) = 0;
  virtual void exitInnerJoinType(MySQLParser::InnerJoinTypeContext *ctx) = 0;

  virtual void enterOuterJoinType(MySQLParser::OuterJoinTypeContext *ctx) = 0;
  virtual void exitOuterJoinType(MySQLParser::OuterJoinTypeContext *ctx) = 0;

  virtual void enterTableFactor(MySQLParser::TableFactorContext *ctx) = 0;
  virtual void exitTableFactor(MySQLParser::TableFactorContext *ctx) = 0;

  virtual void enterSingleTable(MySQLParser::SingleTableContext *ctx) = 0;
  virtual void exitSingleTable(MySQLParser::SingleTableContext *ctx) = 0;

  virtual void enterSingleTableParens(MySQLParser::SingleTableParensContext *ctx) = 0;
  virtual void exitSingleTableParens(MySQLParser::SingleTableParensContext *ctx) = 0;

  virtual void enterDerivedTable(MySQLParser::DerivedTableContext *ctx) = 0;
  virtual void exitDerivedTable(MySQLParser::DerivedTableContext *ctx) = 0;

  virtual void enterTableReferenceListParens(MySQLParser::TableReferenceListParensContext *ctx) = 0;
  virtual void exitTableReferenceListParens(MySQLParser::TableReferenceListParensContext *ctx) = 0;

  virtual void enterTableFunction(MySQLParser::TableFunctionContext *ctx) = 0;
  virtual void exitTableFunction(MySQLParser::TableFunctionContext *ctx) = 0;

  virtual void enterColumnsClause(MySQLParser::ColumnsClauseContext *ctx) = 0;
  virtual void exitColumnsClause(MySQLParser::ColumnsClauseContext *ctx) = 0;

  virtual void enterJtColumn(MySQLParser::JtColumnContext *ctx) = 0;
  virtual void exitJtColumn(MySQLParser::JtColumnContext *ctx) = 0;

  virtual void enterOnEmptyOrError(MySQLParser::OnEmptyOrErrorContext *ctx) = 0;
  virtual void exitOnEmptyOrError(MySQLParser::OnEmptyOrErrorContext *ctx) = 0;

  virtual void enterOnEmpty(MySQLParser::OnEmptyContext *ctx) = 0;
  virtual void exitOnEmpty(MySQLParser::OnEmptyContext *ctx) = 0;

  virtual void enterOnError(MySQLParser::OnErrorContext *ctx) = 0;
  virtual void exitOnError(MySQLParser::OnErrorContext *ctx) = 0;

  virtual void enterJtOnResponse(MySQLParser::JtOnResponseContext *ctx) = 0;
  virtual void exitJtOnResponse(MySQLParser::JtOnResponseContext *ctx) = 0;

  virtual void enterUnionOption(MySQLParser::UnionOptionContext *ctx) = 0;
  virtual void exitUnionOption(MySQLParser::UnionOptionContext *ctx) = 0;

  virtual void enterTableAlias(MySQLParser::TableAliasContext *ctx) = 0;
  virtual void exitTableAlias(MySQLParser::TableAliasContext *ctx) = 0;

  virtual void enterIndexHintList(MySQLParser::IndexHintListContext *ctx) = 0;
  virtual void exitIndexHintList(MySQLParser::IndexHintListContext *ctx) = 0;

  virtual void enterIndexHint(MySQLParser::IndexHintContext *ctx) = 0;
  virtual void exitIndexHint(MySQLParser::IndexHintContext *ctx) = 0;

  virtual void enterIndexHintType(MySQLParser::IndexHintTypeContext *ctx) = 0;
  virtual void exitIndexHintType(MySQLParser::IndexHintTypeContext *ctx) = 0;

  virtual void enterKeyOrIndex(MySQLParser::KeyOrIndexContext *ctx) = 0;
  virtual void exitKeyOrIndex(MySQLParser::KeyOrIndexContext *ctx) = 0;

  virtual void enterConstraintKeyType(MySQLParser::ConstraintKeyTypeContext *ctx) = 0;
  virtual void exitConstraintKeyType(MySQLParser::ConstraintKeyTypeContext *ctx) = 0;

  virtual void enterIndexHintClause(MySQLParser::IndexHintClauseContext *ctx) = 0;
  virtual void exitIndexHintClause(MySQLParser::IndexHintClauseContext *ctx) = 0;

  virtual void enterIndexList(MySQLParser::IndexListContext *ctx) = 0;
  virtual void exitIndexList(MySQLParser::IndexListContext *ctx) = 0;

  virtual void enterIndexListElement(MySQLParser::IndexListElementContext *ctx) = 0;
  virtual void exitIndexListElement(MySQLParser::IndexListElementContext *ctx) = 0;

  virtual void enterUpdateStatement(MySQLParser::UpdateStatementContext *ctx) = 0;
  virtual void exitUpdateStatement(MySQLParser::UpdateStatementContext *ctx) = 0;

  virtual void enterTransactionOrLockingStatement(MySQLParser::TransactionOrLockingStatementContext *ctx) = 0;
  virtual void exitTransactionOrLockingStatement(MySQLParser::TransactionOrLockingStatementContext *ctx) = 0;

  virtual void enterTransactionStatement(MySQLParser::TransactionStatementContext *ctx) = 0;
  virtual void exitTransactionStatement(MySQLParser::TransactionStatementContext *ctx) = 0;

  virtual void enterBeginWork(MySQLParser::BeginWorkContext *ctx) = 0;
  virtual void exitBeginWork(MySQLParser::BeginWorkContext *ctx) = 0;

  virtual void enterTransactionCharacteristic(MySQLParser::TransactionCharacteristicContext *ctx) = 0;
  virtual void exitTransactionCharacteristic(MySQLParser::TransactionCharacteristicContext *ctx) = 0;

  virtual void enterSavepointStatement(MySQLParser::SavepointStatementContext *ctx) = 0;
  virtual void exitSavepointStatement(MySQLParser::SavepointStatementContext *ctx) = 0;

  virtual void enterLockStatement(MySQLParser::LockStatementContext *ctx) = 0;
  virtual void exitLockStatement(MySQLParser::LockStatementContext *ctx) = 0;

  virtual void enterLockItem(MySQLParser::LockItemContext *ctx) = 0;
  virtual void exitLockItem(MySQLParser::LockItemContext *ctx) = 0;

  virtual void enterLockOption(MySQLParser::LockOptionContext *ctx) = 0;
  virtual void exitLockOption(MySQLParser::LockOptionContext *ctx) = 0;

  virtual void enterXaStatement(MySQLParser::XaStatementContext *ctx) = 0;
  virtual void exitXaStatement(MySQLParser::XaStatementContext *ctx) = 0;

  virtual void enterXaConvert(MySQLParser::XaConvertContext *ctx) = 0;
  virtual void exitXaConvert(MySQLParser::XaConvertContext *ctx) = 0;

  virtual void enterXid(MySQLParser::XidContext *ctx) = 0;
  virtual void exitXid(MySQLParser::XidContext *ctx) = 0;

  virtual void enterReplicationStatement(MySQLParser::ReplicationStatementContext *ctx) = 0;
  virtual void exitReplicationStatement(MySQLParser::ReplicationStatementContext *ctx) = 0;

  virtual void enterResetOption(MySQLParser::ResetOptionContext *ctx) = 0;
  virtual void exitResetOption(MySQLParser::ResetOptionContext *ctx) = 0;

  virtual void enterMasterResetOptions(MySQLParser::MasterResetOptionsContext *ctx) = 0;
  virtual void exitMasterResetOptions(MySQLParser::MasterResetOptionsContext *ctx) = 0;

  virtual void enterReplicationLoad(MySQLParser::ReplicationLoadContext *ctx) = 0;
  virtual void exitReplicationLoad(MySQLParser::ReplicationLoadContext *ctx) = 0;

  virtual void enterChangeMaster(MySQLParser::ChangeMasterContext *ctx) = 0;
  virtual void exitChangeMaster(MySQLParser::ChangeMasterContext *ctx) = 0;

  virtual void enterChangeMasterOptions(MySQLParser::ChangeMasterOptionsContext *ctx) = 0;
  virtual void exitChangeMasterOptions(MySQLParser::ChangeMasterOptionsContext *ctx) = 0;

  virtual void enterMasterOption(MySQLParser::MasterOptionContext *ctx) = 0;
  virtual void exitMasterOption(MySQLParser::MasterOptionContext *ctx) = 0;

  virtual void enterPrivilegeCheckDef(MySQLParser::PrivilegeCheckDefContext *ctx) = 0;
  virtual void exitPrivilegeCheckDef(MySQLParser::PrivilegeCheckDefContext *ctx) = 0;

  virtual void enterMasterTlsCiphersuitesDef(MySQLParser::MasterTlsCiphersuitesDefContext *ctx) = 0;
  virtual void exitMasterTlsCiphersuitesDef(MySQLParser::MasterTlsCiphersuitesDefContext *ctx) = 0;

  virtual void enterMasterFileDef(MySQLParser::MasterFileDefContext *ctx) = 0;
  virtual void exitMasterFileDef(MySQLParser::MasterFileDefContext *ctx) = 0;

  virtual void enterServerIdList(MySQLParser::ServerIdListContext *ctx) = 0;
  virtual void exitServerIdList(MySQLParser::ServerIdListContext *ctx) = 0;

  virtual void enterChangeReplication(MySQLParser::ChangeReplicationContext *ctx) = 0;
  virtual void exitChangeReplication(MySQLParser::ChangeReplicationContext *ctx) = 0;

  virtual void enterFilterDefinition(MySQLParser::FilterDefinitionContext *ctx) = 0;
  virtual void exitFilterDefinition(MySQLParser::FilterDefinitionContext *ctx) = 0;

  virtual void enterFilterDbList(MySQLParser::FilterDbListContext *ctx) = 0;
  virtual void exitFilterDbList(MySQLParser::FilterDbListContext *ctx) = 0;

  virtual void enterFilterTableList(MySQLParser::FilterTableListContext *ctx) = 0;
  virtual void exitFilterTableList(MySQLParser::FilterTableListContext *ctx) = 0;

  virtual void enterFilterStringList(MySQLParser::FilterStringListContext *ctx) = 0;
  virtual void exitFilterStringList(MySQLParser::FilterStringListContext *ctx) = 0;

  virtual void enterFilterWildDbTableString(MySQLParser::FilterWildDbTableStringContext *ctx) = 0;
  virtual void exitFilterWildDbTableString(MySQLParser::FilterWildDbTableStringContext *ctx) = 0;

  virtual void enterFilterDbPairList(MySQLParser::FilterDbPairListContext *ctx) = 0;
  virtual void exitFilterDbPairList(MySQLParser::FilterDbPairListContext *ctx) = 0;

  virtual void enterSlave(MySQLParser::SlaveContext *ctx) = 0;
  virtual void exitSlave(MySQLParser::SlaveContext *ctx) = 0;

  virtual void enterSlaveUntilOptions(MySQLParser::SlaveUntilOptionsContext *ctx) = 0;
  virtual void exitSlaveUntilOptions(MySQLParser::SlaveUntilOptionsContext *ctx) = 0;

  virtual void enterSlaveConnectionOptions(MySQLParser::SlaveConnectionOptionsContext *ctx) = 0;
  virtual void exitSlaveConnectionOptions(MySQLParser::SlaveConnectionOptionsContext *ctx) = 0;

  virtual void enterSlaveThreadOptions(MySQLParser::SlaveThreadOptionsContext *ctx) = 0;
  virtual void exitSlaveThreadOptions(MySQLParser::SlaveThreadOptionsContext *ctx) = 0;

  virtual void enterSlaveThreadOption(MySQLParser::SlaveThreadOptionContext *ctx) = 0;
  virtual void exitSlaveThreadOption(MySQLParser::SlaveThreadOptionContext *ctx) = 0;

  virtual void enterGroupReplication(MySQLParser::GroupReplicationContext *ctx) = 0;
  virtual void exitGroupReplication(MySQLParser::GroupReplicationContext *ctx) = 0;

  virtual void enterPreparedStatement(MySQLParser::PreparedStatementContext *ctx) = 0;
  virtual void exitPreparedStatement(MySQLParser::PreparedStatementContext *ctx) = 0;

  virtual void enterExecuteStatement(MySQLParser::ExecuteStatementContext *ctx) = 0;
  virtual void exitExecuteStatement(MySQLParser::ExecuteStatementContext *ctx) = 0;

  virtual void enterExecuteVarList(MySQLParser::ExecuteVarListContext *ctx) = 0;
  virtual void exitExecuteVarList(MySQLParser::ExecuteVarListContext *ctx) = 0;

  virtual void enterCloneStatement(MySQLParser::CloneStatementContext *ctx) = 0;
  virtual void exitCloneStatement(MySQLParser::CloneStatementContext *ctx) = 0;

  virtual void enterDataDirSSL(MySQLParser::DataDirSSLContext *ctx) = 0;
  virtual void exitDataDirSSL(MySQLParser::DataDirSSLContext *ctx) = 0;

  virtual void enterSsl(MySQLParser::SslContext *ctx) = 0;
  virtual void exitSsl(MySQLParser::SslContext *ctx) = 0;

  virtual void enterAccountManagementStatement(MySQLParser::AccountManagementStatementContext *ctx) = 0;
  virtual void exitAccountManagementStatement(MySQLParser::AccountManagementStatementContext *ctx) = 0;

  virtual void enterAlterUser(MySQLParser::AlterUserContext *ctx) = 0;
  virtual void exitAlterUser(MySQLParser::AlterUserContext *ctx) = 0;

  virtual void enterAlterUserTail(MySQLParser::AlterUserTailContext *ctx) = 0;
  virtual void exitAlterUserTail(MySQLParser::AlterUserTailContext *ctx) = 0;

  virtual void enterUserFunction(MySQLParser::UserFunctionContext *ctx) = 0;
  virtual void exitUserFunction(MySQLParser::UserFunctionContext *ctx) = 0;

  virtual void enterCreateUser(MySQLParser::CreateUserContext *ctx) = 0;
  virtual void exitCreateUser(MySQLParser::CreateUserContext *ctx) = 0;

  virtual void enterCreateUserTail(MySQLParser::CreateUserTailContext *ctx) = 0;
  virtual void exitCreateUserTail(MySQLParser::CreateUserTailContext *ctx) = 0;

  virtual void enterDefaultRoleClause(MySQLParser::DefaultRoleClauseContext *ctx) = 0;
  virtual void exitDefaultRoleClause(MySQLParser::DefaultRoleClauseContext *ctx) = 0;

  virtual void enterRequireClause(MySQLParser::RequireClauseContext *ctx) = 0;
  virtual void exitRequireClause(MySQLParser::RequireClauseContext *ctx) = 0;

  virtual void enterConnectOptions(MySQLParser::ConnectOptionsContext *ctx) = 0;
  virtual void exitConnectOptions(MySQLParser::ConnectOptionsContext *ctx) = 0;

  virtual void enterAccountLockPasswordExpireOptions(MySQLParser::AccountLockPasswordExpireOptionsContext *ctx) = 0;
  virtual void exitAccountLockPasswordExpireOptions(MySQLParser::AccountLockPasswordExpireOptionsContext *ctx) = 0;

  virtual void enterDropUser(MySQLParser::DropUserContext *ctx) = 0;
  virtual void exitDropUser(MySQLParser::DropUserContext *ctx) = 0;

  virtual void enterGrant(MySQLParser::GrantContext *ctx) = 0;
  virtual void exitGrant(MySQLParser::GrantContext *ctx) = 0;

  virtual void enterGrantTargetList(MySQLParser::GrantTargetListContext *ctx) = 0;
  virtual void exitGrantTargetList(MySQLParser::GrantTargetListContext *ctx) = 0;

  virtual void enterGrantOptions(MySQLParser::GrantOptionsContext *ctx) = 0;
  virtual void exitGrantOptions(MySQLParser::GrantOptionsContext *ctx) = 0;

  virtual void enterExceptRoleList(MySQLParser::ExceptRoleListContext *ctx) = 0;
  virtual void exitExceptRoleList(MySQLParser::ExceptRoleListContext *ctx) = 0;

  virtual void enterWithRoles(MySQLParser::WithRolesContext *ctx) = 0;
  virtual void exitWithRoles(MySQLParser::WithRolesContext *ctx) = 0;

  virtual void enterGrantAs(MySQLParser::GrantAsContext *ctx) = 0;
  virtual void exitGrantAs(MySQLParser::GrantAsContext *ctx) = 0;

  virtual void enterVersionedRequireClause(MySQLParser::VersionedRequireClauseContext *ctx) = 0;
  virtual void exitVersionedRequireClause(MySQLParser::VersionedRequireClauseContext *ctx) = 0;

  virtual void enterRenameUser(MySQLParser::RenameUserContext *ctx) = 0;
  virtual void exitRenameUser(MySQLParser::RenameUserContext *ctx) = 0;

  virtual void enterRevoke(MySQLParser::RevokeContext *ctx) = 0;
  virtual void exitRevoke(MySQLParser::RevokeContext *ctx) = 0;

  virtual void enterOnTypeTo(MySQLParser::OnTypeToContext *ctx) = 0;
  virtual void exitOnTypeTo(MySQLParser::OnTypeToContext *ctx) = 0;

  virtual void enterAclType(MySQLParser::AclTypeContext *ctx) = 0;
  virtual void exitAclType(MySQLParser::AclTypeContext *ctx) = 0;

  virtual void enterRoleOrPrivilegesList(MySQLParser::RoleOrPrivilegesListContext *ctx) = 0;
  virtual void exitRoleOrPrivilegesList(MySQLParser::RoleOrPrivilegesListContext *ctx) = 0;

  virtual void enterRoleOrPrivilege(MySQLParser::RoleOrPrivilegeContext *ctx) = 0;
  virtual void exitRoleOrPrivilege(MySQLParser::RoleOrPrivilegeContext *ctx) = 0;

  virtual void enterGrantIdentifier(MySQLParser::GrantIdentifierContext *ctx) = 0;
  virtual void exitGrantIdentifier(MySQLParser::GrantIdentifierContext *ctx) = 0;

  virtual void enterRequireList(MySQLParser::RequireListContext *ctx) = 0;
  virtual void exitRequireList(MySQLParser::RequireListContext *ctx) = 0;

  virtual void enterRequireListElement(MySQLParser::RequireListElementContext *ctx) = 0;
  virtual void exitRequireListElement(MySQLParser::RequireListElementContext *ctx) = 0;

  virtual void enterGrantOption(MySQLParser::GrantOptionContext *ctx) = 0;
  virtual void exitGrantOption(MySQLParser::GrantOptionContext *ctx) = 0;

  virtual void enterSetRole(MySQLParser::SetRoleContext *ctx) = 0;
  virtual void exitSetRole(MySQLParser::SetRoleContext *ctx) = 0;

  virtual void enterRoleList(MySQLParser::RoleListContext *ctx) = 0;
  virtual void exitRoleList(MySQLParser::RoleListContext *ctx) = 0;

  virtual void enterRole(MySQLParser::RoleContext *ctx) = 0;
  virtual void exitRole(MySQLParser::RoleContext *ctx) = 0;

  virtual void enterTableAdministrationStatement(MySQLParser::TableAdministrationStatementContext *ctx) = 0;
  virtual void exitTableAdministrationStatement(MySQLParser::TableAdministrationStatementContext *ctx) = 0;

  virtual void enterHistogram(MySQLParser::HistogramContext *ctx) = 0;
  virtual void exitHistogram(MySQLParser::HistogramContext *ctx) = 0;

  virtual void enterCheckOption(MySQLParser::CheckOptionContext *ctx) = 0;
  virtual void exitCheckOption(MySQLParser::CheckOptionContext *ctx) = 0;

  virtual void enterRepairType(MySQLParser::RepairTypeContext *ctx) = 0;
  virtual void exitRepairType(MySQLParser::RepairTypeContext *ctx) = 0;

  virtual void enterInstallUninstallStatment(MySQLParser::InstallUninstallStatmentContext *ctx) = 0;
  virtual void exitInstallUninstallStatment(MySQLParser::InstallUninstallStatmentContext *ctx) = 0;

  virtual void enterSetStatement(MySQLParser::SetStatementContext *ctx) = 0;
  virtual void exitSetStatement(MySQLParser::SetStatementContext *ctx) = 0;

  virtual void enterStartOptionValueList(MySQLParser::StartOptionValueListContext *ctx) = 0;
  virtual void exitStartOptionValueList(MySQLParser::StartOptionValueListContext *ctx) = 0;

  virtual void enterTransactionCharacteristics(MySQLParser::TransactionCharacteristicsContext *ctx) = 0;
  virtual void exitTransactionCharacteristics(MySQLParser::TransactionCharacteristicsContext *ctx) = 0;

  virtual void enterTransactionAccessMode(MySQLParser::TransactionAccessModeContext *ctx) = 0;
  virtual void exitTransactionAccessMode(MySQLParser::TransactionAccessModeContext *ctx) = 0;

  virtual void enterIsolationLevel(MySQLParser::IsolationLevelContext *ctx) = 0;
  virtual void exitIsolationLevel(MySQLParser::IsolationLevelContext *ctx) = 0;

  virtual void enterOptionValueListContinued(MySQLParser::OptionValueListContinuedContext *ctx) = 0;
  virtual void exitOptionValueListContinued(MySQLParser::OptionValueListContinuedContext *ctx) = 0;

  virtual void enterOptionValueNoOptionType(MySQLParser::OptionValueNoOptionTypeContext *ctx) = 0;
  virtual void exitOptionValueNoOptionType(MySQLParser::OptionValueNoOptionTypeContext *ctx) = 0;

  virtual void enterOptionValue(MySQLParser::OptionValueContext *ctx) = 0;
  virtual void exitOptionValue(MySQLParser::OptionValueContext *ctx) = 0;

  virtual void enterSetSystemVariable(MySQLParser::SetSystemVariableContext *ctx) = 0;
  virtual void exitSetSystemVariable(MySQLParser::SetSystemVariableContext *ctx) = 0;

  virtual void enterStartOptionValueListFollowingOptionType(MySQLParser::StartOptionValueListFollowingOptionTypeContext *ctx) = 0;
  virtual void exitStartOptionValueListFollowingOptionType(MySQLParser::StartOptionValueListFollowingOptionTypeContext *ctx) = 0;

  virtual void enterOptionValueFollowingOptionType(MySQLParser::OptionValueFollowingOptionTypeContext *ctx) = 0;
  virtual void exitOptionValueFollowingOptionType(MySQLParser::OptionValueFollowingOptionTypeContext *ctx) = 0;

  virtual void enterSetExprOrDefault(MySQLParser::SetExprOrDefaultContext *ctx) = 0;
  virtual void exitSetExprOrDefault(MySQLParser::SetExprOrDefaultContext *ctx) = 0;

  virtual void enterShowStatement(MySQLParser::ShowStatementContext *ctx) = 0;
  virtual void exitShowStatement(MySQLParser::ShowStatementContext *ctx) = 0;

  virtual void enterShowCommandType(MySQLParser::ShowCommandTypeContext *ctx) = 0;
  virtual void exitShowCommandType(MySQLParser::ShowCommandTypeContext *ctx) = 0;

  virtual void enterNonBlocking(MySQLParser::NonBlockingContext *ctx) = 0;
  virtual void exitNonBlocking(MySQLParser::NonBlockingContext *ctx) = 0;

  virtual void enterFromOrIn(MySQLParser::FromOrInContext *ctx) = 0;
  virtual void exitFromOrIn(MySQLParser::FromOrInContext *ctx) = 0;

  virtual void enterInDb(MySQLParser::InDbContext *ctx) = 0;
  virtual void exitInDb(MySQLParser::InDbContext *ctx) = 0;

  virtual void enterProfileType(MySQLParser::ProfileTypeContext *ctx) = 0;
  virtual void exitProfileType(MySQLParser::ProfileTypeContext *ctx) = 0;

  virtual void enterOtherAdministrativeStatement(MySQLParser::OtherAdministrativeStatementContext *ctx) = 0;
  virtual void exitOtherAdministrativeStatement(MySQLParser::OtherAdministrativeStatementContext *ctx) = 0;

  virtual void enterKeyCacheListOrParts(MySQLParser::KeyCacheListOrPartsContext *ctx) = 0;
  virtual void exitKeyCacheListOrParts(MySQLParser::KeyCacheListOrPartsContext *ctx) = 0;

  virtual void enterKeyCacheList(MySQLParser::KeyCacheListContext *ctx) = 0;
  virtual void exitKeyCacheList(MySQLParser::KeyCacheListContext *ctx) = 0;

  virtual void enterAssignToKeycache(MySQLParser::AssignToKeycacheContext *ctx) = 0;
  virtual void exitAssignToKeycache(MySQLParser::AssignToKeycacheContext *ctx) = 0;

  virtual void enterAssignToKeycachePartition(MySQLParser::AssignToKeycachePartitionContext *ctx) = 0;
  virtual void exitAssignToKeycachePartition(MySQLParser::AssignToKeycachePartitionContext *ctx) = 0;

  virtual void enterCacheKeyList(MySQLParser::CacheKeyListContext *ctx) = 0;
  virtual void exitCacheKeyList(MySQLParser::CacheKeyListContext *ctx) = 0;

  virtual void enterKeyUsageElement(MySQLParser::KeyUsageElementContext *ctx) = 0;
  virtual void exitKeyUsageElement(MySQLParser::KeyUsageElementContext *ctx) = 0;

  virtual void enterKeyUsageList(MySQLParser::KeyUsageListContext *ctx) = 0;
  virtual void exitKeyUsageList(MySQLParser::KeyUsageListContext *ctx) = 0;

  virtual void enterFlushOption(MySQLParser::FlushOptionContext *ctx) = 0;
  virtual void exitFlushOption(MySQLParser::FlushOptionContext *ctx) = 0;

  virtual void enterLogType(MySQLParser::LogTypeContext *ctx) = 0;
  virtual void exitLogType(MySQLParser::LogTypeContext *ctx) = 0;

  virtual void enterFlushTables(MySQLParser::FlushTablesContext *ctx) = 0;
  virtual void exitFlushTables(MySQLParser::FlushTablesContext *ctx) = 0;

  virtual void enterFlushTablesOptions(MySQLParser::FlushTablesOptionsContext *ctx) = 0;
  virtual void exitFlushTablesOptions(MySQLParser::FlushTablesOptionsContext *ctx) = 0;

  virtual void enterPreloadTail(MySQLParser::PreloadTailContext *ctx) = 0;
  virtual void exitPreloadTail(MySQLParser::PreloadTailContext *ctx) = 0;

  virtual void enterPreloadList(MySQLParser::PreloadListContext *ctx) = 0;
  virtual void exitPreloadList(MySQLParser::PreloadListContext *ctx) = 0;

  virtual void enterPreloadKeys(MySQLParser::PreloadKeysContext *ctx) = 0;
  virtual void exitPreloadKeys(MySQLParser::PreloadKeysContext *ctx) = 0;

  virtual void enterAdminPartition(MySQLParser::AdminPartitionContext *ctx) = 0;
  virtual void exitAdminPartition(MySQLParser::AdminPartitionContext *ctx) = 0;

  virtual void enterResourceGroupManagement(MySQLParser::ResourceGroupManagementContext *ctx) = 0;
  virtual void exitResourceGroupManagement(MySQLParser::ResourceGroupManagementContext *ctx) = 0;

  virtual void enterCreateResourceGroup(MySQLParser::CreateResourceGroupContext *ctx) = 0;
  virtual void exitCreateResourceGroup(MySQLParser::CreateResourceGroupContext *ctx) = 0;

  virtual void enterResourceGroupVcpuList(MySQLParser::ResourceGroupVcpuListContext *ctx) = 0;
  virtual void exitResourceGroupVcpuList(MySQLParser::ResourceGroupVcpuListContext *ctx) = 0;

  virtual void enterVcpuNumOrRange(MySQLParser::VcpuNumOrRangeContext *ctx) = 0;
  virtual void exitVcpuNumOrRange(MySQLParser::VcpuNumOrRangeContext *ctx) = 0;

  virtual void enterResourceGroupPriority(MySQLParser::ResourceGroupPriorityContext *ctx) = 0;
  virtual void exitResourceGroupPriority(MySQLParser::ResourceGroupPriorityContext *ctx) = 0;

  virtual void enterResourceGroupEnableDisable(MySQLParser::ResourceGroupEnableDisableContext *ctx) = 0;
  virtual void exitResourceGroupEnableDisable(MySQLParser::ResourceGroupEnableDisableContext *ctx) = 0;

  virtual void enterAlterResourceGroup(MySQLParser::AlterResourceGroupContext *ctx) = 0;
  virtual void exitAlterResourceGroup(MySQLParser::AlterResourceGroupContext *ctx) = 0;

  virtual void enterSetResourceGroup(MySQLParser::SetResourceGroupContext *ctx) = 0;
  virtual void exitSetResourceGroup(MySQLParser::SetResourceGroupContext *ctx) = 0;

  virtual void enterThreadIdList(MySQLParser::ThreadIdListContext *ctx) = 0;
  virtual void exitThreadIdList(MySQLParser::ThreadIdListContext *ctx) = 0;

  virtual void enterDropResourceGroup(MySQLParser::DropResourceGroupContext *ctx) = 0;
  virtual void exitDropResourceGroup(MySQLParser::DropResourceGroupContext *ctx) = 0;

  virtual void enterUtilityStatement(MySQLParser::UtilityStatementContext *ctx) = 0;
  virtual void exitUtilityStatement(MySQLParser::UtilityStatementContext *ctx) = 0;

  virtual void enterDescribeCommand(MySQLParser::DescribeCommandContext *ctx) = 0;
  virtual void exitDescribeCommand(MySQLParser::DescribeCommandContext *ctx) = 0;

  virtual void enterExplainCommand(MySQLParser::ExplainCommandContext *ctx) = 0;
  virtual void exitExplainCommand(MySQLParser::ExplainCommandContext *ctx) = 0;

  virtual void enterExplainableStatement(MySQLParser::ExplainableStatementContext *ctx) = 0;
  virtual void exitExplainableStatement(MySQLParser::ExplainableStatementContext *ctx) = 0;

  virtual void enterHelpCommand(MySQLParser::HelpCommandContext *ctx) = 0;
  virtual void exitHelpCommand(MySQLParser::HelpCommandContext *ctx) = 0;

  virtual void enterUseCommand(MySQLParser::UseCommandContext *ctx) = 0;
  virtual void exitUseCommand(MySQLParser::UseCommandContext *ctx) = 0;

  virtual void enterRestartServer(MySQLParser::RestartServerContext *ctx) = 0;
  virtual void exitRestartServer(MySQLParser::RestartServerContext *ctx) = 0;

  virtual void enterExprOr(MySQLParser::ExprOrContext *ctx) = 0;
  virtual void exitExprOr(MySQLParser::ExprOrContext *ctx) = 0;

  virtual void enterExprNot(MySQLParser::ExprNotContext *ctx) = 0;
  virtual void exitExprNot(MySQLParser::ExprNotContext *ctx) = 0;

  virtual void enterExprIs(MySQLParser::ExprIsContext *ctx) = 0;
  virtual void exitExprIs(MySQLParser::ExprIsContext *ctx) = 0;

  virtual void enterExprAnd(MySQLParser::ExprAndContext *ctx) = 0;
  virtual void exitExprAnd(MySQLParser::ExprAndContext *ctx) = 0;

  virtual void enterExprXor(MySQLParser::ExprXorContext *ctx) = 0;
  virtual void exitExprXor(MySQLParser::ExprXorContext *ctx) = 0;

  virtual void enterPrimaryExprPredicate(MySQLParser::PrimaryExprPredicateContext *ctx) = 0;
  virtual void exitPrimaryExprPredicate(MySQLParser::PrimaryExprPredicateContext *ctx) = 0;

  virtual void enterPrimaryExprCompare(MySQLParser::PrimaryExprCompareContext *ctx) = 0;
  virtual void exitPrimaryExprCompare(MySQLParser::PrimaryExprCompareContext *ctx) = 0;

  virtual void enterPrimaryExprAllAny(MySQLParser::PrimaryExprAllAnyContext *ctx) = 0;
  virtual void exitPrimaryExprAllAny(MySQLParser::PrimaryExprAllAnyContext *ctx) = 0;

  virtual void enterPrimaryExprIsNull(MySQLParser::PrimaryExprIsNullContext *ctx) = 0;
  virtual void exitPrimaryExprIsNull(MySQLParser::PrimaryExprIsNullContext *ctx) = 0;

  virtual void enterCompOp(MySQLParser::CompOpContext *ctx) = 0;
  virtual void exitCompOp(MySQLParser::CompOpContext *ctx) = 0;

  virtual void enterPredicate(MySQLParser::PredicateContext *ctx) = 0;
  virtual void exitPredicate(MySQLParser::PredicateContext *ctx) = 0;

  virtual void enterPredicateExprIn(MySQLParser::PredicateExprInContext *ctx) = 0;
  virtual void exitPredicateExprIn(MySQLParser::PredicateExprInContext *ctx) = 0;

  virtual void enterPredicateExprBetween(MySQLParser::PredicateExprBetweenContext *ctx) = 0;
  virtual void exitPredicateExprBetween(MySQLParser::PredicateExprBetweenContext *ctx) = 0;

  virtual void enterPredicateExprLike(MySQLParser::PredicateExprLikeContext *ctx) = 0;
  virtual void exitPredicateExprLike(MySQLParser::PredicateExprLikeContext *ctx) = 0;

  virtual void enterPredicateExprRegex(MySQLParser::PredicateExprRegexContext *ctx) = 0;
  virtual void exitPredicateExprRegex(MySQLParser::PredicateExprRegexContext *ctx) = 0;

  virtual void enterBitExpr(MySQLParser::BitExprContext *ctx) = 0;
  virtual void exitBitExpr(MySQLParser::BitExprContext *ctx) = 0;

  virtual void enterSimpleExprConvert(MySQLParser::SimpleExprConvertContext *ctx) = 0;
  virtual void exitSimpleExprConvert(MySQLParser::SimpleExprConvertContext *ctx) = 0;

  virtual void enterSimpleExprVariable(MySQLParser::SimpleExprVariableContext *ctx) = 0;
  virtual void exitSimpleExprVariable(MySQLParser::SimpleExprVariableContext *ctx) = 0;

  virtual void enterSimpleExprCast(MySQLParser::SimpleExprCastContext *ctx) = 0;
  virtual void exitSimpleExprCast(MySQLParser::SimpleExprCastContext *ctx) = 0;

  virtual void enterSimpleExprUnary(MySQLParser::SimpleExprUnaryContext *ctx) = 0;
  virtual void exitSimpleExprUnary(MySQLParser::SimpleExprUnaryContext *ctx) = 0;

  virtual void enterSimpleExprOdbc(MySQLParser::SimpleExprOdbcContext *ctx) = 0;
  virtual void exitSimpleExprOdbc(MySQLParser::SimpleExprOdbcContext *ctx) = 0;

  virtual void enterSimpleExprRuntimeFunction(MySQLParser::SimpleExprRuntimeFunctionContext *ctx) = 0;
  virtual void exitSimpleExprRuntimeFunction(MySQLParser::SimpleExprRuntimeFunctionContext *ctx) = 0;

  virtual void enterSimpleExprFunction(MySQLParser::SimpleExprFunctionContext *ctx) = 0;
  virtual void exitSimpleExprFunction(MySQLParser::SimpleExprFunctionContext *ctx) = 0;

  virtual void enterSimpleExprCollate(MySQLParser::SimpleExprCollateContext *ctx) = 0;
  virtual void exitSimpleExprCollate(MySQLParser::SimpleExprCollateContext *ctx) = 0;

  virtual void enterSimpleExprMatch(MySQLParser::SimpleExprMatchContext *ctx) = 0;
  virtual void exitSimpleExprMatch(MySQLParser::SimpleExprMatchContext *ctx) = 0;

  virtual void enterSimpleExprWindowingFunction(MySQLParser::SimpleExprWindowingFunctionContext *ctx) = 0;
  virtual void exitSimpleExprWindowingFunction(MySQLParser::SimpleExprWindowingFunctionContext *ctx) = 0;

  virtual void enterSimpleExprBinary(MySQLParser::SimpleExprBinaryContext *ctx) = 0;
  virtual void exitSimpleExprBinary(MySQLParser::SimpleExprBinaryContext *ctx) = 0;

  virtual void enterSimpleExprColumnRef(MySQLParser::SimpleExprColumnRefContext *ctx) = 0;
  virtual void exitSimpleExprColumnRef(MySQLParser::SimpleExprColumnRefContext *ctx) = 0;

  virtual void enterSimpleExprParamMarker(MySQLParser::SimpleExprParamMarkerContext *ctx) = 0;
  virtual void exitSimpleExprParamMarker(MySQLParser::SimpleExprParamMarkerContext *ctx) = 0;

  virtual void enterSimpleExprSum(MySQLParser::SimpleExprSumContext *ctx) = 0;
  virtual void exitSimpleExprSum(MySQLParser::SimpleExprSumContext *ctx) = 0;

  virtual void enterSimpleExprConvertUsing(MySQLParser::SimpleExprConvertUsingContext *ctx) = 0;
  virtual void exitSimpleExprConvertUsing(MySQLParser::SimpleExprConvertUsingContext *ctx) = 0;

  virtual void enterSimpleExprSubQuery(MySQLParser::SimpleExprSubQueryContext *ctx) = 0;
  virtual void exitSimpleExprSubQuery(MySQLParser::SimpleExprSubQueryContext *ctx) = 0;

  virtual void enterSimpleExprGroupingOperation(MySQLParser::SimpleExprGroupingOperationContext *ctx) = 0;
  virtual void exitSimpleExprGroupingOperation(MySQLParser::SimpleExprGroupingOperationContext *ctx) = 0;

  virtual void enterSimpleExprNot(MySQLParser::SimpleExprNotContext *ctx) = 0;
  virtual void exitSimpleExprNot(MySQLParser::SimpleExprNotContext *ctx) = 0;

  virtual void enterSimpleExprValues(MySQLParser::SimpleExprValuesContext *ctx) = 0;
  virtual void exitSimpleExprValues(MySQLParser::SimpleExprValuesContext *ctx) = 0;

  virtual void enterSimpleExprDefault(MySQLParser::SimpleExprDefaultContext *ctx) = 0;
  virtual void exitSimpleExprDefault(MySQLParser::SimpleExprDefaultContext *ctx) = 0;

  virtual void enterSimpleExprList(MySQLParser::SimpleExprListContext *ctx) = 0;
  virtual void exitSimpleExprList(MySQLParser::SimpleExprListContext *ctx) = 0;

  virtual void enterSimpleExprInterval(MySQLParser::SimpleExprIntervalContext *ctx) = 0;
  virtual void exitSimpleExprInterval(MySQLParser::SimpleExprIntervalContext *ctx) = 0;

  virtual void enterSimpleExprCase(MySQLParser::SimpleExprCaseContext *ctx) = 0;
  virtual void exitSimpleExprCase(MySQLParser::SimpleExprCaseContext *ctx) = 0;

  virtual void enterSimpleExprConcat(MySQLParser::SimpleExprConcatContext *ctx) = 0;
  virtual void exitSimpleExprConcat(MySQLParser::SimpleExprConcatContext *ctx) = 0;

  virtual void enterSimpleExprLiteral(MySQLParser::SimpleExprLiteralContext *ctx) = 0;
  virtual void exitSimpleExprLiteral(MySQLParser::SimpleExprLiteralContext *ctx) = 0;

  virtual void enterArrayCast(MySQLParser::ArrayCastContext *ctx) = 0;
  virtual void exitArrayCast(MySQLParser::ArrayCastContext *ctx) = 0;

  virtual void enterJsonOperator(MySQLParser::JsonOperatorContext *ctx) = 0;
  virtual void exitJsonOperator(MySQLParser::JsonOperatorContext *ctx) = 0;

  virtual void enterSumExpr(MySQLParser::SumExprContext *ctx) = 0;
  virtual void exitSumExpr(MySQLParser::SumExprContext *ctx) = 0;

  virtual void enterGroupingOperation(MySQLParser::GroupingOperationContext *ctx) = 0;
  virtual void exitGroupingOperation(MySQLParser::GroupingOperationContext *ctx) = 0;

  virtual void enterWindowFunctionCall(MySQLParser::WindowFunctionCallContext *ctx) = 0;
  virtual void exitWindowFunctionCall(MySQLParser::WindowFunctionCallContext *ctx) = 0;

  virtual void enterWindowingClause(MySQLParser::WindowingClauseContext *ctx) = 0;
  virtual void exitWindowingClause(MySQLParser::WindowingClauseContext *ctx) = 0;

  virtual void enterLeadLagInfo(MySQLParser::LeadLagInfoContext *ctx) = 0;
  virtual void exitLeadLagInfo(MySQLParser::LeadLagInfoContext *ctx) = 0;

  virtual void enterNullTreatment(MySQLParser::NullTreatmentContext *ctx) = 0;
  virtual void exitNullTreatment(MySQLParser::NullTreatmentContext *ctx) = 0;

  virtual void enterJsonFunction(MySQLParser::JsonFunctionContext *ctx) = 0;
  virtual void exitJsonFunction(MySQLParser::JsonFunctionContext *ctx) = 0;

  virtual void enterInSumExpr(MySQLParser::InSumExprContext *ctx) = 0;
  virtual void exitInSumExpr(MySQLParser::InSumExprContext *ctx) = 0;

  virtual void enterIdentListArg(MySQLParser::IdentListArgContext *ctx) = 0;
  virtual void exitIdentListArg(MySQLParser::IdentListArgContext *ctx) = 0;

  virtual void enterIdentList(MySQLParser::IdentListContext *ctx) = 0;
  virtual void exitIdentList(MySQLParser::IdentListContext *ctx) = 0;

  virtual void enterFulltextOptions(MySQLParser::FulltextOptionsContext *ctx) = 0;
  virtual void exitFulltextOptions(MySQLParser::FulltextOptionsContext *ctx) = 0;

  virtual void enterRuntimeFunctionCall(MySQLParser::RuntimeFunctionCallContext *ctx) = 0;
  virtual void exitRuntimeFunctionCall(MySQLParser::RuntimeFunctionCallContext *ctx) = 0;

  virtual void enterGeometryFunction(MySQLParser::GeometryFunctionContext *ctx) = 0;
  virtual void exitGeometryFunction(MySQLParser::GeometryFunctionContext *ctx) = 0;

  virtual void enterTimeFunctionParameters(MySQLParser::TimeFunctionParametersContext *ctx) = 0;
  virtual void exitTimeFunctionParameters(MySQLParser::TimeFunctionParametersContext *ctx) = 0;

  virtual void enterFractionalPrecision(MySQLParser::FractionalPrecisionContext *ctx) = 0;
  virtual void exitFractionalPrecision(MySQLParser::FractionalPrecisionContext *ctx) = 0;

  virtual void enterWeightStringLevels(MySQLParser::WeightStringLevelsContext *ctx) = 0;
  virtual void exitWeightStringLevels(MySQLParser::WeightStringLevelsContext *ctx) = 0;

  virtual void enterWeightStringLevelListItem(MySQLParser::WeightStringLevelListItemContext *ctx) = 0;
  virtual void exitWeightStringLevelListItem(MySQLParser::WeightStringLevelListItemContext *ctx) = 0;

  virtual void enterDateTimeTtype(MySQLParser::DateTimeTtypeContext *ctx) = 0;
  virtual void exitDateTimeTtype(MySQLParser::DateTimeTtypeContext *ctx) = 0;

  virtual void enterTrimFunction(MySQLParser::TrimFunctionContext *ctx) = 0;
  virtual void exitTrimFunction(MySQLParser::TrimFunctionContext *ctx) = 0;

  virtual void enterSubstringFunction(MySQLParser::SubstringFunctionContext *ctx) = 0;
  virtual void exitSubstringFunction(MySQLParser::SubstringFunctionContext *ctx) = 0;

  virtual void enterFunctionCall(MySQLParser::FunctionCallContext *ctx) = 0;
  virtual void exitFunctionCall(MySQLParser::FunctionCallContext *ctx) = 0;

  virtual void enterUdfExprList(MySQLParser::UdfExprListContext *ctx) = 0;
  virtual void exitUdfExprList(MySQLParser::UdfExprListContext *ctx) = 0;

  virtual void enterUdfExpr(MySQLParser::UdfExprContext *ctx) = 0;
  virtual void exitUdfExpr(MySQLParser::UdfExprContext *ctx) = 0;

  virtual void enterVariable(MySQLParser::VariableContext *ctx) = 0;
  virtual void exitVariable(MySQLParser::VariableContext *ctx) = 0;

  virtual void enterUserVariable(MySQLParser::UserVariableContext *ctx) = 0;
  virtual void exitUserVariable(MySQLParser::UserVariableContext *ctx) = 0;

  virtual void enterSystemVariable(MySQLParser::SystemVariableContext *ctx) = 0;
  virtual void exitSystemVariable(MySQLParser::SystemVariableContext *ctx) = 0;

  virtual void enterInternalVariableName(MySQLParser::InternalVariableNameContext *ctx) = 0;
  virtual void exitInternalVariableName(MySQLParser::InternalVariableNameContext *ctx) = 0;

  virtual void enterWhenExpression(MySQLParser::WhenExpressionContext *ctx) = 0;
  virtual void exitWhenExpression(MySQLParser::WhenExpressionContext *ctx) = 0;

  virtual void enterThenExpression(MySQLParser::ThenExpressionContext *ctx) = 0;
  virtual void exitThenExpression(MySQLParser::ThenExpressionContext *ctx) = 0;

  virtual void enterElseExpression(MySQLParser::ElseExpressionContext *ctx) = 0;
  virtual void exitElseExpression(MySQLParser::ElseExpressionContext *ctx) = 0;

  virtual void enterCastType(MySQLParser::CastTypeContext *ctx) = 0;
  virtual void exitCastType(MySQLParser::CastTypeContext *ctx) = 0;

  virtual void enterExprList(MySQLParser::ExprListContext *ctx) = 0;
  virtual void exitExprList(MySQLParser::ExprListContext *ctx) = 0;

  virtual void enterCharset(MySQLParser::CharsetContext *ctx) = 0;
  virtual void exitCharset(MySQLParser::CharsetContext *ctx) = 0;

  virtual void enterNotRule(MySQLParser::NotRuleContext *ctx) = 0;
  virtual void exitNotRule(MySQLParser::NotRuleContext *ctx) = 0;

  virtual void enterNot2Rule(MySQLParser::Not2RuleContext *ctx) = 0;
  virtual void exitNot2Rule(MySQLParser::Not2RuleContext *ctx) = 0;

  virtual void enterInterval(MySQLParser::IntervalContext *ctx) = 0;
  virtual void exitInterval(MySQLParser::IntervalContext *ctx) = 0;

  virtual void enterIntervalTimeStamp(MySQLParser::IntervalTimeStampContext *ctx) = 0;
  virtual void exitIntervalTimeStamp(MySQLParser::IntervalTimeStampContext *ctx) = 0;

  virtual void enterExprListWithParentheses(MySQLParser::ExprListWithParenthesesContext *ctx) = 0;
  virtual void exitExprListWithParentheses(MySQLParser::ExprListWithParenthesesContext *ctx) = 0;

  virtual void enterExprWithParentheses(MySQLParser::ExprWithParenthesesContext *ctx) = 0;
  virtual void exitExprWithParentheses(MySQLParser::ExprWithParenthesesContext *ctx) = 0;

  virtual void enterSimpleExprWithParentheses(MySQLParser::SimpleExprWithParenthesesContext *ctx) = 0;
  virtual void exitSimpleExprWithParentheses(MySQLParser::SimpleExprWithParenthesesContext *ctx) = 0;

  virtual void enterOrderList(MySQLParser::OrderListContext *ctx) = 0;
  virtual void exitOrderList(MySQLParser::OrderListContext *ctx) = 0;

  virtual void enterOrderExpression(MySQLParser::OrderExpressionContext *ctx) = 0;
  virtual void exitOrderExpression(MySQLParser::OrderExpressionContext *ctx) = 0;

  virtual void enterGroupList(MySQLParser::GroupListContext *ctx) = 0;
  virtual void exitGroupList(MySQLParser::GroupListContext *ctx) = 0;

  virtual void enterGroupingExpression(MySQLParser::GroupingExpressionContext *ctx) = 0;
  virtual void exitGroupingExpression(MySQLParser::GroupingExpressionContext *ctx) = 0;

  virtual void enterChannel(MySQLParser::ChannelContext *ctx) = 0;
  virtual void exitChannel(MySQLParser::ChannelContext *ctx) = 0;

  virtual void enterCompoundStatement(MySQLParser::CompoundStatementContext *ctx) = 0;
  virtual void exitCompoundStatement(MySQLParser::CompoundStatementContext *ctx) = 0;

  virtual void enterReturnStatement(MySQLParser::ReturnStatementContext *ctx) = 0;
  virtual void exitReturnStatement(MySQLParser::ReturnStatementContext *ctx) = 0;

  virtual void enterIfStatement(MySQLParser::IfStatementContext *ctx) = 0;
  virtual void exitIfStatement(MySQLParser::IfStatementContext *ctx) = 0;

  virtual void enterIfBody(MySQLParser::IfBodyContext *ctx) = 0;
  virtual void exitIfBody(MySQLParser::IfBodyContext *ctx) = 0;

  virtual void enterThenStatement(MySQLParser::ThenStatementContext *ctx) = 0;
  virtual void exitThenStatement(MySQLParser::ThenStatementContext *ctx) = 0;

  virtual void enterCompoundStatementList(MySQLParser::CompoundStatementListContext *ctx) = 0;
  virtual void exitCompoundStatementList(MySQLParser::CompoundStatementListContext *ctx) = 0;

  virtual void enterCaseStatement(MySQLParser::CaseStatementContext *ctx) = 0;
  virtual void exitCaseStatement(MySQLParser::CaseStatementContext *ctx) = 0;

  virtual void enterElseStatement(MySQLParser::ElseStatementContext *ctx) = 0;
  virtual void exitElseStatement(MySQLParser::ElseStatementContext *ctx) = 0;

  virtual void enterLabeledBlock(MySQLParser::LabeledBlockContext *ctx) = 0;
  virtual void exitLabeledBlock(MySQLParser::LabeledBlockContext *ctx) = 0;

  virtual void enterUnlabeledBlock(MySQLParser::UnlabeledBlockContext *ctx) = 0;
  virtual void exitUnlabeledBlock(MySQLParser::UnlabeledBlockContext *ctx) = 0;

  virtual void enterLabel(MySQLParser::LabelContext *ctx) = 0;
  virtual void exitLabel(MySQLParser::LabelContext *ctx) = 0;

  virtual void enterBeginEndBlock(MySQLParser::BeginEndBlockContext *ctx) = 0;
  virtual void exitBeginEndBlock(MySQLParser::BeginEndBlockContext *ctx) = 0;

  virtual void enterLabeledControl(MySQLParser::LabeledControlContext *ctx) = 0;
  virtual void exitLabeledControl(MySQLParser::LabeledControlContext *ctx) = 0;

  virtual void enterUnlabeledControl(MySQLParser::UnlabeledControlContext *ctx) = 0;
  virtual void exitUnlabeledControl(MySQLParser::UnlabeledControlContext *ctx) = 0;

  virtual void enterLoopBlock(MySQLParser::LoopBlockContext *ctx) = 0;
  virtual void exitLoopBlock(MySQLParser::LoopBlockContext *ctx) = 0;

  virtual void enterWhileDoBlock(MySQLParser::WhileDoBlockContext *ctx) = 0;
  virtual void exitWhileDoBlock(MySQLParser::WhileDoBlockContext *ctx) = 0;

  virtual void enterRepeatUntilBlock(MySQLParser::RepeatUntilBlockContext *ctx) = 0;
  virtual void exitRepeatUntilBlock(MySQLParser::RepeatUntilBlockContext *ctx) = 0;

  virtual void enterSpDeclarations(MySQLParser::SpDeclarationsContext *ctx) = 0;
  virtual void exitSpDeclarations(MySQLParser::SpDeclarationsContext *ctx) = 0;

  virtual void enterSpDeclaration(MySQLParser::SpDeclarationContext *ctx) = 0;
  virtual void exitSpDeclaration(MySQLParser::SpDeclarationContext *ctx) = 0;

  virtual void enterVariableDeclaration(MySQLParser::VariableDeclarationContext *ctx) = 0;
  virtual void exitVariableDeclaration(MySQLParser::VariableDeclarationContext *ctx) = 0;

  virtual void enterConditionDeclaration(MySQLParser::ConditionDeclarationContext *ctx) = 0;
  virtual void exitConditionDeclaration(MySQLParser::ConditionDeclarationContext *ctx) = 0;

  virtual void enterSpCondition(MySQLParser::SpConditionContext *ctx) = 0;
  virtual void exitSpCondition(MySQLParser::SpConditionContext *ctx) = 0;

  virtual void enterSqlstate(MySQLParser::SqlstateContext *ctx) = 0;
  virtual void exitSqlstate(MySQLParser::SqlstateContext *ctx) = 0;

  virtual void enterHandlerDeclaration(MySQLParser::HandlerDeclarationContext *ctx) = 0;
  virtual void exitHandlerDeclaration(MySQLParser::HandlerDeclarationContext *ctx) = 0;

  virtual void enterHandlerCondition(MySQLParser::HandlerConditionContext *ctx) = 0;
  virtual void exitHandlerCondition(MySQLParser::HandlerConditionContext *ctx) = 0;

  virtual void enterCursorDeclaration(MySQLParser::CursorDeclarationContext *ctx) = 0;
  virtual void exitCursorDeclaration(MySQLParser::CursorDeclarationContext *ctx) = 0;

  virtual void enterIterateStatement(MySQLParser::IterateStatementContext *ctx) = 0;
  virtual void exitIterateStatement(MySQLParser::IterateStatementContext *ctx) = 0;

  virtual void enterLeaveStatement(MySQLParser::LeaveStatementContext *ctx) = 0;
  virtual void exitLeaveStatement(MySQLParser::LeaveStatementContext *ctx) = 0;

  virtual void enterGetDiagnostics(MySQLParser::GetDiagnosticsContext *ctx) = 0;
  virtual void exitGetDiagnostics(MySQLParser::GetDiagnosticsContext *ctx) = 0;

  virtual void enterSignalAllowedExpr(MySQLParser::SignalAllowedExprContext *ctx) = 0;
  virtual void exitSignalAllowedExpr(MySQLParser::SignalAllowedExprContext *ctx) = 0;

  virtual void enterStatementInformationItem(MySQLParser::StatementInformationItemContext *ctx) = 0;
  virtual void exitStatementInformationItem(MySQLParser::StatementInformationItemContext *ctx) = 0;

  virtual void enterConditionInformationItem(MySQLParser::ConditionInformationItemContext *ctx) = 0;
  virtual void exitConditionInformationItem(MySQLParser::ConditionInformationItemContext *ctx) = 0;

  virtual void enterSignalInformationItemName(MySQLParser::SignalInformationItemNameContext *ctx) = 0;
  virtual void exitSignalInformationItemName(MySQLParser::SignalInformationItemNameContext *ctx) = 0;

  virtual void enterSignalStatement(MySQLParser::SignalStatementContext *ctx) = 0;
  virtual void exitSignalStatement(MySQLParser::SignalStatementContext *ctx) = 0;

  virtual void enterResignalStatement(MySQLParser::ResignalStatementContext *ctx) = 0;
  virtual void exitResignalStatement(MySQLParser::ResignalStatementContext *ctx) = 0;

  virtual void enterSignalInformationItem(MySQLParser::SignalInformationItemContext *ctx) = 0;
  virtual void exitSignalInformationItem(MySQLParser::SignalInformationItemContext *ctx) = 0;

  virtual void enterCursorOpen(MySQLParser::CursorOpenContext *ctx) = 0;
  virtual void exitCursorOpen(MySQLParser::CursorOpenContext *ctx) = 0;

  virtual void enterCursorClose(MySQLParser::CursorCloseContext *ctx) = 0;
  virtual void exitCursorClose(MySQLParser::CursorCloseContext *ctx) = 0;

  virtual void enterCursorFetch(MySQLParser::CursorFetchContext *ctx) = 0;
  virtual void exitCursorFetch(MySQLParser::CursorFetchContext *ctx) = 0;

  virtual void enterSchedule(MySQLParser::ScheduleContext *ctx) = 0;
  virtual void exitSchedule(MySQLParser::ScheduleContext *ctx) = 0;

  virtual void enterColumnDefinition(MySQLParser::ColumnDefinitionContext *ctx) = 0;
  virtual void exitColumnDefinition(MySQLParser::ColumnDefinitionContext *ctx) = 0;

  virtual void enterCheckOrReferences(MySQLParser::CheckOrReferencesContext *ctx) = 0;
  virtual void exitCheckOrReferences(MySQLParser::CheckOrReferencesContext *ctx) = 0;

  virtual void enterCheckConstraint(MySQLParser::CheckConstraintContext *ctx) = 0;
  virtual void exitCheckConstraint(MySQLParser::CheckConstraintContext *ctx) = 0;

  virtual void enterConstraintEnforcement(MySQLParser::ConstraintEnforcementContext *ctx) = 0;
  virtual void exitConstraintEnforcement(MySQLParser::ConstraintEnforcementContext *ctx) = 0;

  virtual void enterTableConstraintDef(MySQLParser::TableConstraintDefContext *ctx) = 0;
  virtual void exitTableConstraintDef(MySQLParser::TableConstraintDefContext *ctx) = 0;

  virtual void enterConstraintName(MySQLParser::ConstraintNameContext *ctx) = 0;
  virtual void exitConstraintName(MySQLParser::ConstraintNameContext *ctx) = 0;

  virtual void enterFieldDefinition(MySQLParser::FieldDefinitionContext *ctx) = 0;
  virtual void exitFieldDefinition(MySQLParser::FieldDefinitionContext *ctx) = 0;

  virtual void enterColumnAttribute(MySQLParser::ColumnAttributeContext *ctx) = 0;
  virtual void exitColumnAttribute(MySQLParser::ColumnAttributeContext *ctx) = 0;

  virtual void enterColumnFormat(MySQLParser::ColumnFormatContext *ctx) = 0;
  virtual void exitColumnFormat(MySQLParser::ColumnFormatContext *ctx) = 0;

  virtual void enterStorageMedia(MySQLParser::StorageMediaContext *ctx) = 0;
  virtual void exitStorageMedia(MySQLParser::StorageMediaContext *ctx) = 0;

  virtual void enterGcolAttribute(MySQLParser::GcolAttributeContext *ctx) = 0;
  virtual void exitGcolAttribute(MySQLParser::GcolAttributeContext *ctx) = 0;

  virtual void enterReferences(MySQLParser::ReferencesContext *ctx) = 0;
  virtual void exitReferences(MySQLParser::ReferencesContext *ctx) = 0;

  virtual void enterDeleteOption(MySQLParser::DeleteOptionContext *ctx) = 0;
  virtual void exitDeleteOption(MySQLParser::DeleteOptionContext *ctx) = 0;

  virtual void enterKeyList(MySQLParser::KeyListContext *ctx) = 0;
  virtual void exitKeyList(MySQLParser::KeyListContext *ctx) = 0;

  virtual void enterKeyPart(MySQLParser::KeyPartContext *ctx) = 0;
  virtual void exitKeyPart(MySQLParser::KeyPartContext *ctx) = 0;

  virtual void enterKeyListWithExpression(MySQLParser::KeyListWithExpressionContext *ctx) = 0;
  virtual void exitKeyListWithExpression(MySQLParser::KeyListWithExpressionContext *ctx) = 0;

  virtual void enterKeyPartOrExpression(MySQLParser::KeyPartOrExpressionContext *ctx) = 0;
  virtual void exitKeyPartOrExpression(MySQLParser::KeyPartOrExpressionContext *ctx) = 0;

  virtual void enterKeyListVariants(MySQLParser::KeyListVariantsContext *ctx) = 0;
  virtual void exitKeyListVariants(MySQLParser::KeyListVariantsContext *ctx) = 0;

  virtual void enterIndexType(MySQLParser::IndexTypeContext *ctx) = 0;
  virtual void exitIndexType(MySQLParser::IndexTypeContext *ctx) = 0;

  virtual void enterIndexOption(MySQLParser::IndexOptionContext *ctx) = 0;
  virtual void exitIndexOption(MySQLParser::IndexOptionContext *ctx) = 0;

  virtual void enterCommonIndexOption(MySQLParser::CommonIndexOptionContext *ctx) = 0;
  virtual void exitCommonIndexOption(MySQLParser::CommonIndexOptionContext *ctx) = 0;

  virtual void enterVisibility(MySQLParser::VisibilityContext *ctx) = 0;
  virtual void exitVisibility(MySQLParser::VisibilityContext *ctx) = 0;

  virtual void enterIndexTypeClause(MySQLParser::IndexTypeClauseContext *ctx) = 0;
  virtual void exitIndexTypeClause(MySQLParser::IndexTypeClauseContext *ctx) = 0;

  virtual void enterFulltextIndexOption(MySQLParser::FulltextIndexOptionContext *ctx) = 0;
  virtual void exitFulltextIndexOption(MySQLParser::FulltextIndexOptionContext *ctx) = 0;

  virtual void enterSpatialIndexOption(MySQLParser::SpatialIndexOptionContext *ctx) = 0;
  virtual void exitSpatialIndexOption(MySQLParser::SpatialIndexOptionContext *ctx) = 0;

  virtual void enterDataTypeDefinition(MySQLParser::DataTypeDefinitionContext *ctx) = 0;
  virtual void exitDataTypeDefinition(MySQLParser::DataTypeDefinitionContext *ctx) = 0;

  virtual void enterDataType(MySQLParser::DataTypeContext *ctx) = 0;
  virtual void exitDataType(MySQLParser::DataTypeContext *ctx) = 0;

  virtual void enterNchar(MySQLParser::NcharContext *ctx) = 0;
  virtual void exitNchar(MySQLParser::NcharContext *ctx) = 0;

  virtual void enterRealType(MySQLParser::RealTypeContext *ctx) = 0;
  virtual void exitRealType(MySQLParser::RealTypeContext *ctx) = 0;

  virtual void enterFieldLength(MySQLParser::FieldLengthContext *ctx) = 0;
  virtual void exitFieldLength(MySQLParser::FieldLengthContext *ctx) = 0;

  virtual void enterFieldOptions(MySQLParser::FieldOptionsContext *ctx) = 0;
  virtual void exitFieldOptions(MySQLParser::FieldOptionsContext *ctx) = 0;

  virtual void enterCharsetWithOptBinary(MySQLParser::CharsetWithOptBinaryContext *ctx) = 0;
  virtual void exitCharsetWithOptBinary(MySQLParser::CharsetWithOptBinaryContext *ctx) = 0;

  virtual void enterAscii(MySQLParser::AsciiContext *ctx) = 0;
  virtual void exitAscii(MySQLParser::AsciiContext *ctx) = 0;

  virtual void enterUnicode(MySQLParser::UnicodeContext *ctx) = 0;
  virtual void exitUnicode(MySQLParser::UnicodeContext *ctx) = 0;

  virtual void enterWsNumCodepoints(MySQLParser::WsNumCodepointsContext *ctx) = 0;
  virtual void exitWsNumCodepoints(MySQLParser::WsNumCodepointsContext *ctx) = 0;

  virtual void enterTypeDatetimePrecision(MySQLParser::TypeDatetimePrecisionContext *ctx) = 0;
  virtual void exitTypeDatetimePrecision(MySQLParser::TypeDatetimePrecisionContext *ctx) = 0;

  virtual void enterCharsetName(MySQLParser::CharsetNameContext *ctx) = 0;
  virtual void exitCharsetName(MySQLParser::CharsetNameContext *ctx) = 0;

  virtual void enterCollationName(MySQLParser::CollationNameContext *ctx) = 0;
  virtual void exitCollationName(MySQLParser::CollationNameContext *ctx) = 0;

  virtual void enterCreateTableOptions(MySQLParser::CreateTableOptionsContext *ctx) = 0;
  virtual void exitCreateTableOptions(MySQLParser::CreateTableOptionsContext *ctx) = 0;

  virtual void enterCreateTableOptionsSpaceSeparated(MySQLParser::CreateTableOptionsSpaceSeparatedContext *ctx) = 0;
  virtual void exitCreateTableOptionsSpaceSeparated(MySQLParser::CreateTableOptionsSpaceSeparatedContext *ctx) = 0;

  virtual void enterCreateTableOption(MySQLParser::CreateTableOptionContext *ctx) = 0;
  virtual void exitCreateTableOption(MySQLParser::CreateTableOptionContext *ctx) = 0;

  virtual void enterTernaryOption(MySQLParser::TernaryOptionContext *ctx) = 0;
  virtual void exitTernaryOption(MySQLParser::TernaryOptionContext *ctx) = 0;

  virtual void enterDefaultCollation(MySQLParser::DefaultCollationContext *ctx) = 0;
  virtual void exitDefaultCollation(MySQLParser::DefaultCollationContext *ctx) = 0;

  virtual void enterDefaultEncryption(MySQLParser::DefaultEncryptionContext *ctx) = 0;
  virtual void exitDefaultEncryption(MySQLParser::DefaultEncryptionContext *ctx) = 0;

  virtual void enterDefaultCharset(MySQLParser::DefaultCharsetContext *ctx) = 0;
  virtual void exitDefaultCharset(MySQLParser::DefaultCharsetContext *ctx) = 0;

  virtual void enterPartitionClause(MySQLParser::PartitionClauseContext *ctx) = 0;
  virtual void exitPartitionClause(MySQLParser::PartitionClauseContext *ctx) = 0;

  virtual void enterPartitionDefKey(MySQLParser::PartitionDefKeyContext *ctx) = 0;
  virtual void exitPartitionDefKey(MySQLParser::PartitionDefKeyContext *ctx) = 0;

  virtual void enterPartitionDefHash(MySQLParser::PartitionDefHashContext *ctx) = 0;
  virtual void exitPartitionDefHash(MySQLParser::PartitionDefHashContext *ctx) = 0;

  virtual void enterPartitionDefRangeList(MySQLParser::PartitionDefRangeListContext *ctx) = 0;
  virtual void exitPartitionDefRangeList(MySQLParser::PartitionDefRangeListContext *ctx) = 0;

  virtual void enterSubPartitions(MySQLParser::SubPartitionsContext *ctx) = 0;
  virtual void exitSubPartitions(MySQLParser::SubPartitionsContext *ctx) = 0;

  virtual void enterPartitionKeyAlgorithm(MySQLParser::PartitionKeyAlgorithmContext *ctx) = 0;
  virtual void exitPartitionKeyAlgorithm(MySQLParser::PartitionKeyAlgorithmContext *ctx) = 0;

  virtual void enterPartitionDefinitions(MySQLParser::PartitionDefinitionsContext *ctx) = 0;
  virtual void exitPartitionDefinitions(MySQLParser::PartitionDefinitionsContext *ctx) = 0;

  virtual void enterPartitionDefinition(MySQLParser::PartitionDefinitionContext *ctx) = 0;
  virtual void exitPartitionDefinition(MySQLParser::PartitionDefinitionContext *ctx) = 0;

  virtual void enterPartitionValuesIn(MySQLParser::PartitionValuesInContext *ctx) = 0;
  virtual void exitPartitionValuesIn(MySQLParser::PartitionValuesInContext *ctx) = 0;

  virtual void enterPartitionOption(MySQLParser::PartitionOptionContext *ctx) = 0;
  virtual void exitPartitionOption(MySQLParser::PartitionOptionContext *ctx) = 0;

  virtual void enterSubpartitionDefinition(MySQLParser::SubpartitionDefinitionContext *ctx) = 0;
  virtual void exitSubpartitionDefinition(MySQLParser::SubpartitionDefinitionContext *ctx) = 0;

  virtual void enterPartitionValueItemListParen(MySQLParser::PartitionValueItemListParenContext *ctx) = 0;
  virtual void exitPartitionValueItemListParen(MySQLParser::PartitionValueItemListParenContext *ctx) = 0;

  virtual void enterPartitionValueItem(MySQLParser::PartitionValueItemContext *ctx) = 0;
  virtual void exitPartitionValueItem(MySQLParser::PartitionValueItemContext *ctx) = 0;

  virtual void enterDefinerClause(MySQLParser::DefinerClauseContext *ctx) = 0;
  virtual void exitDefinerClause(MySQLParser::DefinerClauseContext *ctx) = 0;

  virtual void enterIfExists(MySQLParser::IfExistsContext *ctx) = 0;
  virtual void exitIfExists(MySQLParser::IfExistsContext *ctx) = 0;

  virtual void enterIfNotExists(MySQLParser::IfNotExistsContext *ctx) = 0;
  virtual void exitIfNotExists(MySQLParser::IfNotExistsContext *ctx) = 0;

  virtual void enterProcedureParameter(MySQLParser::ProcedureParameterContext *ctx) = 0;
  virtual void exitProcedureParameter(MySQLParser::ProcedureParameterContext *ctx) = 0;

  virtual void enterFunctionParameter(MySQLParser::FunctionParameterContext *ctx) = 0;
  virtual void exitFunctionParameter(MySQLParser::FunctionParameterContext *ctx) = 0;

  virtual void enterCollate(MySQLParser::CollateContext *ctx) = 0;
  virtual void exitCollate(MySQLParser::CollateContext *ctx) = 0;

  virtual void enterTypeWithOptCollate(MySQLParser::TypeWithOptCollateContext *ctx) = 0;
  virtual void exitTypeWithOptCollate(MySQLParser::TypeWithOptCollateContext *ctx) = 0;

  virtual void enterSchemaIdentifierPair(MySQLParser::SchemaIdentifierPairContext *ctx) = 0;
  virtual void exitSchemaIdentifierPair(MySQLParser::SchemaIdentifierPairContext *ctx) = 0;

  virtual void enterViewRefList(MySQLParser::ViewRefListContext *ctx) = 0;
  virtual void exitViewRefList(MySQLParser::ViewRefListContext *ctx) = 0;

  virtual void enterUpdateList(MySQLParser::UpdateListContext *ctx) = 0;
  virtual void exitUpdateList(MySQLParser::UpdateListContext *ctx) = 0;

  virtual void enterUpdateElement(MySQLParser::UpdateElementContext *ctx) = 0;
  virtual void exitUpdateElement(MySQLParser::UpdateElementContext *ctx) = 0;

  virtual void enterCharsetClause(MySQLParser::CharsetClauseContext *ctx) = 0;
  virtual void exitCharsetClause(MySQLParser::CharsetClauseContext *ctx) = 0;

  virtual void enterFieldsClause(MySQLParser::FieldsClauseContext *ctx) = 0;
  virtual void exitFieldsClause(MySQLParser::FieldsClauseContext *ctx) = 0;

  virtual void enterFieldTerm(MySQLParser::FieldTermContext *ctx) = 0;
  virtual void exitFieldTerm(MySQLParser::FieldTermContext *ctx) = 0;

  virtual void enterLinesClause(MySQLParser::LinesClauseContext *ctx) = 0;
  virtual void exitLinesClause(MySQLParser::LinesClauseContext *ctx) = 0;

  virtual void enterLineTerm(MySQLParser::LineTermContext *ctx) = 0;
  virtual void exitLineTerm(MySQLParser::LineTermContext *ctx) = 0;

  virtual void enterUserList(MySQLParser::UserListContext *ctx) = 0;
  virtual void exitUserList(MySQLParser::UserListContext *ctx) = 0;

  virtual void enterCreateUserList(MySQLParser::CreateUserListContext *ctx) = 0;
  virtual void exitCreateUserList(MySQLParser::CreateUserListContext *ctx) = 0;

  virtual void enterAlterUserList(MySQLParser::AlterUserListContext *ctx) = 0;
  virtual void exitAlterUserList(MySQLParser::AlterUserListContext *ctx) = 0;

  virtual void enterCreateUserEntry(MySQLParser::CreateUserEntryContext *ctx) = 0;
  virtual void exitCreateUserEntry(MySQLParser::CreateUserEntryContext *ctx) = 0;

  virtual void enterAlterUserEntry(MySQLParser::AlterUserEntryContext *ctx) = 0;
  virtual void exitAlterUserEntry(MySQLParser::AlterUserEntryContext *ctx) = 0;

  virtual void enterRetainCurrentPassword(MySQLParser::RetainCurrentPasswordContext *ctx) = 0;
  virtual void exitRetainCurrentPassword(MySQLParser::RetainCurrentPasswordContext *ctx) = 0;

  virtual void enterDiscardOldPassword(MySQLParser::DiscardOldPasswordContext *ctx) = 0;
  virtual void exitDiscardOldPassword(MySQLParser::DiscardOldPasswordContext *ctx) = 0;

  virtual void enterReplacePassword(MySQLParser::ReplacePasswordContext *ctx) = 0;
  virtual void exitReplacePassword(MySQLParser::ReplacePasswordContext *ctx) = 0;

  virtual void enterUserIdentifierOrText(MySQLParser::UserIdentifierOrTextContext *ctx) = 0;
  virtual void exitUserIdentifierOrText(MySQLParser::UserIdentifierOrTextContext *ctx) = 0;

  virtual void enterUser(MySQLParser::UserContext *ctx) = 0;
  virtual void exitUser(MySQLParser::UserContext *ctx) = 0;

  virtual void enterLikeClause(MySQLParser::LikeClauseContext *ctx) = 0;
  virtual void exitLikeClause(MySQLParser::LikeClauseContext *ctx) = 0;

  virtual void enterLikeOrWhere(MySQLParser::LikeOrWhereContext *ctx) = 0;
  virtual void exitLikeOrWhere(MySQLParser::LikeOrWhereContext *ctx) = 0;

  virtual void enterOnlineOption(MySQLParser::OnlineOptionContext *ctx) = 0;
  virtual void exitOnlineOption(MySQLParser::OnlineOptionContext *ctx) = 0;

  virtual void enterNoWriteToBinLog(MySQLParser::NoWriteToBinLogContext *ctx) = 0;
  virtual void exitNoWriteToBinLog(MySQLParser::NoWriteToBinLogContext *ctx) = 0;

  virtual void enterUsePartition(MySQLParser::UsePartitionContext *ctx) = 0;
  virtual void exitUsePartition(MySQLParser::UsePartitionContext *ctx) = 0;

  virtual void enterFieldIdentifier(MySQLParser::FieldIdentifierContext *ctx) = 0;
  virtual void exitFieldIdentifier(MySQLParser::FieldIdentifierContext *ctx) = 0;

  virtual void enterColumnName(MySQLParser::ColumnNameContext *ctx) = 0;
  virtual void exitColumnName(MySQLParser::ColumnNameContext *ctx) = 0;

  virtual void enterColumnInternalRef(MySQLParser::ColumnInternalRefContext *ctx) = 0;
  virtual void exitColumnInternalRef(MySQLParser::ColumnInternalRefContext *ctx) = 0;

  virtual void enterColumnInternalRefList(MySQLParser::ColumnInternalRefListContext *ctx) = 0;
  virtual void exitColumnInternalRefList(MySQLParser::ColumnInternalRefListContext *ctx) = 0;

  virtual void enterColumnRef(MySQLParser::ColumnRefContext *ctx) = 0;
  virtual void exitColumnRef(MySQLParser::ColumnRefContext *ctx) = 0;

  virtual void enterInsertIdentifier(MySQLParser::InsertIdentifierContext *ctx) = 0;
  virtual void exitInsertIdentifier(MySQLParser::InsertIdentifierContext *ctx) = 0;

  virtual void enterIndexName(MySQLParser::IndexNameContext *ctx) = 0;
  virtual void exitIndexName(MySQLParser::IndexNameContext *ctx) = 0;

  virtual void enterIndexRef(MySQLParser::IndexRefContext *ctx) = 0;
  virtual void exitIndexRef(MySQLParser::IndexRefContext *ctx) = 0;

  virtual void enterTableWild(MySQLParser::TableWildContext *ctx) = 0;
  virtual void exitTableWild(MySQLParser::TableWildContext *ctx) = 0;

  virtual void enterSchemaName(MySQLParser::SchemaNameContext *ctx) = 0;
  virtual void exitSchemaName(MySQLParser::SchemaNameContext *ctx) = 0;

  virtual void enterSchemaRef(MySQLParser::SchemaRefContext *ctx) = 0;
  virtual void exitSchemaRef(MySQLParser::SchemaRefContext *ctx) = 0;

  virtual void enterProcedureName(MySQLParser::ProcedureNameContext *ctx) = 0;
  virtual void exitProcedureName(MySQLParser::ProcedureNameContext *ctx) = 0;

  virtual void enterProcedureRef(MySQLParser::ProcedureRefContext *ctx) = 0;
  virtual void exitProcedureRef(MySQLParser::ProcedureRefContext *ctx) = 0;

  virtual void enterFunctionName(MySQLParser::FunctionNameContext *ctx) = 0;
  virtual void exitFunctionName(MySQLParser::FunctionNameContext *ctx) = 0;

  virtual void enterFunctionRef(MySQLParser::FunctionRefContext *ctx) = 0;
  virtual void exitFunctionRef(MySQLParser::FunctionRefContext *ctx) = 0;

  virtual void enterTriggerName(MySQLParser::TriggerNameContext *ctx) = 0;
  virtual void exitTriggerName(MySQLParser::TriggerNameContext *ctx) = 0;

  virtual void enterTriggerRef(MySQLParser::TriggerRefContext *ctx) = 0;
  virtual void exitTriggerRef(MySQLParser::TriggerRefContext *ctx) = 0;

  virtual void enterViewName(MySQLParser::ViewNameContext *ctx) = 0;
  virtual void exitViewName(MySQLParser::ViewNameContext *ctx) = 0;

  virtual void enterViewRef(MySQLParser::ViewRefContext *ctx) = 0;
  virtual void exitViewRef(MySQLParser::ViewRefContext *ctx) = 0;

  virtual void enterTablespaceName(MySQLParser::TablespaceNameContext *ctx) = 0;
  virtual void exitTablespaceName(MySQLParser::TablespaceNameContext *ctx) = 0;

  virtual void enterTablespaceRef(MySQLParser::TablespaceRefContext *ctx) = 0;
  virtual void exitTablespaceRef(MySQLParser::TablespaceRefContext *ctx) = 0;

  virtual void enterLogfileGroupName(MySQLParser::LogfileGroupNameContext *ctx) = 0;
  virtual void exitLogfileGroupName(MySQLParser::LogfileGroupNameContext *ctx) = 0;

  virtual void enterLogfileGroupRef(MySQLParser::LogfileGroupRefContext *ctx) = 0;
  virtual void exitLogfileGroupRef(MySQLParser::LogfileGroupRefContext *ctx) = 0;

  virtual void enterEventName(MySQLParser::EventNameContext *ctx) = 0;
  virtual void exitEventName(MySQLParser::EventNameContext *ctx) = 0;

  virtual void enterEventRef(MySQLParser::EventRefContext *ctx) = 0;
  virtual void exitEventRef(MySQLParser::EventRefContext *ctx) = 0;

  virtual void enterUdfName(MySQLParser::UdfNameContext *ctx) = 0;
  virtual void exitUdfName(MySQLParser::UdfNameContext *ctx) = 0;

  virtual void enterServerName(MySQLParser::ServerNameContext *ctx) = 0;
  virtual void exitServerName(MySQLParser::ServerNameContext *ctx) = 0;

  virtual void enterServerRef(MySQLParser::ServerRefContext *ctx) = 0;
  virtual void exitServerRef(MySQLParser::ServerRefContext *ctx) = 0;

  virtual void enterEngineRef(MySQLParser::EngineRefContext *ctx) = 0;
  virtual void exitEngineRef(MySQLParser::EngineRefContext *ctx) = 0;

  virtual void enterTableName(MySQLParser::TableNameContext *ctx) = 0;
  virtual void exitTableName(MySQLParser::TableNameContext *ctx) = 0;

  virtual void enterFilterTableRef(MySQLParser::FilterTableRefContext *ctx) = 0;
  virtual void exitFilterTableRef(MySQLParser::FilterTableRefContext *ctx) = 0;

  virtual void enterTableRefWithWildcard(MySQLParser::TableRefWithWildcardContext *ctx) = 0;
  virtual void exitTableRefWithWildcard(MySQLParser::TableRefWithWildcardContext *ctx) = 0;

  virtual void enterTableRef(MySQLParser::TableRefContext *ctx) = 0;
  virtual void exitTableRef(MySQLParser::TableRefContext *ctx) = 0;

  virtual void enterTableRefList(MySQLParser::TableRefListContext *ctx) = 0;
  virtual void exitTableRefList(MySQLParser::TableRefListContext *ctx) = 0;

  virtual void enterTableAliasRefList(MySQLParser::TableAliasRefListContext *ctx) = 0;
  virtual void exitTableAliasRefList(MySQLParser::TableAliasRefListContext *ctx) = 0;

  virtual void enterParameterName(MySQLParser::ParameterNameContext *ctx) = 0;
  virtual void exitParameterName(MySQLParser::ParameterNameContext *ctx) = 0;

  virtual void enterLabelIdentifier(MySQLParser::LabelIdentifierContext *ctx) = 0;
  virtual void exitLabelIdentifier(MySQLParser::LabelIdentifierContext *ctx) = 0;

  virtual void enterLabelRef(MySQLParser::LabelRefContext *ctx) = 0;
  virtual void exitLabelRef(MySQLParser::LabelRefContext *ctx) = 0;

  virtual void enterRoleIdentifier(MySQLParser::RoleIdentifierContext *ctx) = 0;
  virtual void exitRoleIdentifier(MySQLParser::RoleIdentifierContext *ctx) = 0;

  virtual void enterRoleRef(MySQLParser::RoleRefContext *ctx) = 0;
  virtual void exitRoleRef(MySQLParser::RoleRefContext *ctx) = 0;

  virtual void enterPluginRef(MySQLParser::PluginRefContext *ctx) = 0;
  virtual void exitPluginRef(MySQLParser::PluginRefContext *ctx) = 0;

  virtual void enterComponentRef(MySQLParser::ComponentRefContext *ctx) = 0;
  virtual void exitComponentRef(MySQLParser::ComponentRefContext *ctx) = 0;

  virtual void enterResourceGroupRef(MySQLParser::ResourceGroupRefContext *ctx) = 0;
  virtual void exitResourceGroupRef(MySQLParser::ResourceGroupRefContext *ctx) = 0;

  virtual void enterWindowName(MySQLParser::WindowNameContext *ctx) = 0;
  virtual void exitWindowName(MySQLParser::WindowNameContext *ctx) = 0;

  virtual void enterPureIdentifier(MySQLParser::PureIdentifierContext *ctx) = 0;
  virtual void exitPureIdentifier(MySQLParser::PureIdentifierContext *ctx) = 0;

  virtual void enterIdentifier(MySQLParser::IdentifierContext *ctx) = 0;
  virtual void exitIdentifier(MySQLParser::IdentifierContext *ctx) = 0;

  virtual void enterIdentifierList(MySQLParser::IdentifierListContext *ctx) = 0;
  virtual void exitIdentifierList(MySQLParser::IdentifierListContext *ctx) = 0;

  virtual void enterIdentifierListWithParentheses(MySQLParser::IdentifierListWithParenthesesContext *ctx) = 0;
  virtual void exitIdentifierListWithParentheses(MySQLParser::IdentifierListWithParenthesesContext *ctx) = 0;

  virtual void enterQualifiedIdentifier(MySQLParser::QualifiedIdentifierContext *ctx) = 0;
  virtual void exitQualifiedIdentifier(MySQLParser::QualifiedIdentifierContext *ctx) = 0;

  virtual void enterSimpleIdentifier(MySQLParser::SimpleIdentifierContext *ctx) = 0;
  virtual void exitSimpleIdentifier(MySQLParser::SimpleIdentifierContext *ctx) = 0;

  virtual void enterDotIdentifier(MySQLParser::DotIdentifierContext *ctx) = 0;
  virtual void exitDotIdentifier(MySQLParser::DotIdentifierContext *ctx) = 0;

  virtual void enterUlong_number(MySQLParser::Ulong_numberContext *ctx) = 0;
  virtual void exitUlong_number(MySQLParser::Ulong_numberContext *ctx) = 0;

  virtual void enterReal_ulong_number(MySQLParser::Real_ulong_numberContext *ctx) = 0;
  virtual void exitReal_ulong_number(MySQLParser::Real_ulong_numberContext *ctx) = 0;

  virtual void enterUlonglong_number(MySQLParser::Ulonglong_numberContext *ctx) = 0;
  virtual void exitUlonglong_number(MySQLParser::Ulonglong_numberContext *ctx) = 0;

  virtual void enterReal_ulonglong_number(MySQLParser::Real_ulonglong_numberContext *ctx) = 0;
  virtual void exitReal_ulonglong_number(MySQLParser::Real_ulonglong_numberContext *ctx) = 0;

  virtual void enterLiteral(MySQLParser::LiteralContext *ctx) = 0;
  virtual void exitLiteral(MySQLParser::LiteralContext *ctx) = 0;

  virtual void enterSignedLiteral(MySQLParser::SignedLiteralContext *ctx) = 0;
  virtual void exitSignedLiteral(MySQLParser::SignedLiteralContext *ctx) = 0;

  virtual void enterStringList(MySQLParser::StringListContext *ctx) = 0;
  virtual void exitStringList(MySQLParser::StringListContext *ctx) = 0;

  virtual void enterTextStringLiteral(MySQLParser::TextStringLiteralContext *ctx) = 0;
  virtual void exitTextStringLiteral(MySQLParser::TextStringLiteralContext *ctx) = 0;

  virtual void enterTextString(MySQLParser::TextStringContext *ctx) = 0;
  virtual void exitTextString(MySQLParser::TextStringContext *ctx) = 0;

  virtual void enterTextStringHash(MySQLParser::TextStringHashContext *ctx) = 0;
  virtual void exitTextStringHash(MySQLParser::TextStringHashContext *ctx) = 0;

  virtual void enterTextLiteral(MySQLParser::TextLiteralContext *ctx) = 0;
  virtual void exitTextLiteral(MySQLParser::TextLiteralContext *ctx) = 0;

  virtual void enterTextStringNoLinebreak(MySQLParser::TextStringNoLinebreakContext *ctx) = 0;
  virtual void exitTextStringNoLinebreak(MySQLParser::TextStringNoLinebreakContext *ctx) = 0;

  virtual void enterTextStringLiteralList(MySQLParser::TextStringLiteralListContext *ctx) = 0;
  virtual void exitTextStringLiteralList(MySQLParser::TextStringLiteralListContext *ctx) = 0;

  virtual void enterNumLiteral(MySQLParser::NumLiteralContext *ctx) = 0;
  virtual void exitNumLiteral(MySQLParser::NumLiteralContext *ctx) = 0;

  virtual void enterBoolLiteral(MySQLParser::BoolLiteralContext *ctx) = 0;
  virtual void exitBoolLiteral(MySQLParser::BoolLiteralContext *ctx) = 0;

  virtual void enterNullLiteral(MySQLParser::NullLiteralContext *ctx) = 0;
  virtual void exitNullLiteral(MySQLParser::NullLiteralContext *ctx) = 0;

  virtual void enterTemporalLiteral(MySQLParser::TemporalLiteralContext *ctx) = 0;
  virtual void exitTemporalLiteral(MySQLParser::TemporalLiteralContext *ctx) = 0;

  virtual void enterFloatOptions(MySQLParser::FloatOptionsContext *ctx) = 0;
  virtual void exitFloatOptions(MySQLParser::FloatOptionsContext *ctx) = 0;

  virtual void enterStandardFloatOptions(MySQLParser::StandardFloatOptionsContext *ctx) = 0;
  virtual void exitStandardFloatOptions(MySQLParser::StandardFloatOptionsContext *ctx) = 0;

  virtual void enterPrecision(MySQLParser::PrecisionContext *ctx) = 0;
  virtual void exitPrecision(MySQLParser::PrecisionContext *ctx) = 0;

  virtual void enterTextOrIdentifier(MySQLParser::TextOrIdentifierContext *ctx) = 0;
  virtual void exitTextOrIdentifier(MySQLParser::TextOrIdentifierContext *ctx) = 0;

  virtual void enterLValueIdentifier(MySQLParser::LValueIdentifierContext *ctx) = 0;
  virtual void exitLValueIdentifier(MySQLParser::LValueIdentifierContext *ctx) = 0;

  virtual void enterRoleIdentifierOrText(MySQLParser::RoleIdentifierOrTextContext *ctx) = 0;
  virtual void exitRoleIdentifierOrText(MySQLParser::RoleIdentifierOrTextContext *ctx) = 0;

  virtual void enterSizeNumber(MySQLParser::SizeNumberContext *ctx) = 0;
  virtual void exitSizeNumber(MySQLParser::SizeNumberContext *ctx) = 0;

  virtual void enterParentheses(MySQLParser::ParenthesesContext *ctx) = 0;
  virtual void exitParentheses(MySQLParser::ParenthesesContext *ctx) = 0;

  virtual void enterEqual(MySQLParser::EqualContext *ctx) = 0;
  virtual void exitEqual(MySQLParser::EqualContext *ctx) = 0;

  virtual void enterOptionType(MySQLParser::OptionTypeContext *ctx) = 0;
  virtual void exitOptionType(MySQLParser::OptionTypeContext *ctx) = 0;

  virtual void enterVarIdentType(MySQLParser::VarIdentTypeContext *ctx) = 0;
  virtual void exitVarIdentType(MySQLParser::VarIdentTypeContext *ctx) = 0;

  virtual void enterSetVarIdentType(MySQLParser::SetVarIdentTypeContext *ctx) = 0;
  virtual void exitSetVarIdentType(MySQLParser::SetVarIdentTypeContext *ctx) = 0;

  virtual void enterIdentifierKeyword(MySQLParser::IdentifierKeywordContext *ctx) = 0;
  virtual void exitIdentifierKeyword(MySQLParser::IdentifierKeywordContext *ctx) = 0;

  virtual void enterIdentifierKeywordsAmbiguous1RolesAndLabels(MySQLParser::IdentifierKeywordsAmbiguous1RolesAndLabelsContext *ctx) = 0;
  virtual void exitIdentifierKeywordsAmbiguous1RolesAndLabels(MySQLParser::IdentifierKeywordsAmbiguous1RolesAndLabelsContext *ctx) = 0;

  virtual void enterIdentifierKeywordsAmbiguous2Labels(MySQLParser::IdentifierKeywordsAmbiguous2LabelsContext *ctx) = 0;
  virtual void exitIdentifierKeywordsAmbiguous2Labels(MySQLParser::IdentifierKeywordsAmbiguous2LabelsContext *ctx) = 0;

  virtual void enterLabelKeyword(MySQLParser::LabelKeywordContext *ctx) = 0;
  virtual void exitLabelKeyword(MySQLParser::LabelKeywordContext *ctx) = 0;

  virtual void enterIdentifierKeywordsAmbiguous3Roles(MySQLParser::IdentifierKeywordsAmbiguous3RolesContext *ctx) = 0;
  virtual void exitIdentifierKeywordsAmbiguous3Roles(MySQLParser::IdentifierKeywordsAmbiguous3RolesContext *ctx) = 0;

  virtual void enterIdentifierKeywordsUnambiguous(MySQLParser::IdentifierKeywordsUnambiguousContext *ctx) = 0;
  virtual void exitIdentifierKeywordsUnambiguous(MySQLParser::IdentifierKeywordsUnambiguousContext *ctx) = 0;

  virtual void enterRoleKeyword(MySQLParser::RoleKeywordContext *ctx) = 0;
  virtual void exitRoleKeyword(MySQLParser::RoleKeywordContext *ctx) = 0;

  virtual void enterLValueKeyword(MySQLParser::LValueKeywordContext *ctx) = 0;
  virtual void exitLValueKeyword(MySQLParser::LValueKeywordContext *ctx) = 0;

  virtual void enterIdentifierKeywordsAmbiguous4SystemVariables(MySQLParser::IdentifierKeywordsAmbiguous4SystemVariablesContext *ctx) = 0;
  virtual void exitIdentifierKeywordsAmbiguous4SystemVariables(MySQLParser::IdentifierKeywordsAmbiguous4SystemVariablesContext *ctx) = 0;

  virtual void enterRoleOrIdentifierKeyword(MySQLParser::RoleOrIdentifierKeywordContext *ctx) = 0;
  virtual void exitRoleOrIdentifierKeyword(MySQLParser::RoleOrIdentifierKeywordContext *ctx) = 0;

  virtual void enterRoleOrLabelKeyword(MySQLParser::RoleOrLabelKeywordContext *ctx) = 0;
  virtual void exitRoleOrLabelKeyword(MySQLParser::RoleOrLabelKeywordContext *ctx) = 0;


};

