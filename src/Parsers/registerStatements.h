#pragma once


namespace DB
{
class StatementFactory;

void registerStatementAlter(StatementFactory & factory);
void registerStatementAlterNamedCollection(StatementFactory & factory);
void registerStatementCheck(StatementFactory & factory);
void registerStatementColumnsTransformers(StatementFactory & factory);
void registerStatementCreate(StatementFactory & factory);
void registerStatementCreateFunction(StatementFactory & factory);
void registerStatementCreateHandler(StatementFactory & factory);
void registerStatementCreateToken(StatementFactory & factory);
void registerStatementDelete(StatementFactory & factory);
void registerStatementDescribeTable(StatementFactory & factory);
void registerStatementDrop(StatementFactory & factory);
void registerStatementExists(StatementFactory & factory);
void registerStatementExplain(StatementFactory & factory);
void registerStatementHypotheticalIndex(StatementFactory & factory);
void registerStatementHypotheticalProjection(StatementFactory & factory);
void registerStatementIn(StatementFactory & factory);
void registerStatementInsert(StatementFactory & factory);
void registerStatementKillQuery(StatementFactory & factory);
void registerStatementOnCluster(StatementFactory & factory);
void registerStatementOptimize(StatementFactory & factory);
void registerStatementParallelWith(StatementFactory & factory);
void registerStatementPipeOperators(StatementFactory & factory);
void registerStatementQueryWithOutput(StatementFactory & factory);
void registerStatementRename(StatementFactory & factory);
void registerStatementSelect(StatementFactory & factory);
void registerStatementSet(StatementFactory & factory);
void registerStatementShow(StatementFactory & factory);
void registerStatementSystem(StatementFactory & factory);
void registerStatementTablesInSelect(StatementFactory & factory);
void registerStatementUndrop(StatementFactory & factory);
void registerStatementUnion(StatementFactory & factory);
void registerStatementUpdate(StatementFactory & factory);
void registerStatementUse(StatementFactory & factory);
void registerStatementWith(StatementFactory & factory);
void registerStatementCheckGrant(StatementFactory & factory);
void registerStatementExecuteAs(StatementFactory & factory);
void registerStatementGrant(StatementFactory & factory);
void registerStatementMaskingPolicy(StatementFactory & factory);
void registerStatementMoveAccessEntity(StatementFactory & factory);
void registerStatementQuota(StatementFactory & factory);
void registerStatementRole(StatementFactory & factory);
void registerStatementRowPolicy(StatementFactory & factory);
void registerStatementSetRole(StatementFactory & factory);
void registerStatementSettingsProfile(StatementFactory & factory);
void registerStatementUser(StatementFactory & factory);

void registerStatements();

}
