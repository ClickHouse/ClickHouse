#pragma once


namespace DB
{
class InterpreterFactory;

void registerStatementAlter(InterpreterFactory & factory);
void registerStatementAlterNamedCollection(InterpreterFactory & factory);
void registerStatementCheck(InterpreterFactory & factory);
void registerStatementColumnsTransformers(InterpreterFactory & factory);
void registerStatementCreate(InterpreterFactory & factory);
void registerStatementCreateFunction(InterpreterFactory & factory);
void registerStatementDelete(InterpreterFactory & factory);
void registerStatementDescribeTable(InterpreterFactory & factory);
void registerStatementDrop(InterpreterFactory & factory);
void registerStatementExists(InterpreterFactory & factory);
void registerStatementExplain(InterpreterFactory & factory);
void registerStatementHypotheticalIndex(InterpreterFactory & factory);
void registerStatementIn(InterpreterFactory & factory);
void registerStatementInsert(InterpreterFactory & factory);
void registerStatementKillQuery(InterpreterFactory & factory);
void registerStatementOnCluster(InterpreterFactory & factory);
void registerStatementOptimize(InterpreterFactory & factory);
void registerStatementParallelWith(InterpreterFactory & factory);
void registerStatementPipeOperators(InterpreterFactory & factory);
void registerStatementQueryWithOutput(InterpreterFactory & factory);
void registerStatementRename(InterpreterFactory & factory);
void registerStatementSelect(InterpreterFactory & factory);
void registerStatementSet(InterpreterFactory & factory);
void registerStatementShow(InterpreterFactory & factory);
void registerStatementSystem(InterpreterFactory & factory);
void registerStatementTablesInSelect(InterpreterFactory & factory);
void registerStatementUndrop(InterpreterFactory & factory);
void registerStatementUnion(InterpreterFactory & factory);
void registerStatementUpdate(InterpreterFactory & factory);
void registerStatementUse(InterpreterFactory & factory);
void registerStatementWatch(InterpreterFactory & factory);
void registerStatementWith(InterpreterFactory & factory);
void registerStatementCheckGrant(InterpreterFactory & factory);
void registerStatementExecuteAs(InterpreterFactory & factory);
void registerStatementGrant(InterpreterFactory & factory);
void registerStatementMaskingPolicy(InterpreterFactory & factory);
void registerStatementMoveAccessEntity(InterpreterFactory & factory);
void registerStatementQuota(InterpreterFactory & factory);
void registerStatementRole(InterpreterFactory & factory);
void registerStatementRowPolicy(InterpreterFactory & factory);
void registerStatementSetRole(InterpreterFactory & factory);
void registerStatementSettingsProfile(InterpreterFactory & factory);
void registerStatementUser(InterpreterFactory & factory);

void registerStatements();

}
