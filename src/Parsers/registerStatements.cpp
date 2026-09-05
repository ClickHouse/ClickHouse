#include <Parsers/registerStatements.h>

#include <Parsers/StatementFactory.h>


namespace DB
{

void registerStatements()
{
    auto & factory = StatementFactory::instance();

    registerStatementAlter(factory);
    registerStatementAlterNamedCollection(factory);
    registerStatementCheck(factory);
    registerStatementColumnsTransformers(factory);
    registerStatementCreate(factory);
    registerStatementCreateFunction(factory);
    registerStatementCreateHandler(factory);
    registerStatementDelete(factory);
    registerStatementDescribeTable(factory);
    registerStatementDrop(factory);
    registerStatementExists(factory);
    registerStatementExplain(factory);
    registerStatementHypotheticalIndex(factory);
    registerStatementHypotheticalProjection(factory);
    registerStatementIn(factory);
    registerStatementInsert(factory);
    registerStatementKillQuery(factory);
    registerStatementOnCluster(factory);
    registerStatementOptimize(factory);
    registerStatementParallelWith(factory);
    registerStatementPipeOperators(factory);
    registerStatementQueryWithOutput(factory);
    registerStatementRename(factory);
    registerStatementSelect(factory);
    registerStatementSet(factory);
    registerStatementShow(factory);
    registerStatementSystem(factory);
    registerStatementTablesInSelect(factory);
    registerStatementUndrop(factory);
    registerStatementUnion(factory);
    registerStatementUpdate(factory);
    registerStatementUse(factory);
    registerStatementWith(factory);
    registerStatementCheckGrant(factory);
    registerStatementExecuteAs(factory);
    registerStatementGrant(factory);
    registerStatementMaskingPolicy(factory);
    registerStatementMoveAccessEntity(factory);
    registerStatementQuota(factory);
    registerStatementRole(factory);
    registerStatementRowPolicy(factory);
    registerStatementSetRole(factory);
    registerStatementSettingsProfile(factory);
    registerStatementUser(factory);
}

}
