#include <Interpreters/Access/checkRowPolicyForBlendingEngine.h>

#include <Access/AccessControl.h>
#include <Access/RowPolicy.h>
#include <Core/Names.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_suspicious_row_policies_with_blending_engines;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// The visited set terminates the walk if the chain of underlying storages loops
const IStorage * findBlendingEngineImpl(const IStorage & storage, NameSet & visited)
{
    if (storage.isBlendingEngine())
        return &storage;

    if (!visited.emplace(storage.getStorageID().getNameForLogs()).second)
        return nullptr;

    for (const auto & underlying : storage.getUnderlyingStorages())
    {
        if (!underlying)
            continue;
        if (const auto * blending = findBlendingEngineImpl(*underlying, visited))
            return blending;
    }
    return nullptr;
}

}

const IStorage * findBlendingEngine(const IStorage & storage)
{
    NameSet visited;
    return findBlendingEngineImpl(storage, visited);
}

void checkNoRowPolicyForBlendingEngine(
    const IStorage & storage,
    const StorageID & table_id,
    bool include_database_policies,
    const ContextPtr & context,
    const IStorage * replaced)
{
    if (context->getSettingsRef()[Setting::allow_suspicious_row_policies_with_blending_engines])
        return;

    const auto & access_control = context->getAccessControl();
    for (const auto & id : access_control.findAll<RowPolicy>())
    {
        auto policy = access_control.tryRead<RowPolicy>(id);
        if (!policy || policy->getDatabase() != table_id.database_name)
            continue;
        if (policy->isForDatabase() ? !include_database_policies : policy->getTableName() != table_id.table_name)
            continue;

        /// Resolving the storages can load a lazy table, so it happens only after a policy is found
        const auto * blending = findBlendingEngine(storage);
        if (!blending || (replaced && findBlendingEngine(*replaced)))
            return;

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Row policy {} is used for table {}, which {} the {} engine: a merge takes the values of all the rows with "
            "the same sorting key into one row, so the policy does not hide the values of the rows it filters out. "
            "Drop the policy and define it on the table which stores the raw rows, or set "
            "allow_suspicious_row_policies_with_blending_engines = 1 to continue anyway",
            policy->getName(),
            table_id.getNameForLogs(),
            blending == &storage ? "has" : "reads a table with",
            blending->getName());
    }
}

}
