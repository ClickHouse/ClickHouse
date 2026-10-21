#include <Interpreters/Access/checkRowPolicyForBlendingEngine.h>

#include <Access/AccessControl.h>
#include <Access/ContextAccess.h>
#include <Access/RowPolicy.h>
#include <Common/typeid_cast.h>
#include <Core/Names.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageMaterializedView.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_suspicious_row_policies_with_blending_engines;
}

namespace ErrorCodes
{
    extern const int ACCESS_DENIED;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// A materialized view and a `Buffer` hand the row policy filter to the read of their target, but do not list it
/// as an underlying storage, because the target's own policies must not apply to them
std::vector<StoragePtr> getReadTargets(const IStorage & storage)
{
    if (const auto * view = typeid_cast<const StorageMaterializedView *>(&storage))
        return {view->tryGetTargetTable()};
    if (const auto * buffer = typeid_cast<const StorageBuffer *>(&storage))
        return {buffer->getDestinationTable()};
    return storage.getUnderlyingStorages();
}

/// The visited set terminates the walk if the chain of underlying storages loops
ConstStoragePtr findBlendingEngineImpl(const IStorage & storage, const ContextPtr & context, NameSet & visited)
{
    if (storage.isBlendingEngine())
        return storage.shared_from_this();

    if (!visited.emplace(storage.getStorageID().getNameForLogs()).second)
        return nullptr;

    /// The inner table of a materialized view is a part of the view, so the user sees its engine anyway
    const auto * view = typeid_cast<const StorageMaterializedView *>(&storage);
    bool check_access = !view || !view->hasInnerTable();

    for (const auto & target : getReadTargets(storage))
    {
        if (!target)
            continue;

        /// Otherwise the answer tells the engine of a table which the user cannot see
        auto target_id = target->getStorageID();
        if (check_access && !context->getAccess()->isGranted(AccessType::SHOW_TABLES, target_id.database_name, target_id.table_name))
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "Not enough privileges to check the engine of a table which {} reads: a row policy on a table which reads "
                "a table with an engine that merges rows does not hide the values of the rows it filters out. "
                "Grant SHOW TABLES on the tables it reads, or set allow_suspicious_row_policies_with_blending_engines = 1 to continue anyway",
                storage.getStorageID().getNameForLogs());

        if (auto blending = findBlendingEngineImpl(*target, context, visited))
            return blending;
    }
    return nullptr;
}

}

ConstStoragePtr findBlendingEngine(const IStorage & storage, const ContextPtr & context)
{
    NameSet visited;
    return findBlendingEngineImpl(storage, context, visited);
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
        auto blending = findBlendingEngine(storage, context);
        if (!blending || (replaced && findBlendingEngine(*replaced, context)))
            return;

        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "A row policy is used for table {}, which {} the {} engine: a merge takes the values of all the rows with "
            "the same sorting key into one row, so the policy does not hide the values of the rows it filters out. "
            "Drop the policy and define it on the table which stores the raw rows, or set "
            "allow_suspicious_row_policies_with_blending_engines = 1 to continue anyway",
            table_id.getNameForLogs(),
            blending.get() == &storage ? "has" : "reads a table with",
            blending->getName());
    }
}

}
