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
#include <Storages/StorageAlias.h>
#include <Storages/StorageBuffer.h>
#include <Storages/StorageMaterializedView.h>
#include <Storages/StorageMerge.h>
#include <Storages/StorageProxy.h>

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

/// The storages which `storage` hands the row policy filter to. Throws unless the user can see all the tables which
/// the definition of `storage` names for the read, whether they exist or not. Otherwise the answer tells the user
/// the engine of a table which the user cannot see, or that the table exists.
std::vector<StoragePtr> getReadTargets(const IStorage & storage, const ContextPtr & context)
{
    auto access = context->getAccess();
    auto can_see = [&](const StorageID & id) { return access->isGranted(AccessType::SHOW_TABLES, id.database_name, id.table_name); };
    auto check = [&](bool granted)
    {
        if (!granted)
            throw Exception(
                ErrorCodes::ACCESS_DENIED,
                "Not enough privileges to check the engine of the tables which {} reads: a row policy on a table which reads "
                "a table with an engine that merges rows does not hide the values of the rows it filters out. "
                "Grant SHOW TABLES on all the tables which it can read, or set allow_suspicious_row_policies_with_blending_engines = 1 "
                "to continue anyway",
                storage.getStorageID().getNameForLogs());
    };

    /// A materialized view, a `Buffer` and a `Merge` table hand the filter to the read of their targets, but do not list
    /// them as underlying storages, because the targets' own policies must not apply to them
    if (const auto * view = typeid_cast<const StorageMaterializedView *>(&storage))
    {
        /// The inner table of a materialized view is a part of the view, so the user sees its engine anyway
        check(view->hasInnerTable() || can_see(view->getTargetTableId()));
        return {view->tryGetTargetTable()};
    }
    if (const auto * buffer = typeid_cast<const StorageBuffer *>(&storage))
    {
        check(!buffer->getDestinationId() || can_see(buffer->getDestinationId()));
        return {buffer->getDestinationTable()};
    }
    if (const auto * merge = typeid_cast<const StorageMerge *>(&storage))
    {
        /// A regular expression selects the sources, so the user must see all the tables which it can select
        auto source_database = merge->getSourceDatabase();
        check(source_database ? access->isGranted(AccessType::SHOW_TABLES, *source_database) : access->isGranted(AccessType::SHOW_TABLES));
        std::vector<StoragePtr> sources;
        merge->hasChildTable([&](const StoragePtr & source)
        {
            sources.push_back(source);
            return false;
        });
        return sources;
    }
    if (const auto * alias = typeid_cast<const StorageAlias *>(&storage))
    {
        check(alias->isDeclaredTargetGranted(context, AccessType::SHOW_TABLES, {}));
        return {alias->tryGetTargetTable()};
    }

    auto targets = storage.getUnderlyingStorages();
    for (const auto & target : targets)
        check(!target || can_see(target->getStorageID()));
    return targets;
}

/// The visited set terminates the walk if the chain of underlying storages loops
ConstStoragePtr findBlendingEngineImpl(const IStorage & storage, const ContextPtr & context, NameSet & visited)
{
    if (storage.isBlendingEngine())
        return storage.shared_from_this();

    /// A table function or a lazily loaded table reads through the storage it creates, which has the same name
    if (const auto * proxy = dynamic_cast<const StorageProxy *>(&storage))
        return findBlendingEngineImpl(*proxy->getNested(), context, visited);

    if (!visited.emplace(storage.getStorageID().getNameForLogs()).second)
        return nullptr;

    for (const auto & target : getReadTargets(storage, context))
    {
        if (!target)
            continue;
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
