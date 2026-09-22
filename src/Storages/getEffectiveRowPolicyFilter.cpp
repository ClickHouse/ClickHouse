#include <Storages/getEffectiveRowPolicyFilter.h>

#include <Access/Common/RowPolicyDefs.h>
#include <Core/Names.h>
#include <Databases/DatabaseOverlay.h>
#include <Interpreters/Context.h>
#include <Interpreters/StorageID.h>
#include <Storages/IStorage.h>

namespace DB
{

namespace
{

/// The visited set terminates the walk: a storage contributes its policies once, even if the chain of underlying storages loops.
void collectRowPolicyFilters(const IStorage & storage, const ContextPtr & context, RowPolicyFilterPtr & result, NameSet & visited)
{
    auto storage_id = storage.getStorageID();
    if (!storage_id.hasDatabase() || !visited.emplace(storage_id.getFullTableName()).second)
        return;

    result = combineRowPolicyFilters(
        std::move(result),
        context->getRowPolicyFilter(storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER));

    for (const auto & underlying : storage.getUnderlyingStorages())
        if (underlying)
            collectRowPolicyFilters(*underlying, context, result, visited);
}

}

RowPolicyFilterPtr getRowPolicyFilterForStorage(const IStorage & storage, const ContextPtr & context)
{
    RowPolicyFilterPtr result;
    NameSet visited;
    collectRowPolicyFilters(storage, context, result, visited);
    return result;
}

RowPolicyFilterPtr getEffectiveRowPolicyFilter(const IStorage & storage, const ContextPtr & context)
{
    auto filter = getRowPolicyFilterForStorage(storage, context);
    if (!filter || filter->isAlwaysTrue())
        return nullptr;
    return filter;
}

RowPolicyFilterPtr getRowPolicyFilterForStorage(const IStorage & storage, const StorageID & as_written_id, const ContextPtr & context)
{
    auto result = getRowPolicyFilterForStorage(storage, context);

    if (const auto source_id = DatabaseOverlay::getSourceTableIdForReadonlyFacade(as_written_id, storage))
    {
        /// The id whose policies the walk above did not see: the facade name as written when the
        /// storage is the source table itself, the carried source id when the storage is the
        /// synthesized view that keeps the facade name.
        const auto storage_id = storage.getStorageID();
        const auto & other_id
            = (source_id->database_name == storage_id.database_name && source_id->table_name == storage_id.table_name)
            ? as_written_id
            : *source_id;
        result = combineRowPolicyFilters(
            std::move(result),
            context->getRowPolicyFilter(other_id.getDatabaseName(), other_id.getTableName(), RowPolicyFilterType::SELECT_FILTER));
    }

    return result;
}

RowPolicyFilterPtr getEffectiveRowPolicyFilter(const IStorage & storage, const StorageID & as_written_id, const ContextPtr & context)
{
    auto filter = getRowPolicyFilterForStorage(storage, as_written_id, context);
    if (!filter || filter->isAlwaysTrue())
        return nullptr;
    return filter;
}

}
