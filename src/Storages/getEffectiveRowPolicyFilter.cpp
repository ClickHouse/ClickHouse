#include <Storages/getEffectiveRowPolicyFilter.h>

#include <Access/Common/RowPolicyDefs.h>
#include <Core/Names.h>
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
    /// A synthetic data source is not a table of the catalog, and it may borrow the name of one,
    /// so the row policies of that name have nothing to do with the rows it produces.
    if (storage.isSyntheticDataSource())
        return;

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

}
