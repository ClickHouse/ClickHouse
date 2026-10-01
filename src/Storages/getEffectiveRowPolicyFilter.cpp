#include <Storages/getEffectiveRowPolicyFilter.h>

#include <Access/Common/RowPolicyDefs.h>
#include <Access/ContextAccess.h>
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
    auto storage_id = storage.getStorageID();
    if (!storage_id.hasDatabase() || !visited.emplace(storage_id.getFullTableName()).second)
        return;

    result = combineRowPolicyFilters(
        std::move(result),
        context->getRowPolicyFilter(storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER));

    /// An Open Policy Agent filter is combined here rather than inside `getRowPolicyFilter`, because a
    /// filter with no `RowPolicy` entity behind it means something specific there - a table that has
    /// policies, none of which match this user - and an OPA filter never has one.
    result = combineRowPolicyFilters(
        std::move(result),
        context->getAccess()->getOpaRowFilter(context, storage_id.getDatabaseName(), storage_id.getTableName()));

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
