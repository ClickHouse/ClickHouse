#include <Analyzer/TableNode.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Interpreters/MaterializedCTE.h>
#include <Parsers/ASTIdentifier.h>

#include <DataTypes/DataTypesNumber.h>
#include <Storages/IStorage.h>
#include <Storages/StorageDummy.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageView.h>

#include <Parsers/IAST.h>

#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>

#include <Core/Settings.h>
#include <Common/SipHash.h>
#include <Common/assert_cast.h>

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

namespace
{

MaterializedCTEPtr extractCTE(StoragePtr storage)
{
    auto * storage_memory = typeid_cast<StorageMemory *>(storage.get());
    if (!storage_memory)
        return {};
    return storage_memory->getMaterializedCTE();
}

/// Each call to a parameterized view with a different set of argument values produces a fresh
/// `StorageView` that carries the substituted inner query but shares the original `StorageID`.
/// Return that substituted query so callers can distinguish such storages by content.
ASTPtr getParameterizedViewInnerQuery(const StoragePtr & storage)
{
    if (!storage)
        return nullptr;
    auto * view = storage->as<StorageView>();
    if (!view || !view->isParameterizedView())
        return nullptr;
    auto metadata_snapshot = view->getInMemoryMetadataPtr(nullptr, false);
    return metadata_snapshot->getSelectQuery().inner_query;
}

}

TableNode::TableNode(StoragePtr storage_, StorageID storage_id_, TableLockHolder storage_lock_, StorageSnapshotPtr storage_snapshot_)
    : IQueryTreeNode(children_size)
    , storage(std::move(storage_))
    , storage_id(std::move(storage_id_))
    , storage_lock(std::move(storage_lock_))
    , storage_snapshot(std::move(storage_snapshot_))
    , materialized_cte(extractCTE(storage))
{}

TableNode::TableNode(StoragePtr storage_, TableLockHolder storage_lock_, StorageSnapshotPtr storage_snapshot_)
    : TableNode(storage_, storage_->getStorageID(), std::move(storage_lock_), std::move(storage_snapshot_))
{
}

TableNode::TableNode(StoragePtr storage_, const ContextPtr & context)
    : TableNode(
          storage_,
          storage_->lockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]),
          storage_->getStorageSnapshot(storage_->getInMemoryMetadataPtr(context, false), context))
{
}

TableNode::TableNode(
    const std::string & cte_name_,
    QueryTreeNodePtr materialized_cte_subquery_,
    const ContextPtr & context_)
    : TableNode(
        std::make_shared<StorageDummy>(
            StorageID{"_dummy", "_materialized_cte_" + cte_name_},
            ColumnsDescription{NamesAndTypesList{{"_dummy", std::make_shared<DataTypeUInt8>()}}}),
        context_)
{
    materialized_cte = std::make_shared<MaterializedCTE>(cte_name_);
    children[materialized_cte_subquery_index] = std::move(materialized_cte_subquery_);
    setTemporaryTableName(materialized_cte->temporary_table_name);
}

void TableNode::finalizeMaterializedCTE(TemporaryTableHolder temporary_table_holder_, const ContextPtr & context_)
{
    auto real_storage = temporary_table_holder_.getTable();
    materialized_cte->storage = real_storage;
    materialized_cte->table_holder = std::move(temporary_table_holder_);
    typeid_cast<StorageMemory *>(real_storage.get())->setMaterializedCTE(materialized_cte);
    updateStorage(std::move(real_storage), context_);
}

void TableNode::updateStorage(StoragePtr storage_value, const ContextPtr & context)
{
    storage = std::move(storage_value);
    storage_id = storage->getStorageID();
    storage_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
    storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(context, false), context);
}

void TableNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "TABLE id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", table_name: " << storage_id.getFullNameNotQuoted();

    if (!temporary_table_name.empty())
        buffer << ", temporary_table_name: " << temporary_table_name;

    if (table_expression_modifiers)
    {
        buffer << ", ";
        table_expression_modifiers->dump(buffer);
    }

    if (isMaterializedCTE())
    {
        buffer << '\n' << std::string(indent + 2, ' ') << "MATERIALIZED CTE SUBQUERY\n";
        getMaterializedCTESubquery()->dumpTreeImpl(buffer, format_state, indent + 4);
    }
}

bool TableNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const TableNode &>(rhs);
    if (storage_id != rhs_typed.storage_id
        || table_expression_modifiers != rhs_typed.table_expression_modifiers
        || temporary_table_name != rhs_typed.temporary_table_name)
        return false;

    /// Parameterized views: two calls with different argument values share the same `StorageID` but
    /// hold different substituted queries. Compare the substituted queries so downstream passes that
    /// key off the tree hash (e.g. `PreparedSets` in `CollectSets`) do not collapse them.
    auto lhs_inner = getParameterizedViewInnerQuery(storage);
    auto rhs_inner = getParameterizedViewInnerQuery(rhs_typed.storage);
    if (lhs_inner || rhs_inner)
    {
        if (!lhs_inner || !rhs_inner)
            return false;
        return lhs_inner->getTreeHash(/*ignore_aliases=*/false) == rhs_inner->getTreeHash(/*ignore_aliases=*/false);
    }

    return true;
}

void TableNode::updateTreeHashImpl(HashState & state, CompareOptions) const
{
    if (!temporary_table_name.empty())
    {
        state.update(temporary_table_name.size());
        state.update(temporary_table_name);
    }
    else
    {
        // In case of cross-replication we don't know what database is used for the table.
        // `storage_id.hasDatabase()` can return false only on the initiator node.
        // Each shard will use the default database (in the case of cross-replication shards may have different defaults).
        auto full_name = storage_id.hasDatabase() ? storage_id.getFullNameNotQuoted() : storage_id.getTableName();
        state.update(full_name.size());
        state.update(full_name);
    }

    if (table_expression_modifiers)
        table_expression_modifiers->updateTreeHash(state);

    /// See note in `isEqualImpl`: parameterized-view calls reuse the same `StorageID`, so we mix in
    /// the substituted inner query to keep distinct argument values hashing to distinct values.
    if (auto inner_query = getParameterizedViewInnerQuery(storage))
        inner_query->updateTreeHash(state, /*ignore_aliases=*/false);
}

QueryTreeNodePtr TableNode::cloneImpl() const
{
    auto result_table_node = std::make_shared<TableNode>(storage, storage_id, storage_lock, storage_snapshot);
    result_table_node->table_expression_modifiers = table_expression_modifiers;
    result_table_node->temporary_table_name = temporary_table_name;

    result_table_node->materialized_cte = materialized_cte;

    return result_table_node;
}

ASTPtr TableNode::toASTImpl(const ConvertToASTOptions & /* options */) const
{
    return toASTIdentifier();
}

boost::intrusive_ptr<ASTTableIdentifier> TableNode::toASTIdentifier() const
{
    if (!temporary_table_name.empty())
        return make_intrusive<ASTTableIdentifier>(temporary_table_name);

    // In case of cross-replication we don't know what database is used for the table.
    // `storage_id.hasDatabase()` can return false only on the initiator node.
    // Each shard will use the default database (in the case of cross-replication shards may have different defaults).
    if (!storage_id.hasDatabase())
        return make_intrusive<ASTTableIdentifier>(storage_id.getTableName());
    return make_intrusive<ASTTableIdentifier>(storage_id.getDatabaseName(), storage_id.getTableName());
}

}
