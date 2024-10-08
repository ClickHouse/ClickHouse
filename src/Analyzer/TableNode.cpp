#include <Analyzer/TableNode.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>

#include <Storages/IStorage.h>

#include <Interpreters/Context.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsSeconds lock_acquire_timeout;
}

TableNode::TableNode(StoragePtr storage_, StorageID storage_id_, TableLockHolder storage_lock_, StorageSnapshotPtr storage_snapshot_)
    : IQueryTreeNode(children_size)
    , storage(std::move(storage_))
    , storage_id(std::move(storage_id_))
    , storage_lock(std::move(storage_lock_))
    , storage_snapshot(std::move(storage_snapshot_))
{}

TableNode::TableNode(StoragePtr storage_, TableLockHolder storage_lock_, StorageSnapshotPtr storage_snapshot_)
    : TableNode(storage_, storage_->getStorageID(), std::move(storage_lock_), std::move(storage_snapshot_))
{
}

TableNode::TableNode(StoragePtr storage_, const ContextPtr & context)
    : TableNode(
          storage_,
          storage_->lockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]),
          storage_->getStorageSnapshot(storage_->getInMemoryMetadataPtr(), context))
{
}

void TableNode::updateStorage(StoragePtr storage_value, const ContextPtr & context)
{
    storage = std::move(storage_value);
    storage_id = storage->getStorageID();
    storage_lock = storage->lockForShare(context->getInitialQueryId(), context->getSettingsRef()[Setting::lock_acquire_timeout]);
    storage_snapshot = storage->getStorageSnapshot(storage->getInMemoryMetadataPtr(), context);
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
}

bool TableNode::isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const
{
    const auto & rhs_typed = assert_cast<const TableNode &>(rhs);
    return storage_id == rhs_typed.storage_id && table_expression_modifiers == rhs_typed.table_expression_modifiers &&
        temporary_table_name == rhs_typed.temporary_table_name;
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
        auto full_name = storage_id.getFullNameNotQuoted();
        state.update(full_name.size());
        state.update(full_name);
    }

    if (table_expression_modifiers)
        table_expression_modifiers->updateTreeHash(state);
}

QueryTreeNodePtr TableNode::cloneImpl() const
{
    auto result_table_node = std::make_shared<TableNode>(storage, storage_id, storage_lock, storage_snapshot);
    result_table_node->table_expression_modifiers = table_expression_modifiers;
    result_table_node->temporary_table_name = temporary_table_name;

    return result_table_node;
}

ASTPtr TableNode::toASTImpl(const ConvertToASTOptions & /* options */) const
{
    if (!temporary_table_name.empty())
        return std::make_shared<ASTTableIdentifier>(temporary_table_name);

    // In case of cross-replication we don't know what database is used for the table.
    // `storage_id.hasDatabase()` can return false only on the initiator node.
    // Each shard will use the default database (in the case of cross-replication shards may have different defaults).
    if (!storage_id.hasDatabase())
        return std::make_shared<ASTTableIdentifier>(storage_id.getTableName());
    return std::make_shared<ASTTableIdentifier>(storage_id.getDatabaseName(), storage_id.getTableName());
}

}
