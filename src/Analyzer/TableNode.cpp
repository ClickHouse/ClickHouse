#include <Analyzer/TableNode.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>

#include <Storages/IStorage.h>

#include <Interpreters/Context.h>

namespace DB
{

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

void TableNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "TABLE id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", table_name: " << storage_id.getFullNameNotQuoted();

    if (table_expression_modifiers)
    {
        buffer << ", ";
        table_expression_modifiers->dump(buffer);
    }
}

bool TableNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const TableNode &>(rhs);

    if (table_expression_modifiers && rhs_typed.table_expression_modifiers && table_expression_modifiers != rhs_typed.table_expression_modifiers)
        return false;
    else if (table_expression_modifiers && !rhs_typed.table_expression_modifiers)
        return false;
    else if (!table_expression_modifiers && rhs_typed.table_expression_modifiers)
        return false;

    return storage_id == rhs_typed.storage_id;
}

void TableNode::updateTreeHashImpl(HashState & state) const
{
    auto full_name = storage_id.getFullNameNotQuoted();
    state.update(full_name.size());
    state.update(full_name);

    if (table_expression_modifiers)
        table_expression_modifiers->updateTreeHash(state);
}

String TableNode::getName() const
{
    return storage->getStorageID().getFullNameNotQuoted();
}

QueryTreeNodePtr TableNode::cloneImpl() const
{
    auto result_table_node = std::make_shared<TableNode>(storage, storage_id, storage_lock, storage_snapshot);
    result_table_node->table_expression_modifiers = table_expression_modifiers;

    return result_table_node;
}

ASTPtr TableNode::toASTImpl() const
{
    return std::make_shared<ASTTableIdentifier>(storage_id.getDatabaseName(), storage_id.getTableName());
}

}
