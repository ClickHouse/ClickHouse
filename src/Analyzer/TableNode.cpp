#include <Analyzer/TableNode.h>

#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>

#include <Parsers/ASTIdentifier.h>

#include <Storages/IStorage.h>

#include <Interpreters/Context.h>

namespace DB
{

TableNode::TableNode(StoragePtr storage_, TableLockHolder storage_lock_, StorageSnapshotPtr storage_snapshot_)
    : storage(std::move(storage_))
    , storage_id(storage->getStorageID())
    , storage_lock(std::move(storage_lock_))
    , storage_snapshot(std::move(storage_snapshot_))
{
}

TableNode::TableNode() : storage_id("system", "one") {}

void TableNode::dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const
{
    buffer << std::string(indent, ' ') << "TABLE id: " << format_state.getNodeId(this);

    if (hasAlias())
        buffer << ", alias: " << getAlias();

    buffer << ", table_name: " << storage_id.getFullNameNotQuoted();
}

bool TableNode::isEqualImpl(const IQueryTreeNode & rhs) const
{
    const auto & rhs_typed = assert_cast<const TableNode &>(rhs);
    return storage_id == rhs_typed.storage_id;
}

void TableNode::updateTreeHashImpl(HashState & state) const
{
    auto full_name = storage_id.getFullNameNotQuoted();
    state.update(full_name.size());
    state.update(full_name);
}

String TableNode::getName() const
{
    return storage->getStorageID().getFullNameNotQuoted();
}

ASTPtr TableNode::toASTImpl() const
{
    return std::make_shared<ASTTableIdentifier>(storage_id.getDatabaseName(), storage_id.getTableName());
}

QueryTreeNodePtr TableNode::cloneImpl() const
{
    TableNodePtr result_table_node(new TableNode());

    result_table_node->storage = storage;
    result_table_node->storage_id = storage_id;
    result_table_node->storage_lock = storage_lock;
    result_table_node->storage_snapshot = storage_snapshot;

    return result_table_node;
}

}
