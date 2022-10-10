#pragma once

#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Storages/StorageSnapshot.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

/** Table node represents table in query tree.
  * Example: SELECT a FROM test_table.
  * test_table - is identifier, that during query analysis pass must be resolved into table node.
  *
  * During construction table node:
  * 1. Lock storage for share. Later lock can be moved out of node using `moveTableLock` method.
  * 2. Take storage snapshot.
  */
class TableNode;
using TableNodePtr = std::shared_ptr<TableNode>;

class TableNode : public IQueryTreeNode
{
public:
    /// Construct table node with storage, storage id, storage lock, storage snapshot
    explicit TableNode(StoragePtr storage_, StorageID storage_id_, TableLockHolder storage_lock_, StorageSnapshotPtr storage_snapshot_);

    /// Construct table node with storage, storage lock, storage snapshot
    explicit TableNode(StoragePtr storage_, TableLockHolder storage_lock_, StorageSnapshotPtr storage_snapshot_);

    /// Get storage
    const StoragePtr & getStorage() const
    {
        return storage;
    }

    /// Get storage id
    const StorageID & getStorageID() const
    {
        return storage_id;
    }

    /// Get storage snapshot
    const StorageSnapshotPtr & getStorageSnapshot() const
    {
        return storage_snapshot;
    }

    /// Get storage lock
    const TableLockHolder & getStorageLock() const
    {
        return storage_lock;
    }

    /// Return true if table node has table expression modifiers, false otherwise
    bool hasTableExpressionModifiers() const
    {
        return table_expression_modifiers.has_value();
    }

    /// Get table expression modifiers
    std::optional<TableExpressionModifiers> getTableExpressionModifiers() const
    {
        return table_expression_modifiers;
    }

    /// Set table expression modifiers
    void setTableExpressionModifiers(TableExpressionModifiers table_expression_modifiers_value)
    {
        table_expression_modifiers = std::move(table_expression_modifiers_value);
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::TABLE;
    }

    String getName() const override;

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & state) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl() const override;

private:
    StoragePtr storage;
    StorageID storage_id;
    TableLockHolder storage_lock;
    StorageSnapshotPtr storage_snapshot;
    std::optional<TableExpressionModifiers> table_expression_modifiers;

    static constexpr size_t children_size = 0;
};

}

