#pragma once

#include <Analyzer/HashUtils.h>
#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

/** Table node represents table in query tree.
  * Example: SELECT a FROM test_table.
  * test_table - is identifier, that during query analysis pass must be resolved into table node.
  */
class TableNode;
using TableNodePtr = std::shared_ptr<TableNode>;

struct StorageSnapshot;
using StorageSnapshotPtr = std::shared_ptr<StorageSnapshot>;

struct TemporaryTableHolder;
using TemporaryTableHolderPtr = std::shared_ptr<TemporaryTableHolder>;

class TableNode : public IQueryTreeNode
{
public:
    /// Construct table node with storage, storage id, storage lock, storage snapshot
    explicit TableNode(StoragePtr storage_, StorageID storage_id_, TableLockHolder storage_lock_, StorageSnapshotPtr storage_snapshot_);

    /// Construct table node with storage, storage lock, storage snapshot
    explicit TableNode(StoragePtr storage_, TableLockHolder storage_lock_, StorageSnapshotPtr storage_snapshot_);

    /// Construct table node with storage, context
    explicit TableNode(StoragePtr storage_, const ContextPtr & context);

    explicit TableNode(
        TemporaryTableHolderPtr temporary_table_holder_,
        QueryTreeNodePtr materialized_cte_subquery_,
        const ContextPtr & context_);

    /** Update table node storage.
      * After this call storage, storage_id, storage_lock, storage_snapshot will be updated using new storage.
      */
    void updateStorage(StoragePtr storage_value, const ContextPtr & context);

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

    /// Get temporary table name
    const std::string & getTemporaryTableName() const
    {
        return temporary_table_name;
    }

    /// Set temporary table name
    void setTemporaryTableName(std::string temporary_table_name_value)
    {
        temporary_table_name = std::move(temporary_table_name_value);
    }

    /// Return true if table node has table expression modifiers, false otherwise
    bool hasTableExpressionModifiers() const
    {
        return table_expression_modifiers.has_value();
    }

    /// Get table expression modifiers
    const std::optional<TableExpressionModifiers> & getTableExpressionModifiers() const
    {
        return table_expression_modifiers;
    }

    /// Get table expression modifiers
    std::optional<TableExpressionModifiers> & getTableExpressionModifiers()
    {
        return table_expression_modifiers;
    }

    /// Set table expression modifiers
    void setTableExpressionModifiers(TableExpressionModifiers table_expression_modifiers_value)
    {
        table_expression_modifiers = std::move(table_expression_modifiers_value);
    }

    const TemporaryTableHolderPtr & getTemporaryTableHolder() const
    {
        return temporary_table_holder;
    }

    bool isMaterializedCTE() const
    {
        return children[materialized_cte_subquery_index] != nullptr;
    }

    const QueryTreeNodePtr & getMaterializedCTESubquery() const
    {
        return children[materialized_cte_subquery_index];
    }

    QueryTreeNodePtr & getMaterializedCTESubquery()
    {
        return children[materialized_cte_subquery_index];
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::TABLE;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    boost::intrusive_ptr<ASTTableIdentifier> toASTIdentifier() const;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    StoragePtr storage;
    StorageID storage_id;
    TableLockHolder storage_lock;
    StorageSnapshotPtr storage_snapshot;
    std::optional<TableExpressionModifiers> table_expression_modifiers;
    std::string temporary_table_name;
    TemporaryTableHolderPtr temporary_table_holder;

    static constexpr size_t materialized_cte_subquery_index = 0;
    static constexpr size_t children_size = materialized_cte_subquery_index + 1;
};

}

