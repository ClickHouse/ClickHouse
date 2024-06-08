#pragma once

#include <Common/SettingsChanges.h>

#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Storages/StorageSnapshot.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>
#include <Analyzer/TableExpressionModifiers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/** Table function node represents table function in query tree.
  * Example: SELECT a FROM table_function(arguments...).
  *
  * In query tree table function arguments are represented by ListNode.
  *
  * Table function resolution must be done during query analysis pass.
  */
class ITableFunction;
using TableFunctionPtr = std::shared_ptr<ITableFunction>;

class TableFunctionNode;
using TableFunctionNodePtr = std::shared_ptr<TableFunctionNode>;

class TableFunctionNode : public IQueryTreeNode
{
public:
    /// Construct table function node with table function name
    explicit TableFunctionNode(String table_function_name);

    /// Get table function name
    const String & getTableFunctionName() const
    {
        return table_function_name;
    }

    /// Get arguments
    const ListNode & getArguments() const
    {
        return children[arguments_child_index]->as<const ListNode &>();
    }

    /// Get arguments
    ListNode & getArguments()
    {
        return children[arguments_child_index]->as<ListNode &>();
    }

    /// Get arguments node
    const QueryTreeNodePtr & getArgumentsNode() const
    {
        return children[arguments_child_index];
    }

    /// Get arguments node
    QueryTreeNodePtr & getArgumentsNode()
    {
        return children[arguments_child_index];
    }

    /// Returns true, if table function is resolved, false otherwise
    bool isResolved() const
    {
        return storage != nullptr && table_function != nullptr;
    }

    /// Get table function, returns nullptr if table function node is not resolved
    const TableFunctionPtr & getTableFunction() const
    {
        return table_function;
    }

    /// Get storage, returns nullptr if table function node is not resolved
    const StoragePtr & getStorage() const
    {
        return storage;
    }

    /// Get storage, throws exception if table function node is not resolved
    const StoragePtr & getStorageOrThrow() const
    {
        if (!storage)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Table function node is not resolved");

        return storage;
    }

    /// Resolve table function with table function, storage and context
    void resolve(TableFunctionPtr table_function_value, StoragePtr storage_value, ContextPtr context, std::vector<size_t> unresolved_arguments_indexes_);

    /// Get storage id, throws exception if function node is not resolved
    const StorageID & getStorageID() const;

    /// Get storage snapshot, throws exception if function node is not resolved
    const StorageSnapshotPtr & getStorageSnapshot() const;

    const std::vector<size_t> & getUnresolvedArgumentIndexes() const
    {
        return unresolved_arguments_indexes;
    }

    /// Return true if table function node has table expression modifiers, false otherwise
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

    /// Get settings changes passed to table function
    const SettingsChanges & getSettingsChanges() const
    {
        return settings_changes;
    }

    /// Set settings changes passed as last argument to table function
    void setSettingsChanges(SettingsChanges settings_changes_)
    {
        settings_changes = std::move(settings_changes_);
    }

    /// Set table expression modifiers
    void setTableExpressionModifiers(TableExpressionModifiers table_expression_modifiers_value)
    {
        table_expression_modifiers = std::move(table_expression_modifiers_value);
    }

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::TABLE_FUNCTION;
    }

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

protected:
    bool isEqualImpl(const IQueryTreeNode & rhs, CompareOptions) const override;

    void updateTreeHashImpl(HashState & state, CompareOptions) const override;

    QueryTreeNodePtr cloneImpl() const override;

    ASTPtr toASTImpl(const ConvertToASTOptions & options) const override;

private:
    String table_function_name;
    TableFunctionPtr table_function;
    StoragePtr storage;
    StorageID storage_id;
    StorageSnapshotPtr storage_snapshot;
    std::vector<size_t> unresolved_arguments_indexes;
    std::optional<TableExpressionModifiers> table_expression_modifiers;
    SettingsChanges settings_changes;

    static constexpr size_t arguments_child_index = 0;
    static constexpr size_t children_size = arguments_child_index + 1;
};

}

