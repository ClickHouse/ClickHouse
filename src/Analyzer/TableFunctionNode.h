#pragma once

#include <Storages/IStorage_fwd.h>
#include <Storages/TableLockHolder.h>
#include <Storages/StorageSnapshot.h>

#include <Interpreters/Context_fwd.h>
#include <Interpreters/StorageID.h>

#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>


namespace DB
{

class ITableFunction;
using TableFunctionPtr = std::shared_ptr<ITableFunction>;

/** Table function node represents table function in query tree.
  * Example: SELECT a FROM table_function(arguments...).
  *
  * In query tree table function arguments are represented by ListNode.
  *
  * Table function resolution must be done during query analysis pass.
  */
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

    const StoragePtr & getStorageOrThrow() const
    {
        if (!storage)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Function node is not resolved");

        return storage;
    }

    /// Resolve table function with table_function, storage and context
    void resolve(TableFunctionPtr table_function_value, StoragePtr storage_value, ContextPtr context);

    /// Get storage id, throws exception if function node is not resolved
    const StorageID & getStorageID() const;

    /// Get storage snapshot, throws exception if function node is not resolved
    const StorageSnapshotPtr & getStorageSnapshot() const;

    QueryTreeNodeType getNodeType() const override
    {
        return QueryTreeNodeType::TABLE_FUNCTION;
    }

    String getName() const override;

    void dumpTreeImpl(WriteBuffer & buffer, FormatState & format_state, size_t indent) const override;

    bool isEqualImpl(const IQueryTreeNode & rhs) const override;

    void updateTreeHashImpl(HashState & state) const override;

protected:
    ASTPtr toASTImpl() const override;

    QueryTreeNodePtr cloneImpl() const override;

private:
    static constexpr size_t arguments_child_index = 0;

    String table_function_name;
    TableFunctionPtr table_function;
    StoragePtr storage;
    StorageID storage_id;
    StorageSnapshotPtr storage_snapshot;
};

}

