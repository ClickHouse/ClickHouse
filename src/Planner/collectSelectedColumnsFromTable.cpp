#include <Planner/collectSelectedColumnsFromTable.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/FunctionNode.h>
#include <Storages/IStorage.h>


namespace DB
{

namespace
{
class CollectSelectedColumnsFromTableVisitor : public InDepthQueryTreeVisitorWithContext<CollectSelectedColumnsFromTableVisitor>
{
public:
    /// Match columns either by the table's `StorageID` (every reference to that table anywhere in the
    /// tree) or by a specific table expression node instance. The instance form is needed when the same
    /// physical table is referenced from several scopes (e.g. once directly and once inside an inlined
    /// view): each instance then owns exactly the columns selected from it in its own scope. It also
    /// covers a table expression that is not a `TableNode` at all, such as the `TableFunctionNode` a
    /// parameterized view is resolved into.
    CollectSelectedColumnsFromTableVisitor(const StorageID & storage_id_, const IQueryTreeNode * table_expression_, const ContextPtr & context)
        : InDepthQueryTreeVisitorWithContext(context), storage_id(storage_id_), table_expression(table_expression_)
    {
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (isIndexHintFunction(node))
        {
            is_inside_index_hint_function = true;
            return;
        }

        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        if (column_node->getColumnName() == "__grouping_set")
            return;

        const auto & column_source = column_node->getColumnSource();
        if (table_expression)
        {
            if (column_source.get() != table_expression)
                return;
        }
        else
        {
            const auto * source_table = column_source->as<TableNode>();
            if (!source_table || source_table->getStorageID() != storage_id)
                return;
        }

        /// A special case for the "indexHint" function. We don't need its arguments for execution if column's source table is MergeTree.
        /// Instead, we prepare an ActionsDAG for its arguments and store it inside a function (see ActionsDAG::buildFilterActionsDAG).
        /// So this optimization allows not to read arguments of "indexHint" (if not needed in other contexts) but only to use index analysis for them.
        if (is_inside_index_hint_function)
        {
            StoragePtr source_storage;
            if (const auto * source_table = column_source->as<TableNode>())
                source_storage = source_table->getStorage();
            else if (const auto * source_table_function = column_source->as<TableFunctionNode>())
                source_storage = source_table_function->getStorage();

            if (source_storage && source_storage->isMergeTree())
                return;
        }

        selected_columns.insert(column_node->getColumnName());
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (isIndexHintFunction(node))
        {
            is_inside_index_hint_function = false;
            return;
        }
    }

    bool isAliasColumn(const QueryTreeNodePtr & node) const
    {
        const auto * column_node = node->as<ColumnNode>();
        if (!column_node || !column_node->hasExpression())
            return false;
        const auto & column_source = column_node->getColumnSourceOrNull();
        if (!column_source)
            return false;
        return column_source->getNodeType() == QueryTreeNodeType::TABLE;
    }

    bool needChildVisit(const QueryTreeNodePtr & parent_node, const QueryTreeNodePtr &) const
    {
        /// Don't go inside alias column expression.
        return !isAliasColumn(parent_node);
    }

    bool isIndexHintFunction(const QueryTreeNodePtr & node) const
    {
        return node->as<FunctionNode>() && node->as<FunctionNode>()->getFunctionName() == "indexHint";
    }

    std::vector<String> getSelectedColumns() const
    {
        return std::vector<String>(selected_columns.begin(), selected_columns.end());
    }

private:
    /// True if we are traversing arguments of function "indexHint".
    bool is_inside_index_hint_function = false;
    const StorageID & storage_id;
    /// When set, match columns by this exact table expression instance instead of by `storage_id`.
    const IQueryTreeNode * table_expression = nullptr;
    std::unordered_set<String> selected_columns;
};

}

std::vector<String> collectSelectedColumnsFromTable(QueryTreeNodePtr & query_tree, const StorageID & storage_id, const ContextPtr & context)
{
    CollectSelectedColumnsFromTableVisitor visitor(storage_id, nullptr, context);
    visitor.visit(query_tree);
    return visitor.getSelectedColumns();
}

std::vector<String> collectSelectedColumnsForTableNode(QueryTreeNodePtr & query_tree, const TableNode & table_node, const ContextPtr & context)
{
    return collectSelectedColumnsForTableExpression(query_tree, table_node, table_node.getStorageID(), context);
}

std::vector<String> collectSelectedColumnsForTableExpression(
    QueryTreeNodePtr & query_tree, const IQueryTreeNode & table_expression, const StorageID & storage_id, const ContextPtr & context)
{
    CollectSelectedColumnsFromTableVisitor visitor(storage_id, &table_expression, context);
    visitor.visit(query_tree);
    return visitor.getSelectedColumns();
}

}
