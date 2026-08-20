#include <Planner/collectSelectedColumnsFromTable.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/FunctionNode.h>
#include <Storages/IStorage.h>


namespace DB
{

namespace
{
class CollectSelectedColumnsFromTableVisitor : public InDepthQueryTreeVisitorWithContext<CollectSelectedColumnsFromTableVisitor>
{
public:
    explicit CollectSelectedColumnsFromTableVisitor(const StorageID & storage_id_, const ContextPtr & context)
        : InDepthQueryTreeVisitorWithContext(context), storage_id(storage_id_)
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

        const auto * source_table = column_node->getColumnSource()->as<TableNode>();
        if (!source_table || source_table->getStorageID() != storage_id)
            return;

        /// A special case for the "indexHint" function. We don't need its arguments for execution if column's source table is MergeTree.
        /// Instead, we prepare an ActionsDAG for its arguments and store it inside a function (see ActionsDAG::buildFilterActionsDAG).
        /// So this optimization allows not to read arguments of "indexHint" (if not needed in other contexts) but only to use index analysis for them.
        if (is_inside_index_hint_function && source_table->getStorage()->isMergeTree())
            return;

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
    std::unordered_set<String> selected_columns;
};

}

std::vector<String> collectSelectedColumnsFromTable(QueryTreeNodePtr & query_tree, const StorageID & storage_id, const ContextPtr & context)
{
    CollectSelectedColumnsFromTableVisitor visitor(storage_id, context);
    visitor.visit(query_tree);
    return visitor.getSelectedColumns();
}

}
