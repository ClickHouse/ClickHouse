#include <Planner/collectSelectedColumnsFromTable.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/TableNode.h>
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
        auto * column_node = node->as<ColumnNode>();
        if (!column_node)
            return;

        if (column_node->getColumnName() == "__grouping_set")
            return;

        const auto * source_table = column_node->getColumnSource()->as<TableNode>();
        if (!source_table || source_table->getStorageID() != storage_id)
            return;

        /// Note that arguments of the "indexHint" function need to be checked for SELECT privilege
        selected_columns.insert(column_node->getColumnName());
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

    std::vector<String> getSelectedColumns() const
    {
        return std::vector<String>(selected_columns.begin(), selected_columns.end());
    }

private:
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
