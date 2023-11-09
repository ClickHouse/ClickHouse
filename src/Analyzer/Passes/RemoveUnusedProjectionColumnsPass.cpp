#include <Analyzer/Passes/RemoveUnusedProjectionColumnsPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/SortNode.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

class CollectUsedColumnsVisitor : public InDepthQueryTreeVisitor<CollectUsedColumnsVisitor>
{
public:
    bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr & child)
    {
        auto node_type = child->getNodeType();
        if (node_type == QueryTreeNodeType::QUERY || node_type == QueryTreeNodeType::UNION)
        {
            subqueries_nodes_to_visit.insert(child);
            return false;
        }

        return true;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto node_type = node->getNodeType();
        if (node_type != QueryTreeNodeType::COLUMN)
            return;

        auto & column_node = node->as<ColumnNode &>();
        auto column_source_node = column_node.getColumnSource();
        auto column_source_node_type = column_source_node->getNodeType();

        if (column_source_node_type == QueryTreeNodeType::QUERY || column_source_node_type == QueryTreeNodeType::UNION)
        {
            auto * column_source_node_ptr = column_source_node.get();
            query_or_union_node_to_used_columns[column_source_node_ptr].insert(column_node.getColumnName());
        }
    }

    void reset()
    {
        subqueries_nodes_to_visit.clear();
        query_or_union_node_to_used_columns.clear();
    }

    std::unordered_set<QueryTreeNodePtr> subqueries_nodes_to_visit;
    std::unordered_map<IQueryTreeNode *, std::unordered_set<std::string>> query_or_union_node_to_used_columns;
};

}

void RemoveUnusedProjectionColumnsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr)
{
    std::vector<QueryTreeNodePtr> nodes_to_visit;
    nodes_to_visit.push_back(query_tree_node);

    CollectUsedColumnsVisitor visitor;

    while (!nodes_to_visit.empty())
    {
        auto node_to_visit = std::move(nodes_to_visit.back());
        nodes_to_visit.pop_back();

        visitor.visit(node_to_visit);

        for (auto & [query_or_union_node, used_columns] : visitor.query_or_union_node_to_used_columns)
        {
            if (auto * union_node = query_or_union_node->as<UnionNode>())
                union_node->removeUnusedProjectionColumns(used_columns);
            else if (auto * query_node = query_or_union_node->as<QueryNode>())
                query_node->removeUnusedProjectionColumns(used_columns);
        }

        for (const auto & subquery_node_to_visit : visitor.subqueries_nodes_to_visit)
            nodes_to_visit.push_back(subquery_node_to_visit);

        visitor.reset();
    }
}

}
