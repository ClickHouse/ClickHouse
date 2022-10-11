#include <Analyzer/Passes/OrderByLimitByDuplicateEliminationPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>

namespace DB
{

namespace
{

struct QueryTreeNodeHash
{
    size_t operator()(const IQueryTreeNode * node) const
    {
        return node->getTreeHash().first;
    }
};

struct QueryTreeNodeEqualTo
{
    size_t operator()(const IQueryTreeNode * lhs_node, const IQueryTreeNode * rhs_node) const
    {
        return lhs_node->isEqual(*rhs_node);
    }
};

using QueryTreeNodeSet = std::unordered_set<const IQueryTreeNode *, QueryTreeNodeHash, QueryTreeNodeEqualTo>;

class OrderByLimitByDuplicateEliminationVisitor : public InDepthQueryTreeVisitor<OrderByLimitByDuplicateEliminationVisitor>
{
public:
    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        if (query_node->hasOrderBy())
        {
            QueryTreeNodes result_nodes;

            auto & query_order_by_nodes = query_node->getOrderBy().getNodes();

            for (auto & sort_node : query_order_by_nodes)
            {
                auto & sort_node_typed = sort_node->as<SortNode &>();

                /// Skip elements with WITH FILL
                if (sort_node_typed.withFill())
                {
                    result_nodes.push_back(sort_node);
                    continue;
                }

                auto [_, inserted] = unique_expressions_nodes_set.emplace(sort_node_typed.getExpression().get());
                if (inserted)
                    result_nodes.push_back(sort_node);
            }

            query_order_by_nodes = std::move(result_nodes);
        }

        unique_expressions_nodes_set.clear();

        if (query_node->hasLimitBy())
        {
            QueryTreeNodes result_nodes;

            auto & query_limit_by_nodes = query_node->getLimitBy().getNodes();

            for (auto & limit_by_node : query_node->getLimitBy().getNodes())
            {
                auto [_, inserted] = unique_expressions_nodes_set.emplace(limit_by_node.get());
                if (inserted)
                    result_nodes.push_back(limit_by_node);
            }

            query_limit_by_nodes = std::move(result_nodes);
        }

        unique_expressions_nodes_set.clear();
    }

private:
    QueryTreeNodeSet unique_expressions_nodes_set;
};

}

void OrderByLimitByDuplicateEliminationPass::run(QueryTreeNodePtr query_tree_node, ContextPtr)
{
    OrderByLimitByDuplicateEliminationVisitor visitor;
    visitor.visit(query_tree_node);
}

}

