#include <Analyzer/Passes/OptimizeRedundantFunctionsInOrderByPass.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/SortNode.h>
#include <Functions/IFunction.h>

namespace DB
{

namespace
{

class OptimizeRedundantFunctionsInOrderByVisitor : public InDepthQueryTreeVisitor<OptimizeRedundantFunctionsInOrderByVisitor>
{

    struct RedundancyVerdict
    {
        bool redundant = true;
        bool done = false;
    };

    static constexpr RedundancyVerdict makeNonRedundant() noexcept { return { .redundant = false, .done = true }; }

    std::unordered_set<String> existing_keys;

    RedundancyVerdict isRedundantExpression(FunctionNode * function)
    {
        if (function->getArguments().getNodes().empty())
            return makeNonRedundant();

        if (function->getFunction()->isDeterministicInScopeOfQuery())
            return makeNonRedundant();

        // TODO: handle constants here
        for (auto & arg : function->getArguments().getNodes())
        {
            switch (arg->getNodeType())
            {
                case QueryTreeNodeType::FUNCTION:
                {
                    auto subresult = isRedundantExpression(arg->as<FunctionNode>());
                    if (subresult.done)
                        return subresult;
                    break;
                }
                case QueryTreeNodeType::COLUMN:
                {
                    auto * column = arg->as<ColumnNode>();
                    if (!existing_keys.contains(column->getColumnName()))
                        return makeNonRedundant();
                    break;
                }
                default:
                    return makeNonRedundant();
            }
        }

        return {};
    }

public:
    bool needChildVisit(QueryTreeNodePtr & node, QueryTreeNodePtr & /*parent*/)
    {
        if (node->as<FunctionNode>())
            return false;
        return true;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * query = node->as<QueryNode>();
        if (!query)
            return;

        if (!query->hasOrderBy())
            return;

        auto & order_by = query->getOrderBy();
        for (auto & elem : order_by.getNodes())
        {
            auto * order_by_elem = elem->as<SortNode>();
            if (order_by_elem->withFill())
                return;
        }

        QueryTreeNodes new_order_by;
        new_order_by.reserve(order_by.getNodes().size());

        for (auto & elem : order_by.getNodes())
        {
            auto * order_by_elem = elem->as<SortNode>();
            if (auto * expr = order_by_elem->getExpression()->as<FunctionNode>())
            {
                if (isRedundantExpression(expr).redundant)
                    continue;
            }
            else if (auto * column = elem->as<ColumnNode>())
            {
                existing_keys.insert(column->getColumnName());
            }

            new_order_by.push_back(elem);
        }
        existing_keys.clear();

        if (new_order_by.size() < order_by.getNodes().size())
            order_by.getNodes() = std::move(new_order_by);
    }
};

}

void OptimizeRedundantFunctionsInOrderByPass::run(QueryTreeNodePtr query_tree_node, ContextPtr /*context*/)
{
    OptimizeRedundantFunctionsInOrderByVisitor().visit(query_tree_node);
}

}
