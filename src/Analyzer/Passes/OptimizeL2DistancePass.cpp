#include "OptimizeL2DistancePass.h"
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTExpressionList.h>

namespace DB
{

namespace
{

class OptimizeL2DistancePassVisitor : public InDepthQueryTreeVisitorWithContext<OptimizeL2DistancePassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<OptimizeL2DistancePassVisitor>;
    using Base::Base;

    explicit OptimizeL2DistancePassVisitor(ContextPtr context) : Base(std::move(context)) {}

    void visit(ASTPtr & ast, void * data) override // Изменен тип Data на void*
    {
        if (auto * select_query = ast->as<ASTSelectQuery>())
        {
            visit(*select_query, data);
            visitChildren(ast, data);
        }
        else
        {
            visitChildren(ast, data);
        }
    }

private:
    void visit(ASTSelectQuery & select_query, void * data) // Изменен тип Data на void*
    {
        if (auto * order_by = select_query.orderBy())
        {
            for (auto & order_elem : order_by->children)
            {
                auto * function_node = order_elem->as<ASTFunction>();
                if (!function_node)
                    continue;

                if (function_node->name == "l2distance")
                {
                    auto sqrt_function_node = std::make_shared<ASTFunction>();
                    sqrt_function_node->name = "sqrt";

                    auto argument = std::make_shared<ASTFunction>();
                    argument->name = "l2squared";
                    argument->arguments = function_node->arguments;

                    sqrt_function_node->arguments = std::make_shared<ASTExpressionList>();
                    sqrt_function_node->arguments->children.push_back(argument);

                    order_elem = sqrt_function_node;
                }
            }
        }
    }
};

}

void OptimizeL2DistancePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    OptimizeL2DistancePassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
