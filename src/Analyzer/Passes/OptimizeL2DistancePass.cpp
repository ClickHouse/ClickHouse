#include <Analyzer/Passes/OptimizeL2DistancePass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

class L2DistanceOptimizationPassVisitor : public InDepthQueryTreeVisitorWithContext<L2DistanceOptimizationPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<L2DistanceOptimizationPassVisitor>;
    using Base::Base;

    explicit L2DistanceOptimizationPassVisitor(ContextPtr context)
        : Base(std::move(context))
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (auto * func_node = typeid_cast<ASTFunction *>(node.get()))
        {
            if (func_node->name == "L2Distance")
            {
                if (func_node->arguments->children.size() != 2)
                return;

                String vec_arg = func_node->arguments->children.at(1)->getColumnName();

                auto new_function_name = "sqrt";
                auto new_function_arg = std::make_shared<ASTFunction>();
                new_function_arg->name = "L2SquaredDistance";
                new_function_arg->arguments = func_node->arguments;

                auto new_function_node = std::make_shared<ASTFunction>();
                new_function_node->name = new_function_name;
                new_function_node->arguments = std::make_shared<ASTExpressionList>();
                new_function_node->arguments->children.push_back(new_function_arg);

                node = new_function_node;
            }
        }
    }
};

}

void L2DistanceOptimizationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    L2DistanceOptimizationPassVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
