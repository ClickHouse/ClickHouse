#include <Analyzer/Passes/OptimizeL2DistancePass.h>
#include <Parsers/ExpressionListParsers.h>

namespace DB
{

void L2DistanceOptimizationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    Data data;
    query_tree_node->visit(data, [this](ASTPtr & node, ASTPtr & parent, Data & data) { enterImpl(node, parent, data); }, data);
}

void L2DistanceOptimizationPass::enterImpl(ASTPtr & node, ASTPtr & parent, Data & data)
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

}
