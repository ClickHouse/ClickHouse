#include <Analyzer/Passes/UniqInjectiveFunctionsEliminationPass.h>

#include <Functions/IFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>

namespace DB
{

namespace
{

bool isUniqFunction(const String & function_name)
{
    return function_name == "uniq" ||
        function_name == "uniqExact" ||
        function_name == "uniqHLL12" ||
        function_name == "uniqCombined" ||
        function_name == "uniqCombined64" ||
        function_name == "uniqTheta";
}

class UniqInjectiveFunctionsEliminationVisitor : public InDepthQueryTreeVisitor<UniqInjectiveFunctionsEliminationVisitor>
{
public:
    static void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction() || !isUniqFunction(function_node->getFunctionName()))
            return;

        auto & uniq_function_arguments_nodes = function_node->getArguments().getNodes();
        for (auto & uniq_function_argument_node : uniq_function_arguments_nodes)
        {
            auto * uniq_function_argument_node_typed = uniq_function_argument_node->as<FunctionNode>();
            if (!uniq_function_argument_node_typed || !uniq_function_argument_node_typed->isOrdinaryFunction())
                continue;

            auto & uniq_function_argument_node_argument_nodes = uniq_function_argument_node_typed->getArguments().getNodes();

            /// Do not apply optimization if injective function contains multiple arguments
            if (uniq_function_argument_node_argument_nodes.size() != 1)
                continue;

            const auto & uniq_function_argument_node_function = uniq_function_argument_node_typed->getFunction();
            if (!uniq_function_argument_node_function->isInjective({}))
                continue;

            /// Replace injective function with its single argument
            uniq_function_argument_node = uniq_function_argument_node_argument_nodes[0];
        }
    }
};

}

void UniqInjectiveFunctionsEliminationPass::run(QueryTreeNodePtr query_tree_node, ContextPtr)
{
    UniqInjectiveFunctionsEliminationVisitor visitor;
    visitor.visit(query_tree_node);
}

}
