#include <Analyzer/Passes/UniqInjectiveFunctionsEliminationPass.h>

#include <Functions/IFunction.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

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

class UniqInjectiveFunctionsEliminationVisitor : public InDepthQueryTreeVisitorWithContext<UniqInjectiveFunctionsEliminationVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<UniqInjectiveFunctionsEliminationVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_injective_functions_inside_uniq)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction() || !isUniqFunction(function_node->getFunctionName()))
            return;

        bool replaced_argument = false;
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
            replaced_argument = true;
        }

        if (!replaced_argument)
            return;

        const auto & function_node_argument_nodes = function_node->getArguments().getNodes();

        DataTypes argument_types;
        argument_types.reserve(function_node_argument_nodes.size());

        for (const auto & function_node_argument : function_node_argument_nodes)
            argument_types.emplace_back(function_node_argument->getResultType());

        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get(
            function_node->getFunctionName(),
            NullsAction::EMPTY,
            argument_types,
            function_node->getAggregateFunction()->getParameters(),
            properties);

        function_node->resolveAsAggregateFunction(std::move(aggregate_function));
    }
};

}

void UniqInjectiveFunctionsEliminationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    UniqInjectiveFunctionsEliminationVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
