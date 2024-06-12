#include <memory>
#include <Analyzer/Passes/UniqInjectiveFunctionsEliminationPass.h>

#include <Functions/IFunction.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Analyzer/ListNode.h>


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

        auto new_arguments_nodes = std::make_shared<ListNode>();
        DataTypes new_argument_types;

        auto remove_injective_function = [&replaced_argument](QueryTreeNodePtr & arg) -> bool
        {
            auto * arg_typed = arg->as<FunctionNode>();
            if (!arg_typed || !arg_typed->isOrdinaryFunction())
                return false;

            auto & arg_arguments_nodes = arg_typed->getArguments().getNodes();
            if (arg_arguments_nodes.size() != 1)
                return false;

            const auto & arg_function = arg_typed->getFunction();
            if (!arg_function->isInjective({}))
                return false;

            arg = arg_arguments_nodes[0];
            return replaced_argument = true;
        };

        for (auto uniq_function_argument_node : uniq_function_arguments_nodes)
        {
            while (remove_injective_function(uniq_function_argument_node))
                ;
            new_arguments_nodes->getNodes().push_back(uniq_function_argument_node);
            new_argument_types.push_back(uniq_function_argument_node->getResultType());
        }

        if (!replaced_argument)
            return;

        auto current_aggregate_function = function_node->getAggregateFunction();
        AggregateFunctionProperties properties;
        auto new_aggregate_function = AggregateFunctionFactory::instance().get(
            function_node->getFunctionName(),
            NullsAction::EMPTY,
            new_argument_types,
            current_aggregate_function->getParameters(),
            properties);

        /// Enforce that new aggregate function does not change the result type
        if (current_aggregate_function->getResultType()->equals(*new_aggregate_function->getResultType()))
        {
            function_node->getArgumentsNode() = new_arguments_nodes->clone();
            function_node->resolveAsAggregateFunction(std::move(new_aggregate_function));
        }
    }
};

}

void UniqInjectiveFunctionsEliminationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    UniqInjectiveFunctionsEliminationVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
