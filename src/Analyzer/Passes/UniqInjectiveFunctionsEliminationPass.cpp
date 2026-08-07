#include <Analyzer/Passes/UniqInjectiveFunctionsEliminationPass.h>

#include <Functions/IFunction.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>


namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_injective_functions_inside_uniq;
}

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
        if (!getSettings()[Setting::optimize_injective_functions_inside_uniq])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction() || !isUniqFunction(function_node->getFunctionName()))
            return;

        bool replaced_argument = false;
        auto replaced_uniq_function_arguments_nodes = function_node->getArguments().getNodes();

        /// Replace injective function with its single argument
        auto remove_injective_function = [&replaced_argument](QueryTreeNodePtr & arg) -> bool
        {
            auto * arg_typed = arg->as<FunctionNode>();
            if (!arg_typed || !arg_typed->isOrdinaryFunction())
                return false;

            /// Do not apply optimization if injective function contains multiple arguments
            auto & arg_arguments_nodes = arg_typed->getArguments().getNodes();
            if (arg_arguments_nodes.size() != 1)
                return false;

            const auto & arg_function = arg_typed->getFunction();
            if (!arg_function->isInjective({}))
                return false;

            arg = arg_arguments_nodes[0];
            return replaced_argument = true;
        };

        for (auto & uniq_function_argument_node : replaced_uniq_function_arguments_nodes)
        {
            while (remove_injective_function(uniq_function_argument_node))
                ;
        }

        if (!replaced_argument)
            return;

        DataTypes replaced_argument_types;
        replaced_argument_types.reserve(replaced_uniq_function_arguments_nodes.size());

        for (const auto & function_node_argument : replaced_uniq_function_arguments_nodes)
            replaced_argument_types.emplace_back(function_node_argument->getResultType());

        auto current_aggregate_function = function_node->getAggregateFunction();
        AggregateFunctionProperties properties;
        auto replaced_aggregate_function = AggregateFunctionFactory::instance().get(
            function_node->getFunctionName(),
            NullsAction::EMPTY,
            replaced_argument_types,
            current_aggregate_function->getParameters(),
            properties);

        /// uniqCombined returns nullable with nullable arguments so the result type might change which breaks the pass
        if (!replaced_aggregate_function->getResultType()->equals(*current_aggregate_function->getResultType()))
            return;

        function_node->getArguments().getNodes() = std::move(replaced_uniq_function_arguments_nodes);
        function_node->resolveAsAggregateFunction(std::move(replaced_aggregate_function));
    }
};

}

void UniqInjectiveFunctionsEliminationPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    UniqInjectiveFunctionsEliminationVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
