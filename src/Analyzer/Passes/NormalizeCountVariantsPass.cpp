#include <Analyzer/Passes/NormalizeCountVariantsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Interpreters/Context.h>
#include <DataTypes/DataTypesNumber.h>

namespace DB
{

namespace
{

class NormalizeCountVariantsVisitor : public InDepthQueryTreeVisitorWithContext<NormalizeCountVariantsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<NormalizeCountVariantsVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings().optimize_normalize_count_variants)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction() || (function_node->getFunctionName() != "count" && function_node->getFunctionName() != "sum"))
            return;

        if (function_node->getArguments().getNodes().size() != 1)
            return;

        /// forbid the optimization if return value of sum() and count() differs:
        /// count() returns only UInt64 type, while sum() could return Nullable().
        if (!function_node->getResultType()->equals(DataTypeUInt64()))
            return;

        auto & first_argument = function_node->getArguments().getNodes()[0];
        auto * first_argument_constant_node = first_argument->as<ConstantNode>();
        if (!first_argument_constant_node)
            return;

        const auto & first_argument_constant_literal = first_argument_constant_node->getValue();

        if (function_node->getFunctionName() == "count" && !first_argument_constant_literal.isNull())
        {
            resolveAsCountAggregateFunction(*function_node);
            function_node->getArguments().getNodes().clear();
        }
        else if (function_node->getFunctionName() == "sum" &&
            first_argument_constant_literal.getType() == Field::Types::UInt64 &&
            first_argument_constant_literal.get<UInt64>() == 1)
        {
            resolveAsCountAggregateFunction(*function_node);
            function_node->getArguments().getNodes().clear();
        }
    }
private:
    static void resolveAsCountAggregateFunction(FunctionNode & function_node)
    {
        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get("count", NullsAction::EMPTY, {}, {}, properties);

        function_node.resolveAsAggregateFunction(std::move(aggregate_function));
    }
};

}

void NormalizeCountVariantsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    NormalizeCountVariantsVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
