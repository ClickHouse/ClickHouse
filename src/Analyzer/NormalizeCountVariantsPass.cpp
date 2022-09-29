#include <Analyzer/NormalizeCountVariantsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>

namespace DB
{

namespace
{

class NormalizeCountVariantsMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<NormalizeCountVariantsMatcher, true>;

    struct Data
    {
    };

    static void visit(QueryTreeNodePtr & node, Data &)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction() || (function_node->getFunctionName() != "count" && function_node->getFunctionName() != "sum"))
            return;

        if (function_node->getArguments().getNodes().size() != 1)
            return;

        auto & first_argument = function_node->getArguments().getNodes()[0];
        auto first_argument_constant_value = first_argument->getConstantValueOrNull();
        if (!first_argument_constant_value)
            return;

        const auto & first_argument_constant_literal = first_argument_constant_value->getValue();

        if (function_node->getFunctionName() == "count" && !first_argument_constant_literal.isNull())
        {
            function_node->getArguments().getNodes().clear();
        }
        else if (function_node->getFunctionName() == "sum" && first_argument_constant_literal.getType() == Field::Types::UInt64 &&
            first_argument_constant_literal.get<UInt64>() == 1)
        {
            auto result_type = function_node->getResultType();
            AggregateFunctionProperties properties;
            auto aggregate_function = AggregateFunctionFactory::instance().get("count", {}, {}, properties);
            function_node->resolveAsAggregateFunction(std::move(aggregate_function), std::move(result_type));
            function_node->getArguments().getNodes().clear();
        }
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr &)
    {
        return true;
    }
};

}

void NormalizeCountVariantsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr)
{
    NormalizeCountVariantsMatcher::Data data{};
    NormalizeCountVariantsMatcher::Visitor visitor(data);
    visitor.visit(query_tree_node);
}

}
