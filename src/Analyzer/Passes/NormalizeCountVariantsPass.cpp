#include <Analyzer/Passes/NormalizeCountVariantsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace
{

class NormalizeCountVariantsVisitor : public InDepthQueryTreeVisitor<NormalizeCountVariantsVisitor>
{
public:
    explicit NormalizeCountVariantsVisitor(ContextPtr context_) : context(std::move(context_)) {}
    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction() || (function_node->getFunctionName() != "count" && function_node->getFunctionName() != "sum"))
            return;

        if (function_node->getArguments().getNodes().size() != 1)
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
            first_argument_constant_literal.get<UInt64>() == 1 &&
            !context->getSettingsRef().aggregate_functions_null_for_empty)
        {
            resolveAsCountAggregateFunction(*function_node);
            function_node->getArguments().getNodes().clear();
        }
    }
private:
    ContextPtr context;

    static inline void resolveAsCountAggregateFunction(FunctionNode & function_node)
    {
        auto function_result_type = function_node.getResultType();

        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get("count", {}, {}, properties);

        function_node.resolveAsAggregateFunction(std::move(aggregate_function), std::move(function_result_type));
    }
};

}

void NormalizeCountVariantsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    NormalizeCountVariantsVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
