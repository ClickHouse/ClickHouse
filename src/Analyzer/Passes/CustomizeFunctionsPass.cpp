#include <Analyzer/Passes/CustomizeFunctionsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>

namespace DB
{

namespace
{

class CustomizeFunctionsVisitor : public InDepthQueryTreeVisitorWithContext<CustomizeFunctionsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<CustomizeFunctionsVisitor>;
    using Base::Base;

    void visitImpl(QueryTreeNodePtr & node) const
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const auto & settings = getSettings();

        /// After successful function replacement function name and function name lowercase must be recalculated
        auto function_name = function_node->getFunctionName();
        auto function_name_lowercase = Poco::toLower(function_name);

        if (settings.transform_null_in)
        {
            auto function_result_type = function_node->getResultType();

            static constexpr std::array<std::pair<std::string_view, std::string_view>, 4> in_function_to_replace_null_in_function_map =
            {{
                {"in", "nullIn"},
                {"notin", "notNullIn"},
                {"globalin", "globalNullIn"},
                {"globalnotin", "globalNotNullIn"},
            }};

            for (const auto & [in_function_name, in_function_name_to_replace] : in_function_to_replace_null_in_function_map)
            {
                if (function_name_lowercase == in_function_name)
                {
                    resolveOrdinaryFunctionNode(*function_node, String(in_function_name_to_replace));
                    function_name = function_node->getFunctionName();
                    function_name_lowercase = Poco::toLower(function_name);
                    break;
                }
            }
        }
    }

    inline void resolveOrdinaryFunctionNode(FunctionNode & function_node, const String & function_name) const
    {
        auto function = FunctionFactory::instance().get(function_name, getContext());
        function_node.resolveAsFunction(function->build(function_node.getArgumentColumns()));
    }
};

}

void CustomizeFunctionsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    CustomizeFunctionsVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
