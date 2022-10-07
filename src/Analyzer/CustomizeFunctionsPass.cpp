#include <Analyzer/CustomizeFunctionsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>

namespace DB
{

namespace
{

class CustomizeFunctionsVisitor : public InDepthQueryTreeVisitor<CustomizeFunctionsVisitor>
{
public:
    explicit CustomizeFunctionsVisitor(ContextPtr & context_)
        : context(context_)
    {}

    void visitImpl(QueryTreeNodePtr & node) const
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        const auto & settings = context->getSettingsRef();

        /// After successful function replacement function name and function name lowercase must be recalculated
        auto function_name = function_node->getFunctionName();
        auto function_name_lowercase = Poco::toLower(function_name);

        if (function_node->isAggregateFunction() || function_node->isWindowFunction())
        {
            auto count_distinct_implementation_function_name = String(settings.count_distinct_implementation);

            /// Replace countDistinct with countDistinct implementation
            if (function_name_lowercase == "countdistinct")
            {
                resolveAggregateOrWindowFunctionNode(*function_node, count_distinct_implementation_function_name);
                function_name = function_node->getFunctionName();
                function_name_lowercase = Poco::toLower(function_name);
            }

            /// Replace countDistinct with countIfDistinct with countDistinctIf implementation
            if (function_name_lowercase == "countifdistinct")
            {
                resolveAggregateOrWindowFunctionNode(*function_node, count_distinct_implementation_function_name + "If");
                function_name = function_node->getFunctionName();
                function_name_lowercase = Poco::toLower(function_name);
            }

            /// Swap aggregateFunctionIfDistinct into aggregateFunctionDistinctIf to make execution more optimal
            if (function_name_lowercase.ends_with("ifdistinct"))
            {
                size_t prefix_length = function_name_lowercase.size() - strlen("ifdistinct");
                auto updated_function_name = function_name_lowercase.substr(0, prefix_length) + "DistinctIf";
                resolveAggregateOrWindowFunctionNode(*function_node, updated_function_name);
                function_name = function_node->getFunctionName();
                function_name_lowercase = Poco::toLower(function_name);
            }

            /// Rewrite all aggregate functions to add -OrNull suffix to them
            if (settings.aggregate_functions_null_for_empty && !function_name.ends_with("OrNull"))
            {
                auto function_properies = AggregateFunctionFactory::instance().tryGetProperties(function_name);
                if (function_properies && !function_properies->returns_default_when_only_null)
                {
                    auto updated_function_name = function_name + "OrNull";
                    resolveAggregateOrWindowFunctionNode(*function_node, updated_function_name);
                    function_name = function_node->getFunctionName();
                    function_name_lowercase = Poco::toLower(function_name);
                }
            }

            /** Move -OrNull suffix ahead, this should execute after add -OrNull suffix.
              * Used to rewrite aggregate functions with -OrNull suffix in some cases.
              * Example: sumIfOrNull.
              * Result: sumOrNullIf.
              */
            if (function_name.ends_with("OrNull"))
            {
                auto function_properies = AggregateFunctionFactory::instance().tryGetProperties(function_name);
                if (function_properies && !function_properies->returns_default_when_only_null)
                {
                    size_t function_name_size = function_name.size();

                    static constexpr std::array<std::string_view, 4> suffixes_to_replace = {"MergeState", "Merge", "State", "If"};
                    for (const auto & suffix : suffixes_to_replace)
                    {
                        if (!function_name_lowercase.ends_with(suffix))
                            continue;

                        auto updated_function_name = function_name_lowercase.substr(0, function_name_size - suffix.size()) + "OrNull" + String(suffix);
                        resolveAggregateOrWindowFunctionNode(*function_node, updated_function_name);
                        function_name = function_node->getFunctionName();
                        function_name_lowercase = Poco::toLower(function_name);
                        break;
                    }
                }
            }

            return;
        }

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

    static inline void resolveAggregateOrWindowFunctionNode(FunctionNode & function_node, const String & aggregate_function_name)
    {
        auto function_result_type = function_node.getResultType();
        auto function_aggregate_function = function_node.getAggregateFunction();

        AggregateFunctionProperties properties;
        auto aggregate_function = AggregateFunctionFactory::instance().get(aggregate_function_name,
            function_aggregate_function->getArgumentTypes(),
            function_aggregate_function->getParameters(),
            properties);

        if (function_node.isAggregateFunction())
            function_node.resolveAsAggregateFunction(std::move(aggregate_function), std::move(function_result_type));
        else if (function_node.isWindowFunction())
            function_node.resolveAsWindowFunction(std::move(aggregate_function), std::move(function_result_type));
    }

    inline void resolveOrdinaryFunctionNode(FunctionNode & function_node, const String & function_name) const
    {
        auto function_result_type = function_node.getResultType();
        auto function = FunctionFactory::instance().get(function_name, context);
        function_node.resolveAsFunction(function, std::move(function_result_type));
    }

private:
    ContextPtr & context;
};

}

void CustomizeFunctionsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    CustomizeFunctionsVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
