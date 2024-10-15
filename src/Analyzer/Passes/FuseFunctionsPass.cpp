#include <Analyzer/Passes/FuseFunctionsPass.h>

#include <Common/iota.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

#include <Functions/FunctionFactory.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>

#include <numeric>


namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_syntax_fuse_functions;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

class FuseFunctionsVisitor : public InDepthQueryTreeVisitorWithContext<FuseFunctionsVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<FuseFunctionsVisitor>;
    using Base::Base;

    explicit FuseFunctionsVisitor(const std::unordered_set<String> names_to_collect_, ContextPtr context)
        : Base(std::move(context))
        , names_to_collect(names_to_collect_)
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_syntax_fuse_functions])
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction() || !names_to_collect.contains(function_node->getFunctionName()))
            return;

        if (function_node->getResultType()->isNullable())
            /// Do not apply to functions with Nullable result type, because `sumCount` handles it different from `sum` and `avg`.
            return;

        const auto & argument_nodes = function_node->getArguments().getNodes();
        if (argument_nodes.size() != 1)
            /// Do not apply for `count()` with without arguments or `count(*)`, only `count(x)` is supported.
            return;

        argument_to_functions_mapping[argument_nodes[0]].insert(&node);
    }

    /// argument -> list of sum/count/avg functions with this argument
    QueryTreeNodePtrWithHashMap<std::unordered_set<QueryTreeNodePtr *>> argument_to_functions_mapping;

private:
    std::unordered_set<String> names_to_collect;
};

QueryTreeNodePtr createResolvedFunction(const ContextPtr & context, const String & name, QueryTreeNodes arguments)
{
    auto function_node = std::make_shared<FunctionNode>(name);

    auto function = FunctionFactory::instance().get(name, context);
    function_node->getArguments().getNodes() = std::move(arguments);
    function_node->resolveAsFunction(function->build(function_node->getArgumentColumns()));
    return function_node;
}

FunctionNodePtr createResolvedAggregateFunction(
    const String & name, const QueryTreeNodePtr & argument, const Array & parameters = {}, NullsAction action = NullsAction::EMPTY)
{
    auto function_node = std::make_shared<FunctionNode>(name);
    function_node->setNullsAction(action);

    if (!parameters.empty())
    {
        QueryTreeNodes parameter_nodes;
        for (const auto & param : parameters)
            parameter_nodes.emplace_back(std::make_shared<ConstantNode>(param));
        function_node->getParameters().getNodes() = std::move(parameter_nodes);
    }
    function_node->getArguments().getNodes() = { argument };

    AggregateFunctionProperties properties;
    auto aggregate_function = AggregateFunctionFactory::instance().get(name, action, {argument->getResultType()}, parameters, properties);
    function_node->resolveAsAggregateFunction(std::move(aggregate_function));

    return function_node;
}

QueryTreeNodePtr createTupleElementFunction(const ContextPtr & context, QueryTreeNodePtr argument, UInt64 index)
{
    return createResolvedFunction(context, "tupleElement", {argument, std::make_shared<ConstantNode>(index)});
}

QueryTreeNodePtr createArrayElementFunction(const ContextPtr & context, QueryTreeNodePtr argument, UInt64 index)
{
    return createResolvedFunction(context, "arrayElement", {argument, std::make_shared<ConstantNode>(index)});
}

void replaceWithSumCount(QueryTreeNodePtr & node, const FunctionNodePtr & sum_count_node, ContextPtr context)
{
    auto sum_count_result_type = std::dynamic_pointer_cast<const DataTypeTuple>(sum_count_node->getResultType());
    if (!sum_count_result_type || sum_count_result_type->getElements().size() != 2)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected return type '{}' of function '{}', should be tuple of two elements",
            sum_count_node->getResultType(), sum_count_node->getFunctionName());
    }

    String function_name = node->as<const FunctionNode &>().getFunctionName();

    if (function_name == "sum")
    {
        assert(node->getResultType()->equals(*sum_count_result_type->getElement(0)));
        node = createTupleElementFunction(context, sum_count_node, 1);
    }
    else if (function_name == "count")
    {
        assert(node->getResultType()->equals(*sum_count_result_type->getElement(1)));
        node = createTupleElementFunction(context, sum_count_node, 2);
    }
    else if (function_name == "avg")
    {
        auto sum_result = createTupleElementFunction(context, sum_count_node, 1);
        auto count_result = createTupleElementFunction(context, sum_count_node, 2);
        /// To avoid integer division by zero
        auto count_float_result = createResolvedFunction(context, "toFloat64", {count_result});
        node = createResolvedFunction(context, "divide", {sum_result, count_float_result});
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported function '{}'", function_name);
    }
}

/// Reorder nodes according to the value of the quantile level parameter.
/// Levels are sorted in ascending order to make pass result deterministic.
FunctionNodePtr createFusedQuantilesNode(std::vector<QueryTreeNodePtr *> & nodes, const QueryTreeNodePtr & argument)
{
    Array parameters;
    parameters.reserve(nodes.size());

    for (const auto * node : nodes)
    {
        const FunctionNode & function_node = (*node)->as<const FunctionNode &>();
        const auto & function_name = function_node.getFunctionName();

        const auto & parameter_nodes = function_node.getParameters().getNodes();
        if (parameter_nodes.empty())
        {
            parameters.push_back(Float64(0.5)); /// default value
            continue;
        }

        if (parameter_nodes.size() != 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' should have exactly one parameter", function_name);

        const auto * constant_node = parameter_nodes.front()->as<ConstantNode>();
        if (!constant_node)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' should have constant parameter", function_name);

        const auto & value = constant_node->getValue();
        if (value.getType() != Field::Types::Float64)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Function '{}' should have parameter of type Float64, got '{}'",
                function_name, value.getTypeName());

        parameters.push_back(value);
    }

    {
        /// Sort nodes and parameters in ascending order of quantile level
        std::vector<size_t> permutation(nodes.size());
        iota(permutation.data(), permutation.size(), size_t(0));
        std::sort(permutation.begin(), permutation.end(), [&](size_t i, size_t j) { return parameters[i].safeGet<Float64>() < parameters[j].safeGet<Float64>(); });

        std::vector<QueryTreeNodePtr *> new_nodes;
        new_nodes.reserve(permutation.size());

        Array new_parameters;
        new_parameters.reserve(permutation.size());

        for (size_t i : permutation)
        {
            new_nodes.emplace_back(nodes[i]);
            new_parameters.emplace_back(std::move(parameters[i]));
        }
        nodes = std::move(new_nodes);
        parameters = std::move(new_parameters);
    }

    return createResolvedAggregateFunction("quantiles", argument, parameters);
}


void tryFuseSumCountAvg(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    FuseFunctionsVisitor visitor({"sum", "count", "avg"}, context);
    visitor.visit(query_tree_node);

    for (auto & [argument, nodes] : visitor.argument_to_functions_mapping)
    {
        if (nodes.size() < 2)
            continue;

        auto sum_count_node = createResolvedAggregateFunction("sumCount", argument.node);
        for (auto * node : nodes)
        {
            assert(node);
            replaceWithSumCount(*node, sum_count_node, context);
        }
    }
}

void tryFuseQuantiles(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    FuseFunctionsVisitor visitor_quantile({"quantile"}, context);
    visitor_quantile.visit(query_tree_node);

    for (auto & [argument, nodes_set] : visitor_quantile.argument_to_functions_mapping)
    {
        size_t nodes_size = nodes_set.size();
        if (nodes_size < 2)
            continue;

        std::vector<QueryTreeNodePtr *> nodes(nodes_set.begin(), nodes_set.end());

        auto quantiles_node = createFusedQuantilesNode(nodes, argument.node);
        auto result_array_type = std::dynamic_pointer_cast<const DataTypeArray>(quantiles_node->getResultType());
        if (!result_array_type)
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unexpected return type '{}' of function '{}', should be array",
                quantiles_node->getResultType(), quantiles_node->getFunctionName());

        for (size_t i = 0; i < nodes_set.size(); ++i)
        {
            size_t array_index = i + 1;
            *nodes[i] = createArrayElementFunction(context, quantiles_node, array_index);
        }
    }
}

}

void FuseFunctionsPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    tryFuseSumCountAvg(query_tree_node, context);
    tryFuseQuantiles(query_tree_node, context);
}

}
