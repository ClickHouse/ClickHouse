#include <Analyzer/Passes/FuseFunctionsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
}

namespace
{

class FuseFunctionsVisitor : public InDepthQueryTreeVisitor<FuseFunctionsVisitor>
{
public:

    explicit FuseFunctionsVisitor(const std::unordered_set<String> names_to_collect_)
        : names_to_collect(names_to_collect_)
    {}

    void visitImpl(QueryTreeNodePtr & node)
    {
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

        mapping[QueryTreeNodeWithHash(argument_nodes[0])].push_back(&node);
    }

    struct QueryTreeNodeWithHash
    {
        const QueryTreeNodePtr & node;
        IQueryTreeNode::Hash hash;

        explicit QueryTreeNodeWithHash(const QueryTreeNodePtr & node_)
            : node(node_)
            , hash(node->getTreeHash())
        {}

        bool operator==(const QueryTreeNodeWithHash & rhs) const
        {
            return hash == rhs.hash && node->isEqual(*rhs.node);
        }

        struct Hash
        {
            size_t operator() (const QueryTreeNodeWithHash & key) const { return key.hash.first ^ key.hash.second; }
        };
    };

    /// argument -> list of sum/count/avg functions with this argument
    std::unordered_map<QueryTreeNodeWithHash, std::vector<QueryTreeNodePtr *>, QueryTreeNodeWithHash::Hash> mapping;

private:
    std::unordered_set<String> names_to_collect;
};

QueryTreeNodePtr createResolvedFunction(ContextPtr context, const String & name, DataTypePtr result_type, QueryTreeNodes arguments)
{
    auto function_node = std::make_shared<FunctionNode>(name);
    auto function = FunctionFactory::instance().get(name, context);
    function_node->resolveAsFunction(std::move(function), result_type);
    function_node->getArguments().getNodes() = std::move(arguments);
    return function_node;
}

FunctionNodePtr createResolvedAggregateFunction(const String & name, const QueryTreeNodePtr & argument, const Array & parameters = {})
{
    auto function_node = std::make_shared<FunctionNode>(name);

    AggregateFunctionProperties properties;
    auto aggregate_function = AggregateFunctionFactory::instance().get(name, {argument->getResultType()}, parameters, properties);

    function_node->resolveAsAggregateFunction(aggregate_function, aggregate_function->getReturnType());

    function_node->getArgumentsNode() = std::make_shared<ListNode>(QueryTreeNodes{argument});
    return function_node;
}

QueryTreeNodePtr createTupleElementFunction(ContextPtr context, DataTypePtr result_type, QueryTreeNodePtr argument, UInt64 index)
{
    return createResolvedFunction(context, "tupleElement", result_type, {argument, std::make_shared<ConstantNode>(index)});
}

QueryTreeNodePtr createArrayElementFunction(ContextPtr context, DataTypePtr result_type, QueryTreeNodePtr argument, UInt64 index)
{
    return createResolvedFunction(context, "arrayElement", result_type, {argument, std::make_shared<ConstantNode>(index)});
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
        node = createTupleElementFunction(context, node->getResultType(), sum_count_node, 1);
    }
    else if (function_name == "count")
    {
        assert(node->getResultType()->equals(*sum_count_result_type->getElement(1)));
        node = createTupleElementFunction(context, node->getResultType(), sum_count_node, 2);
    }
    else if (function_name == "avg")
    {
        auto sum_result = createTupleElementFunction(context, sum_count_result_type->getElement(0), sum_count_node, 1);
        auto count_result = createTupleElementFunction(context, sum_count_result_type->getElement(1), sum_count_node, 2);
        /// To avoid integer division by zero
        auto count_float_result = createResolvedFunction(context, "toFloat64", std::make_shared<DataTypeFloat64>(), {count_result});
        node = createResolvedFunction(context, "divide", node->getResultType(), {sum_result, count_float_result});
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported function '{}'", function_name);
    }
}

FunctionNodePtr createFusedQuantilesNode(const std::vector<QueryTreeNodePtr *> nodes, const QueryTreeNodePtr & argument)
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

        const auto & constant_value = parameter_nodes.front()->getConstantValueOrNull();
        if (!constant_value)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Function '{}' should have constant parameter", function_name);

        parameters.push_back(constant_value->getValue());
    }
    return createResolvedAggregateFunction("quantiles", argument, parameters);
}


void tryFuseSumCountAvg(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    FuseFunctionsVisitor visitor({"sum", "count", "avg"});
    visitor.visit(query_tree_node);

    for (auto & [argument, nodes] : visitor.mapping)
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
    FuseFunctionsVisitor visitor_quantile({"quantile"});
    visitor_quantile.visit(query_tree_node);
    for (auto & [argument, nodes] : visitor_quantile.mapping)
    {
        if (nodes.size() < 2)
            continue;

        auto quantiles_node = createFusedQuantilesNode(nodes, argument.node);
        auto result_array_type = std::dynamic_pointer_cast<const DataTypeArray>(quantiles_node->getResultType());
        if (!result_array_type)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR,
                "Unexpected return type '{}' of function '{}', should be array",
                quantiles_node->getResultType(), quantiles_node->getFunctionName());
        }

        for (size_t i = 0; i < nodes.size(); ++i)
        {
            *nodes[i] = createArrayElementFunction(context, result_array_type->getNestedType(), quantiles_node, i + 1);
        }
    }
}

}

void FuseFunctionsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    tryFuseSumCountAvg(query_tree_node, context);
    tryFuseQuantiles(query_tree_node, context);
}

}
