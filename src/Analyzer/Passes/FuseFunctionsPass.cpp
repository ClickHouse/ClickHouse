#include <Analyzer/Passes/FuseFunctionsPass.h>

#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Functions/FunctionFactory.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/ConstantNode.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

namespace
{

class FuseFunctionsMatcher : public InDepthQueryTreeVisitor<FuseFunctionsMatcher>
{
public:

    static bool matchFunctionName(const String & name)
    {
        return name == "count" || name == "sum" || name == "avg";
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction() || !matchFunctionName(function_node->getFunctionName()))
            return;

        auto argument_hash = function_node->getArgumentsNode()->getTreeHash();
        mapping[argument_hash].push_back(&node);
    }

    struct QueryTreeHashForMap
    {
        size_t operator()(const IQueryTreeNode::Hash & hash) const { return hash.first ^ hash.second; }
    };

    /// argument -> list of sum/count/avg functions with this argument
    std::unordered_map<IQueryTreeNode::Hash, std::vector<QueryTreeNodePtr *>, QueryTreeHashForMap> mapping;
};

template <typename... Args>
QueryTreeNodePtr createResolvedFunction(ContextPtr context, const String & name, DataTypePtr result_type, Args &&... args)
{
    auto function_node = std::make_shared<FunctionNode>(name);
    auto function = FunctionFactory::instance().get(name, context);
    function_node->resolveAsFunction(function, result_type);
    function_node->getArguments().getNodes() = { std::forward<Args>(args)... };
    return function_node;
}

QueryTreeNodePtr createTupleElement(ContextPtr context, DataTypePtr result_type, QueryTreeNodePtr argument, UInt64 idx)
{
    return createResolvedFunction(context, "tupleElement", result_type, argument, std::make_shared<ConstantNode>(idx));
}

QueryTreeNodePtr createSumCount(const FunctionNode & function_node, ContextPtr context)
{
    auto sum_count_node = std::make_shared<FunctionNode>("sumCount");

    DataTypePtr sum_return_type;
    DataTypePtr count_return_type;
    {
        AggregateFunctionProperties properties;
        auto function_aggregate_function = function_node.getAggregateFunction();

        auto aggregate_function = AggregateFunctionFactory::instance().get("sumCount",
            function_aggregate_function->getArgumentTypes(),
            function_aggregate_function->getParameters(),
            properties);

        sum_count_node->resolveAsAggregateFunction(aggregate_function, aggregate_function->getReturnType());

        if (auto ret_type = std::dynamic_pointer_cast<const DataTypeTuple>(aggregate_function->getReturnType()))
        {
            sum_return_type = ret_type->getElement(0);
            count_return_type = ret_type->getElement(1);
        }
        else
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected return type '{}' of sumCount aggregate function",
                aggregate_function->getReturnType()->getName());
        }

        sum_count_node->getArgumentsNode() = function_node.getArgumentsNode();
    }

    /// TODO: function_node.getResultType() or sum_return_type/count_return_type.
    /// Should it be the same?

    if (function_node.getFunctionName() == "sum")
        return createTupleElement(context, function_node.getResultType(), sum_count_node, 1);

    if (function_node.getFunctionName() == "count")
        return createTupleElement(context, function_node.getResultType(), sum_count_node, 2);

    if (function_node.getFunctionName() == "avg")
    {
        auto sum_result = createTupleElement(context, sum_return_type, sum_count_node, 1);
        auto count_result = createTupleElement(context, count_return_type, sum_count_node, 2);
        /// To avoid integer division by zero
        auto count_float_result = createResolvedFunction(context, "toFloat64", std::make_shared<DataTypeFloat64>(), count_result);
        return createResolvedFunction(context, "divide", function_node.getResultType(), sum_result, count_float_result);
    }

    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unsupported function '{}'", function_node.getFunctionName());
}

}

void FuseFunctionsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    FuseFunctionsMatcher visitor;
    visitor.visit(query_tree_node);

    for (auto & [_, nodes] : visitor.mapping)
    {
        if (nodes.size() < 2)
            continue;

        for (auto * node : nodes)
        {
            *node = createSumCount((*node)->as<const FunctionNode &>(), context);
        }
    }
}

}
