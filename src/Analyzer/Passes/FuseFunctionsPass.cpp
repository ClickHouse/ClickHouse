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

class FuseFunctionsVisitor : public InDepthQueryTreeVisitor<FuseFunctionsVisitor>
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

        const auto & arguments = function_node->getArgumentsNode()->getChildren();
        if (arguments.size() != 1)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Aggregate function {} must have exactly one argument", function_node->getFunctionName());

        mapping[QueryTreeNodeWithHash(arguments[0])].push_back(&node);
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
};

QueryTreeNodePtr createResolvedFunction(ContextPtr context, const String & name, DataTypePtr result_type, QueryTreeNodes arguments)
{
    auto function_node = std::make_shared<FunctionNode>(name);
    auto function = FunctionFactory::instance().get(name, context);
    function_node->resolveAsFunction(function, result_type);
    function_node->getArgumentsNode() = std::make_shared<ListNode>(std::move(arguments));
    return function_node;
}

FunctionNodePtr createSumCoundNode(const QueryTreeNodePtr & argument)
{
    auto sum_count_node = std::make_shared<FunctionNode>("sumCount");

    AggregateFunctionProperties properties;
    auto aggregate_function = AggregateFunctionFactory::instance().get("sumCount", {argument->getResultType()}, {}, properties);

    sum_count_node->resolveAsAggregateFunction(aggregate_function, aggregate_function->getReturnType());

    sum_count_node->getArgumentsNode() = std::make_shared<ListNode>(QueryTreeNodes{argument});
    return sum_count_node;
}

QueryTreeNodePtr createTupleElementFunction(ContextPtr context, DataTypePtr result_type, QueryTreeNodePtr argument, UInt64 index)
{
    return createResolvedFunction(context, "tupleElement", result_type, {argument, std::make_shared<ConstantNode>(index)});
}

void replaceWithSumCount(QueryTreeNodePtr & node, const FunctionNodePtr & sum_count_node, ContextPtr context)
{
    auto sum_count_result_type = std::dynamic_pointer_cast<const DataTypeTuple>(sum_count_node->getResultType());
    if (!sum_count_result_type || sum_count_result_type->getElements().size() != 2)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Unexpected return type '{}' of function {}, should be tuple of two elements",
            sum_count_node->getResultType(), sum_count_node->getFunctionName());
    }

    String function_name = node->as<const FunctionNode &>().getFunctionName();

    if (function_name == "sum")
    {
        assert(node->getResultType() == sum_count_result_type->getElement(0));
        node = createTupleElementFunction(context, node->getResultType(), sum_count_node, 1);
    }
    else if (function_name == "count")
    {
        assert(node->getResultType() == sum_count_result_type->getElement(1));
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

}

void FuseFunctionsPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    FuseFunctionsVisitor visitor;
    visitor.visit(query_tree_node);

    for (auto & [argument, nodes] : visitor.mapping)
    {
        if (nodes.size() < 2)
            continue;

        auto sum_count_node = createSumCoundNode(argument.node);
        for (auto * node : nodes)
        {
            replaceWithSumCount(*node, sum_count_node, context);
        }
    }
}

}
