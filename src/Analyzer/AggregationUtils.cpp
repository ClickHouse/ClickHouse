#include <Analyzer/AggregationUtils.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_AGGREGATION;
}

namespace
{

class CollectAggregateFunctionNodesVisitor : public ConstInDepthQueryTreeVisitor<CollectAggregateFunctionNodesVisitor>
{
public:
    explicit CollectAggregateFunctionNodesVisitor(QueryTreeNodes * aggregate_function_nodes_)
        : aggregate_function_nodes(aggregate_function_nodes_)
    {}

    explicit CollectAggregateFunctionNodesVisitor(String assert_no_aggregates_place_message_)
        : assert_no_aggregates_place_message(std::move(assert_no_aggregates_place_message_))
    {}

    explicit CollectAggregateFunctionNodesVisitor(bool only_check_)
        : only_check(only_check_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (only_check && has_aggregate_functions)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isAggregateFunction())
            return;

        if (!assert_no_aggregates_place_message.empty())
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "Aggregate function {} is found {} in query",
                function_node->formatASTForErrorMessage(),
                assert_no_aggregates_place_message);

        if (aggregate_function_nodes)
            aggregate_function_nodes->push_back(node);

        has_aggregate_functions = true;
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node) const
    {
        if (only_check && has_aggregate_functions)
            return false;

        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

    bool hasAggregateFunctions() const
    {
        return has_aggregate_functions;
    }

private:
    String assert_no_aggregates_place_message;
    QueryTreeNodes * aggregate_function_nodes = nullptr;
    bool only_check = false;
    bool has_aggregate_functions = false;
};

}

QueryTreeNodes collectAggregateFunctionNodes(const QueryTreeNodePtr & node)
{
    QueryTreeNodes result;
    CollectAggregateFunctionNodesVisitor visitor(&result);
    visitor.visit(node);

    return result;
}

void collectAggregateFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes & result)
{
    CollectAggregateFunctionNodesVisitor visitor(&result);
    visitor.visit(node);
}

bool hasAggregateFunctionNodes(const QueryTreeNodePtr & node)
{
    CollectAggregateFunctionNodesVisitor visitor(true /*only_check*/);
    visitor.visit(node);

    return visitor.hasAggregateFunctions();
}

void assertNoAggregateFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_aggregates_place_message)
{
    CollectAggregateFunctionNodesVisitor visitor(assert_no_aggregates_place_message);
    visitor.visit(node);
}

void assertNoGroupingFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_grouping_function_place_message)
{
    assertNoFunctionNodes(node, "grouping", ErrorCodes::ILLEGAL_AGGREGATION, "GROUPING", assert_no_grouping_function_place_message);
}

}
