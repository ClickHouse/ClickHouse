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

    void visitImpl(const QueryTreeNodePtr & node)
    {
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
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return !(child_node->getNodeType() == QueryTreeNodeType::QUERY || child_node->getNodeType() == QueryTreeNodeType::UNION);
    }

private:
    String assert_no_aggregates_place_message;
    QueryTreeNodes * aggregate_function_nodes = nullptr;
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

void assertNoAggregateFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_aggregates_place_message)
{
    CollectAggregateFunctionNodesVisitor visitor(assert_no_aggregates_place_message);
    visitor.visit(node);
}

void assertNoGroupingFunction(const QueryTreeNodePtr & node, const String & assert_no_grouping_function_place_message)
{
    assertNoFunction(node, "grouping", ErrorCodes::ILLEGAL_AGGREGATION, "GROUPING", assert_no_grouping_function_place_message);
}

}
