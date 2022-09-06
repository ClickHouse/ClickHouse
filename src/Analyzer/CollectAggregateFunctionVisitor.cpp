#include <Analyzer/CollectAggregateFunctionVisitor.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_AGGREGATION;
}

namespace
{

class CollectAggregateFunctionNodesMatcher
{
public:
    using Visitor = ConstInDepthQueryTreeVisitor<CollectAggregateFunctionNodesMatcher, true, false>;

    struct Data
    {
        Data() = default;

        String assert_no_aggregates_place_message;
        QueryTreeNodes * aggregate_function_nodes = nullptr;
    };

    static void visit(const QueryTreeNodePtr & node, Data & data)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node)
            return;

        if (!function_node->isAggregateFunction())
            return;

        if (!data.assert_no_aggregates_place_message.empty())
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "Aggregate function {} is found {} in query",
                function_node->getName(),
                data.assert_no_aggregates_place_message);

        if (data.aggregate_function_nodes)
            data.aggregate_function_nodes->push_back(node);
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return !(child_node->getNodeType() == QueryTreeNodeType::QUERY || child_node->getNodeType() == QueryTreeNodeType::UNION);
    }
};

using CollectAggregateFunctionNodesVisitor = CollectAggregateFunctionNodesMatcher::Visitor;

}

QueryTreeNodes collectAggregateFunctionNodes(const QueryTreeNodePtr & node)
{
    QueryTreeNodes result;

    CollectAggregateFunctionNodesVisitor::Data data;
    data.aggregate_function_nodes = &result;

    CollectAggregateFunctionNodesVisitor(data).visit(node);

    return result;
}

void collectAggregateFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes & result)
{
    CollectAggregateFunctionNodesVisitor::Data data;
    data.aggregate_function_nodes = &result;

    CollectAggregateFunctionNodesVisitor(data).visit(node);
}

void assertNoAggregateFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_aggregates_place_message)
{
    CollectAggregateFunctionNodesVisitor::Data data;
    data.assert_no_aggregates_place_message = assert_no_aggregates_place_message;

    CollectAggregateFunctionNodesVisitor(data).visit(node);
}

}
