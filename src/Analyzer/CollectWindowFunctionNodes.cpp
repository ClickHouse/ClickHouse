#include <Analyzer/CollectWindowFunctionNodes.h>

#include <Analyzer/IQueryTreeNode.h>
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

class CollecWindowFunctionNodesMatcher
{
public:
    using Visitor = ConstInDepthQueryTreeVisitor<CollecWindowFunctionNodesMatcher, true, false>;

    struct Data
    {
        Data() = default;

        String assert_no_window_functions_place_message;
        QueryTreeNodes * window_function_nodes = nullptr;
    };

    static void visit(const QueryTreeNodePtr & node, Data & data)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isWindowFunction())
            return;

        if (!data.assert_no_window_functions_place_message.empty())
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "Window function {} is found {} in query",
                function_node->getName(),
                data.assert_no_window_functions_place_message);

        if (data.window_function_nodes)
            data.window_function_nodes->push_back(node);
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node)
    {
        return !(child_node->getNodeType() == QueryTreeNodeType::QUERY || child_node->getNodeType() == QueryTreeNodeType::UNION);
    }
};

using CollectWindowFunctionNodesVisitor = CollecWindowFunctionNodesMatcher::Visitor;

}

QueryTreeNodes collectWindowFunctionNodes(const QueryTreeNodePtr & node)
{
    QueryTreeNodes window_function_nodes;

    CollectWindowFunctionNodesVisitor::Data data;
    data.window_function_nodes = &window_function_nodes;

    CollectWindowFunctionNodesVisitor visitor(data);
    visitor.visit(node);

    return window_function_nodes;
}

void collectWindowFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes & result)
{
    CollectWindowFunctionNodesVisitor::Data data;
    data.window_function_nodes = &result;

    CollectWindowFunctionNodesVisitor visitor(data);
    visitor.visit(node);
}

void assertNoWindowFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_window_functions_place_message)
{
    CollectWindowFunctionNodesVisitor::Data data;
    data.assert_no_window_functions_place_message = assert_no_window_functions_place_message;

    CollectWindowFunctionNodesVisitor(data).visit(node);
}

}
