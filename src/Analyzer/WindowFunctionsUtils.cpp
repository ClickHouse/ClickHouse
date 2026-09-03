#include <Analyzer/WindowFunctionsUtils.h>

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

class CollectWindowFunctionNodeVisitor : public ConstInDepthQueryTreeVisitor<CollectWindowFunctionNodeVisitor>
{
public:
    explicit CollectWindowFunctionNodeVisitor(QueryTreeNodes * window_function_nodes_)
        : window_function_nodes(window_function_nodes_)
    {}

    explicit CollectWindowFunctionNodeVisitor(String assert_no_window_functions_place_message_)
        : assert_no_window_functions_place_message(std::move(assert_no_window_functions_place_message_))
    {}

    explicit CollectWindowFunctionNodeVisitor(bool only_check_)
        : only_check(only_check_)
    {}

    void visitImpl(const QueryTreeNodePtr & node)
    {
        if (only_check && has_window_functions)
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || !function_node->isWindowFunction())
            return;

        if (!assert_no_window_functions_place_message.empty())
            throw Exception(ErrorCodes::ILLEGAL_AGGREGATION,
                "Window function {} is found {} in query",
                function_node->formatASTForErrorMessage(),
                assert_no_window_functions_place_message);

        if (window_function_nodes)
            window_function_nodes->push_back(node);

        has_window_functions = true;
    }

    bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr & child_node) const
    {
        if (only_check && has_window_functions)
            return false;

        auto child_node_type = child_node->getNodeType();
        return !(child_node_type == QueryTreeNodeType::QUERY || child_node_type == QueryTreeNodeType::UNION);
    }

    bool hasWindowFunctions() const
    {
        return has_window_functions;
    }
private:
    QueryTreeNodes * window_function_nodes = nullptr;
    String assert_no_window_functions_place_message;
    bool only_check = false;
    bool has_window_functions = false;
};

}

QueryTreeNodes collectWindowFunctionNodes(const QueryTreeNodePtr & node)
{
    QueryTreeNodes window_function_nodes;
    CollectWindowFunctionNodeVisitor visitor(&window_function_nodes);
    visitor.visit(node);

    return window_function_nodes;
}

bool hasWindowFunctionNodes(const QueryTreeNodePtr & node)
{
    CollectWindowFunctionNodeVisitor visitor(true /*only_check*/);
    visitor.visit(node);

    return visitor.hasWindowFunctions();
}

void collectWindowFunctionNodes(const QueryTreeNodePtr & node, QueryTreeNodes & result)
{
    CollectWindowFunctionNodeVisitor visitor(&result);
    visitor.visit(node);
}

void assertNoWindowFunctionNodes(const QueryTreeNodePtr & node, const String & assert_no_window_functions_place_message)
{
    CollectWindowFunctionNodeVisitor visitor(assert_no_window_functions_place_message);
    visitor.visit(node);
}

}
