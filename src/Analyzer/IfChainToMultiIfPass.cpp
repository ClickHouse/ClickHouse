#include <Analyzer/IfChainToMultiIfPass.h>

#include <DataTypes/DataTypesNumber.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Functions/FunctionFactory.h>

namespace DB
{

namespace
{

class IfChainToMultiIfPassMatcher
{
public:
    using Visitor = InDepthQueryTreeVisitor<IfChainToMultiIfPassMatcher, true>;

    struct Data
    {
        FunctionOverloadResolverPtr multi_if_function_value;
    };

    static void visit(QueryTreeNodePtr & node, Data & data)
    {
        auto * function_node = node->as<FunctionNode>();
        if (!function_node || function_node->getFunctionName() != "if" || function_node->getArguments().getNodes().size() != 3)
            return;

        std::vector<QueryTreeNodePtr> multi_if_arguments;

        auto & function_node_arguments = function_node->getArguments().getNodes();
        multi_if_arguments.insert(multi_if_arguments.end(), function_node_arguments.begin(), function_node_arguments.end());

        QueryTreeNodePtr if_chain_node = multi_if_arguments.back();

        while (true)
        {
            /// Check if last `multiIf` argument is `if` function
            auto * if_chain_function_node = if_chain_node->as<FunctionNode>();
            if (!if_chain_function_node || if_chain_function_node->getFunctionName() != "if" || if_chain_function_node->getArguments().getNodes().size() != 3)
                break;

            /// Replace last `multiIf` argument with `if` function arguments

            multi_if_arguments.pop_back();

            auto & if_chain_function_node_arguments = if_chain_function_node->getArguments().getNodes();
            multi_if_arguments.insert(multi_if_arguments.end(), if_chain_function_node_arguments.begin(), if_chain_function_node_arguments.end());

            /// Use last `multiIf` argument for next check
            if_chain_node = multi_if_arguments.back();
        }

        /// Do not replace `if` with 3 arguments to `multiIf`
        if (multi_if_arguments.size() <= 3)
            return;

        auto multi_if_function = std::make_shared<FunctionNode>("multiIf");
        multi_if_function->resolveAsFunction(data.multi_if_function_value, std::make_shared<DataTypeUInt8>());
        multi_if_function->getArguments().getNodes() = std::move(multi_if_arguments);
        node = std::move(multi_if_function);
    }

    static bool needChildVisit(const QueryTreeNodePtr &, const QueryTreeNodePtr &)
    {
        return true;
    }
};

}

void IfChainToMultiIfPass::run(QueryTreeNodePtr query_tree_node, ContextPtr context)
{
    IfChainToMultiIfPassMatcher::Data data{FunctionFactory::instance().get("multiIf", context)};
    IfChainToMultiIfPassMatcher::Visitor visitor(data);
    visitor.visit(query_tree_node);
}

}
