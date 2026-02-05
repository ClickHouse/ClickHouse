#include <Analyzer/Passes/HasToInPass.h>

#include <DataTypes/IDataType.h>

#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_has_to_in;
}

namespace
{

class RewriteHasToInVisitor : public InDepthQueryTreeVisitorWithContext<RewriteHasToInVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteHasToInVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_has_to_in])
            return;

        auto * has_function_node = node->as<FunctionNode>();
        if (!has_function_node || has_function_node->getFunctionName() != "has")
            return;

        auto & has_function_arguments_nodes = has_function_node->getArguments().getNodes();
        if (has_function_arguments_nodes.size() != 2)
            return;

        /// Check if the first argument is a constant array
        const auto * first_arg_constant = has_function_arguments_nodes[0]->as<ConstantNode>();
        if (!first_arg_constant)
            return;

        /// Verify that the first argument is actually an array type
        const auto & first_arg_type = has_function_arguments_nodes[0]->getResultType();
        if (!isArray(first_arg_type))
            return;

        /// Rewrite has(const_array, elem) -> in(elem, const_array)
        /// Swap the arguments
        std::swap(has_function_arguments_nodes[0], has_function_arguments_nodes[1]);

        /// Resolve the function as 'in' function
        resolveOrdinaryFunctionNodeByName(*has_function_node, "in", getContext());
    }
};

}

void RewriteHasToInPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteHasToInVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
