#include <Analyzer/Passes/RewriteArrayFilterLengthToArrayCountPass.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
#include <Functions/FunctionFactory.h>

namespace DB
{
namespace Setting
{
    extern const SettingsBool optimize_rewrite_array_filter_length_to_array_count;
}

namespace
{

class RewriteArrayFilterLengthToArrayCountVisitor : public InDepthQueryTreeVisitorWithContext<RewriteArrayFilterLengthToArrayCountVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<RewriteArrayFilterLengthToArrayCountVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!getSettings()[Setting::optimize_rewrite_array_filter_length_to_array_count])
            return;

        const auto * length_function_node = node->as<FunctionNode>();
        if (!length_function_node || length_function_node->getFunctionName() != "length")
            return;

        const auto & length_arguments_nodes = length_function_node->getArguments().getNodes();
        if (length_arguments_nodes.size() != 1)
            return;

        const auto * array_filter_function_node = length_arguments_nodes[0]->as<FunctionNode>();
        if (!array_filter_function_node || array_filter_function_node->getFunctionName() != "arrayFilter")
            return;

        /// `arrayFilter` needs at least a lambda and one array.
        const auto & array_filter_arguments_nodes = array_filter_function_node->getArguments().getNodes();
        if (array_filter_arguments_nodes.size() < 2)
            return;

        /// Build a new node instead of rewriting the old one in place: the `arrayFilter` node can be
        /// referenced from somewhere else in the query tree, and its own result is still an array there.
        auto array_count_function_node = std::make_shared<FunctionNode>("arrayCount");
        array_count_function_node->getArguments().getNodes() = array_filter_arguments_nodes;

        auto array_count_function = FunctionFactory::instance().get("arrayCount", getContext());
        array_count_function_node->resolveAsFunction(array_count_function->build(array_count_function_node->getArgumentColumns()));

        /// `length` returns UInt64 and `arrayCount` returns UInt32, so the result has to be widened
        /// back, otherwise the rewrite would change the type of the column.
        node = createCastFunction(std::move(array_count_function_node), std::make_shared<DataTypeUInt64>(), getContext());
    }
};

}

void RewriteArrayFilterLengthToArrayCountPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteArrayFilterLengthToArrayCountVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
