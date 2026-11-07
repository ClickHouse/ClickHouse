#include <Analyzer/Passes/RewriteArrayFilterLengthToArrayCountPass.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>

#include <Core/Settings.h>
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
        if (const auto * query_node = node->as<QueryNode>())
        {
            group_by_keys_stack.push_back(getGroupByKeys(*query_node));
            return;
        }

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

        /// A `GROUP BY` key is read from the aggregation result, so it is not rewritten into its arguments.
        if (isGroupByKeyAfterAggregation(length_arguments_nodes[0], group_by_keys_stack.back()))
            return;

        /// Build a new node instead of rewriting the old one in place: the `arrayFilter` node can be
        /// referenced from somewhere else in the query tree, and its own result is still an array there.
        auto array_count_function_node = std::make_shared<FunctionNode>("arrayCount");
        array_count_function_node->getArguments().getNodes() = array_filter_arguments_nodes;

        auto array_count_function = FunctionFactory::instance().get("arrayCount", getContext());
        array_count_function_node->resolveAsFunction(array_count_function->build(array_count_function_node->getArgumentColumns()));

        /// Both `length` and `arrayCount` return UInt64, so the rewrite changes neither the type nor
        /// the value of the column, for arrays of any size. The constness of the result is also the
        /// same: `arrayCount` returns a full column for a constant predicate as well, in the same way
        /// as `length(arrayFilter(...))` does.
        if (!array_count_function_node->getResultType()->equals(*length_function_node->getFunctionOrThrow()->getResultType()))
            return;

        /// A `group_by_use_nulls` copy of a `GROUP BY` key is the key made `Nullable`: rewrite it like the key and keep it `Nullable`.
        if (length_function_node->isConvertedToNullable())
            array_count_function_node->convertToNullable();

        /// Preserve the generated name because replacing the node changes the formatted expression.
        /// Consumers that match columns by name must continue to see the original `length` expression.
        auto original_column_name = node->formatConvertedASTForErrorMessage();
        array_count_function_node->setAlias(original_column_name);
        node = std::move(array_count_function_node);
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (node->getNodeType() == QueryTreeNodeType::QUERY)
            group_by_keys_stack.pop_back();
    }

private:
    /// The `GROUP BY` keys of each enclosing query; the first entry is for an expression outside of any query.
    std::vector<QueryTreeNodes> group_by_keys_stack{1};
};

}

void RewriteArrayFilterLengthToArrayCountPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    RewriteArrayFilterLengthToArrayCountVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
