#include <Analyzer/Passes/IfChainToMultiIfPass.h>


#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Core/Settings.h>
#include <Functions/multiIf.h>

#include <unordered_set>

namespace DB
{
namespace Setting
{
    extern const SettingsBool allow_execute_multiif_columnar;
    extern const SettingsBool optimize_if_chain_to_multiif;
    extern const SettingsBool use_variant_as_common_type;
    extern const SettingsBool allow_lossy_numeric_supertype;
}

namespace
{

/// The `if` nodes of the query, outside of its subqueries, that are `GROUP BY` keys, collected before any of them is rewritten.
std::unordered_set<QueryTreeNodePtr> collectGroupByKeyIfNodes(const QueryNode & query_node)
{
    auto is_if = [](const QueryTreeNodePtr & node)
    {
        const auto * function_node = node->as<FunctionNode>();
        return function_node && function_node->getFunctionName() == "if" && function_node->getArguments().getNodes().size() == 3;
    };

    auto if_keys = getGroupByKeys(query_node);
    std::erase_if(if_keys, [&](const auto & key) { return !is_if(key); });

    std::unordered_set<QueryTreeNodePtr> result;
    if (if_keys.empty())
        return result;

    std::vector<const IQueryTreeNode *> nodes{&query_node};
    while (!nodes.empty())
    {
        const auto * node = nodes.back();
        nodes.pop_back();
        for (const auto & child : node->getChildren())
        {
            if (!child || child->getNodeType() == QueryTreeNodeType::QUERY || child->getNodeType() == QueryTreeNodeType::UNION)
                continue;
            if (is_if(child) && isGroupByKeyAfterAggregation(child, if_keys))
                result.insert(child);
            nodes.push_back(child.get());
        }
    }
    return result;
}

class IfChainToMultiIfPassVisitor : public InDepthQueryTreeVisitorWithContext<IfChainToMultiIfPassVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<IfChainToMultiIfPassVisitor>;
    using Base::Base;

    explicit IfChainToMultiIfPassVisitor(FunctionOverloadResolverPtr multi_if_function_ptr_, ContextPtr context)
        : Base(std::move(context))
        , multi_if_function_ptr(std::move(multi_if_function_ptr_))
    {
        group_by_key_if_nodes_stack.emplace_back();
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (const auto * query_node = node->as<QueryNode>())
        {
            group_by_key_if_nodes_stack.push_back(
                getSettings()[Setting::optimize_if_chain_to_multiif] ? collectGroupByKeyIfNodes(*query_node) : std::unordered_set<QueryTreeNodePtr>{});
            return;
        }

        if (!getSettings()[Setting::optimize_if_chain_to_multiif])
            return;

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

            /// A `GROUP BY` key is read from the aggregation result, so it is not spliced into the chain.
            if (if_chain_function_node->isConvertedToNullable() || group_by_key_if_nodes_stack.back().contains(if_chain_node))
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
        multi_if_function->getArguments().getNodes() = std::move(multi_if_arguments);
        multi_if_function->resolveAsFunction(multi_if_function_ptr->build(multi_if_function->getArgumentColumns()));

        /// Ignore if returned type changed.
        /// Example : SELECT now64(if(Null, NULL, if(Null, nan, toFloat64(number))), Null) FROM numbers(2)
        if (!multi_if_function->getResultType()->equals(*function_node->getFunctionOrThrow()->getResultType()))
            return;

        /// A `group_by_use_nulls` copy of a GROUP BY key is the key made Nullable: rewrite it like the key and keep it Nullable.
        if (function_node->isConvertedToNullable())
            multi_if_function->convertToNullable();

        node = std::move(multi_if_function);
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (node->getNodeType() == QueryTreeNodeType::QUERY)
            group_by_key_if_nodes_stack.pop_back();
    }

private:
    FunctionOverloadResolverPtr multi_if_function_ptr;

    /// For each enclosing query, its `if` nodes that are `GROUP BY` keys; the first set is for an expression outside of any query.
    std::vector<std::unordered_set<QueryTreeNodePtr>> group_by_key_if_nodes_stack;
};

}

void IfChainToMultiIfPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    const auto & settings = context->getSettingsRef();
    auto multi_if_function_ptr = createInternalMultiIfOverloadResolver(
        settings[Setting::allow_execute_multiif_columnar],
        settings[Setting::use_variant_as_common_type],
        settings[Setting::allow_lossy_numeric_supertype]);
    IfChainToMultiIfPassVisitor visitor(std::move(multi_if_function_ptr), std::move(context));
    visitor.visit(query_tree_node);
}

}
