#include <Analyzer/Passes/IfConstantConditionPass.h>

#include <Functions/FunctionFactory.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/HashUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>

namespace DB
{

namespace
{

/// The planner tells GROUP BY keys apart by expression and by column source, not by the aliases in the expression.
/// A column is named after the unique alias of its source instead, so that comparing keys does not compare sources.
QueryTreeNodePtr cloneForComparison(const QueryTreeNodePtr & node)
{
    auto name_after_source = [](QueryTreeNodePtr & current)
    {
        const auto * column_node = current->as<ColumnNode>();
        if (!column_node)
            return;
        auto source = column_node->getColumnSourceOrNull();
        if (source && source->hasAlias())
            current = std::make_shared<ColumnNode>(
                NameAndTypePair{source->getAlias() + "." + column_node->getColumnName(), column_node->getColumnType()},
                TableExpressionNodeWeakPtr{});
    };

    auto result = node->clone();
    name_after_source(result);
    std::vector<IQueryTreeNode *> nodes{result.get()};
    while (!nodes.empty())
    {
        auto * current = nodes.back();
        nodes.pop_back();
        current->removeAlias();
        for (auto & child : current->getChildren())
        {
            if (!child || child->getNodeType() == QueryTreeNodeType::QUERY || child->getNodeType() == QueryTreeNodeType::UNION)
                continue;
            name_after_source(child);
            nodes.push_back(child.get());
        }
    }
    return result;
}

class IfConstantConditionVisitor : public InDepthQueryTreeVisitorWithContext<IfConstantConditionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<IfConstantConditionVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (const auto * query_node = node->as<QueryNode>())
            skip_query_folding.push_back(foldingMergesGroupingKeys(*query_node));
    }

    /// After the arguments, so that a chain collapses in one visit and visiting a shared node again is a no-op.
    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (node->getNodeType() == QueryTreeNodeType::QUERY)
        {
            skip_query_folding.pop_back();
            return;
        }

        if (!skip_query_folding.empty() && skip_query_folding.back())
            return;

        auto * function_node = node->as<FunctionNode>();
        if (!function_node || (function_node->getFunctionName() != "if" && function_node->getFunctionName() != "multiIf"))
            return;

        if (function_node->getArguments().getNodes().size() != 3)
            return;

        auto & first_argument = function_node->getArguments().getNodes()[0];
        const auto * first_argument_constant_node = first_argument->as<ConstantNode>();
        if (!first_argument_constant_node)
            return;

        const auto & condition_value = first_argument_constant_node->getValue();

        bool condition_boolean_value = false;

        if (condition_value.getType() == Field::Types::Int64)
            condition_boolean_value = static_cast<bool>(condition_value.safeGet<Int64>());
        else if (condition_value.getType() == Field::Types::UInt64)
            condition_boolean_value = static_cast<bool>(condition_value.safeGet<UInt64>());
        else
            return;

        QueryTreeNodePtr argument_node;
        if (condition_boolean_value)
            argument_node = function_node->getArguments().getNodes()[1];
        else
            argument_node = function_node->getArguments().getNodes()[2];

        if (node->getResultType()->equals(*argument_node->getResultType()))
        {
            node = argument_node;
            return;
        }

        /// A `group_by_use_nulls` copy of a GROUP BY key is the key made Nullable: fold it like the key and keep it Nullable.
        const auto argument_node_type = argument_node->getNodeType();
        const bool argument_can_be_nullable = argument_node_type == QueryTreeNodeType::COLUMN
            || (argument_node_type == QueryTreeNodeType::FUNCTION && argument_node->as<FunctionNode &>().isOrdinaryFunction());
        if (argument_can_be_nullable && function_node->getFunctionOrThrow()->getResultType()->equals(*argument_node->getResultType()))
        {
            auto nullable_argument_node = argument_node->clone();
            nullable_argument_node->convertToNullable();
            node = std::move(nullable_argument_node);
        }
    }

private:
    /// ROLLUP, CUBE and GROUPING SETS take their grouping sets from the distinct keys, so folding may make two keys
    /// equal only if they are in the same grouping sets; a ROLLUP or CUBE key is told apart by its position.
    bool foldingMergesGroupingKeys(const QueryNode & query_node) const
    {
        if (!query_node.isGroupByWithRollup() && !query_node.isGroupByWithCube() && !query_node.isGroupByWithGroupingSets())
            return false;

        QueryTreeNodePtrWithHashMap<std::vector<size_t>> key_grouping_sets;
        auto add_key = [&](const QueryTreeNodePtr & key, size_t grouping_set)
        {
            auto & grouping_sets = key_grouping_sets[cloneForComparison(key)];
            if (grouping_sets.empty() || grouping_sets.back() != grouping_set)
                grouping_sets.push_back(grouping_set);
        };

        const auto & group_by_nodes = query_node.getGroupBy().getNodes();
        for (size_t i = 0; i < group_by_nodes.size(); ++i)
        {
            if (query_node.isGroupByWithGroupingSets())
            {
                for (const auto & key : group_by_nodes[i]->as<ListNode &>().getNodes())
                    add_key(key, i);
            }
            else
            {
                add_key(group_by_nodes[i], i);
            }
        }

        QueryTreeNodePtrWithHashMap<const std::vector<size_t> *> folded_key_grouping_sets;
        for (const auto & [key, grouping_sets] : key_grouping_sets)
        {
            auto folded_key = key.node->clone();
            IfConstantConditionVisitor(getContext()).visit(folded_key);
            auto [it, inserted] = folded_key_grouping_sets.emplace(std::move(folded_key), &grouping_sets);
            if (!inserted && *it->second != grouping_sets)
                return true;
        }

        return false;
    }

    /// Whether folding is skipped in the expressions of each enclosing query; nested queries decide for themselves.
    std::vector<bool> skip_query_folding;
};

}

void IfConstantConditionPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    IfConstantConditionVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
