#include <Analyzer/Passes/UnifyGroupByKeysPass.h>

#include <algorithm>
#include <optional>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Passes/OptimizeKeyExpressionsUtils.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/Utils.h>
#include <Common/checkStackSize.h>
#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{

namespace Setting
{
    extern const SettingsBool group_by_use_nulls;
}

namespace
{

class UnifyGroupByKeysVisitor : public InDepthQueryTreeVisitorWithContext<UnifyGroupByKeysVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<UnifyGroupByKeysVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node || !query_node->hasGroupBy())
            return;

        /// The types of keys are different after the aggregation.
        if (getSettings()[Setting::group_by_use_nulls]
            && (query_node->isGroupByWithRollup() || query_node->isGroupByWithCube() || query_node->isGroupByWithGroupingSets()))
            return;

        keys = getGroupByKeys(*query_node);
        if (std::ranges::none_of(keys, hasFunctionWithConstant))
            return;

        equal_keys_can_differ = canEqualGroupByKeysDiffer(*query_node);

        for (auto & clause : query_node->getChildren())
        {
            /// These are calculated before the aggregation, or from the projection.
            if (!clause || clause == query_node->getJoinTreeNode() || clause == query_node->getPrewhere()
                || clause == query_node->getWhere() || clause == query_node->getGroupByNode() || clause == query_node->getInterpolate())
                continue;

            /// Its parts can be shared with other clauses.
            if (hasExpressionToReplace(clause))
            {
                clause = clause->clone();
                replaceWithKeys(clause);
            }
        }
    }

private:
    QueryTreeNodes keys;
    bool equal_keys_can_differ = false;

    static bool hasConstant(const QueryTreeNodePtr & node)
    {
        checkStackSize();

        return node->getNodeType() == QueryTreeNodeType::CONSTANT
            || std::ranges::any_of(node->getChildren(), [](const auto & child) { return child && hasConstant(child); });
    }

    static bool hasFunctionWithConstant(const QueryTreeNodePtr & node)
    {
        return node->getNodeType() == QueryTreeNodeType::FUNCTION && hasConstant(node);
    }

    /// If `node` is equal to a key, returns the key to replace it with, or nullptr if it is left as it is.
    std::optional<QueryTreeNodePtr> findKey(const QueryTreeNodePtr & node) const
    {
        QueryTreeNodePtr result;
        bool equal_keys_are_different = false;
        for (const auto & key : keys)
        {
            if (!key->isEqual(*node, {.compare_aliases = false}) || !areConstantsEqual(key, node))
                continue;

            equal_keys_are_different |= result && !areConstantsWrittenAlike(key, result);
            if (!result)
                result = key;
        }

        if (!result)
            return {};

        if (canBeCalculatedFromKeys(node, keys)
            || (equal_keys_are_different && (equal_keys_can_differ || hasFunctionNode(node, "arrayJoin"))))
            return nullptr;

        return result;
    }

    /// Nothing is replaced in columns and in subqueries. The arguments of aggregate functions are calculated before the aggregation.
    static bool hasReplaceableChildren(const QueryTreeNodePtr & node)
    {
        const auto * function_node = node->as<FunctionNode>();
        const auto node_type = node->getNodeType();
        return node_type != QueryTreeNodeType::COLUMN && node_type != QueryTreeNodeType::QUERY && node_type != QueryTreeNodeType::UNION
            && !(function_node && function_node->isAggregateFunction());
    }

    bool hasExpressionToReplace(const QueryTreeNodePtr & node) const
    {
        checkStackSize();

        if (node->getNodeType() == QueryTreeNodeType::FUNCTION)
            if (auto key = findKey(node))
                return *key != nullptr;

        return hasReplaceableChildren(node)
            && std::ranges::any_of(node->getChildren(), [&](const auto & child) { return child && hasExpressionToReplace(child); });
    }

    void replaceWithKeys(QueryTreeNodePtr & node) const
    {
        checkStackSize();

        if (node->getNodeType() == QueryTreeNodeType::FUNCTION)
        {
            if (auto key = findKey(node))
            {
                if (*key)
                {
                    auto alias = node->getAlias();
                    node = (*key)->clone();
                    node->setAlias(alias);
                }
                return;
            }
        }

        if (!hasReplaceableChildren(node))
            return;

        for (auto & child : node->getChildren())
            if (child)
                replaceWithKeys(child);
    }
};

}

void UnifyGroupByKeysPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    UnifyGroupByKeysVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
