#pragma once

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/Utils.h>
#include <fmt/ranges.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

/// Used to replace columns that changed type because of JOIN to their original type
class ReplaceColumnsVisitor : public InDepthQueryTreeVisitor<ReplaceColumnsVisitor>
{
public:
    explicit ReplaceColumnsVisitor(
        const QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> & replacement_map_,
        const ContextPtr & context_,
        IQueryTreeNode::CloneNodeMapping * applied_replacements_ = nullptr)
        : replacement_map(replacement_map_)
        , context(context_)
        , applied_replacements(applied_replacements_)
    {}

    /// Apply replacement transitively, because column may change it's type twice, one to have a supertype and then because of `joun_use_nulls`
    static QueryTreeNodePtr findTransitiveReplacement(QueryTreeNodePtr node, const QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> & replacement_map_)
    {
        auto it = replacement_map_.find(node);
        QueryTreeNodePtr result_node = nullptr;
        for (; it != replacement_map_.end(); it = replacement_map_.find(result_node))
        {
            if (result_node && result_node->isEqual(*it->second))
            {
                Strings map_dump;
                for (const auto & [k, v]: replacement_map_)
                    map_dump.push_back(fmt::format("{} -> {} (is_equals: {}, is_same: {})",
                        k.node->dumpTree(), v->dumpTree(), k.node->isEqual(*v), k.node == v));
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Infinite loop in query tree replacement map: {}", fmt::join(map_dump, "; "));
            }
            chassert(it->second);

            result_node = it->second;
        }
        return result_node;
    }

    void visitImpl(QueryTreeNodePtr & node)
    {
        if (auto replacement_node = findTransitiveReplacement(node, replacement_map))
        {
            if (applied_replacements && replacement_node.get() != node.get())
            {
                const auto [existing, inserted] = applied_replacements->emplace(node.get(), replacement_node);
                if (!inserted && existing->second.get() != replacement_node.get())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "One PREWHERE node maps to conflicting exact replacements");
            }
            node = replacement_node;
        }

        if (auto * function_node = node->as<FunctionNode>(); function_node && function_node->isResolved())
            rerunFunctionResolve(function_node, context);
    }

    /// We want to re-run resolve for function _after_ its arguments are replaced
    bool shouldTraverseTopToBottom() const { return false; }

    bool needChildVisit(QueryTreeNodePtr & /* parent */, QueryTreeNodePtr & child)
    {
        /// Visit only expressions, but not subqueries
        return child->getNodeType() == QueryTreeNodeType::IDENTIFIER
            || child->getNodeType() == QueryTreeNodeType::LIST
            || child->getNodeType() == QueryTreeNodeType::FUNCTION
            || child->getNodeType() == QueryTreeNodeType::COLUMN;
    }

private:
    const QueryTreeNodePtrWithHashMap<QueryTreeNodePtr> & replacement_map;
    const ContextPtr & context;
    IQueryTreeNode::CloneNodeMapping * applied_replacements = nullptr;
};

}
