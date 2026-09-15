#include <Analyzer/Passes/DisableParallelReplicasPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
}

class DisableParallelReplicasVisitor : public InDepthQueryTreeVisitorWithContext<DisableParallelReplicasVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<DisableParallelReplicasVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (!isScope(node))
            return;

        const bool is_blocker = isBlocker(node);
        scopes.push_back({blockers_seen, is_blocker});
        if (is_blocker)
            ++blockers_on_path;
        blockers_seen += is_blocker;
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!isScope(node))
            return;

        const Scope scope = scopes.back();
        scopes.pop_back();
        if (scope.is_blocker)
            --blockers_on_path;

        /// A blocker constrains its own subtree, whose reads it drives, and its ancestors, which plan
        /// it. It says nothing about a node in a sibling branch: `Planner` plans the branches of a
        /// union independently.
        const bool blocker_in_subtree = blockers_seen > scope.blockers_seen_before;
        if (blockers_on_path == 0 && !blocker_in_subtree)
            return;

        if (auto * query_node = node->as<QueryNode>())
            disable(query_node->getMutableContext());
        if (auto * union_node = node->as<UnionNode>())
            disable(union_node->getMutableContext());
    }

private:
    struct Scope
    {
        size_t blockers_seen_before;
        bool is_blocker;
    };

    /// A node that owns a context, i.e. one whose parallel-replicas eligibility is decided separately.
    static bool isScope(const QueryTreeNodePtr & node) { return node->as<QueryNode>() || node->as<UnionNode>(); }

    static bool isBlocker(const QueryTreeNodePtr & node)
    {
        if (const auto * query_node = node->as<QueryNode>())
            return query_node->isCorrelated();

        if (const auto * union_node = node->as<UnionNode>())
        {
            /// A recursive CTE is initiator-local: its source owns a pair of temporary tables, rotates
            /// them between iterations and rewrites the CTE's self-reference in place, so the
            /// fixed-point loop has no distributed form.
            return union_node->isCorrelated() || union_node->hasRecursiveCTETable();
        }

        return false;
    }

    /// Every (sub)query carries its own context, and a nested `SETTINGS` clause can enable parallel
    /// replicas for one alone, so eligibility is a property of this node rather than of the query.
    /// It is also false on a follower, which must keep reading its assigned ranges.
    static void disable(const ContextMutablePtr & node_context)
    {
        if (!node_context->canUseParallelReplicasOnInitiator())
            return;

        node_context->setSetting("allow_experimental_parallel_reading_from_replicas", String("0"));
    }

    size_t blockers_seen = 0;
    size_t blockers_on_path = 0;
    std::vector<Scope> scopes;
};


void DisableParallelReplicasPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    DisableParallelReplicasVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
