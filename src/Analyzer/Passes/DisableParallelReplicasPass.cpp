#include <Analyzer/Passes/DisableParallelReplicasPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Storages/IStorage.h>

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

    /// With `dry_run`, only records whether some query or union keeps parallel replicas, see `disablesParallelReplicasInWholeTree`.
    DisableParallelReplicasVisitor(ContextPtr context, bool dry_run_)
        : Base(std::move(context))
        , dry_run(dry_run_)
    {
    }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (dry_run)
        {
            /// The tree shows how a `MergeTree` table is read: with the context of the query around it. Any other
            /// storage is opaque to it, so whether that read uses parallel replicas cannot be told from here.
            StoragePtr storage;
            if (const auto * table_node = node->as<TableNode>())
                storage = table_node->getStorage();
            else if (const auto * table_function_node = node->as<TableFunctionNode>())
                storage = table_function_node->getStorage();
            if (storage && !storage->isMergeTree())
                reads_opaque_storage = true;
        }

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

        /// `Planner` plans the branches of a union independently, so a blocker binds its own subtree
        /// and its ancestors, which plan it, but not a sibling branch.
        const bool blocker_in_subtree = blockers_seen > scope.blockers_seen_before;
        if (blockers_on_path == 0 && !blocker_in_subtree)
        {
            kept_in_some_query = true;
            return;
        }

        if (dry_run)
            return;

        if (auto * query_node = node->as<QueryNode>())
            disable(query_node->getMutableContext());
        if (auto * union_node = node->as<UnionNode>())
            disable(union_node->getMutableContext());
    }

    /// Every query and union has a blocker on its path or in its subtree, so the pass turns parallel replicas off
    /// in each of them, and no opaque storage, which might still use them, was read.
    [[nodiscard]] bool disablesEverywhere() const { return !kept_in_some_query && !reads_opaque_storage; }

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
            return query_node->isCorrelated() || evaluatesCorrelatedSubquery(*query_node);

        if (const auto * union_node = node->as<UnionNode>())
        {
            /// A recursive CTE is initiator-local: its source rotates a pair of temporary tables
            /// between iterations and rewrites the self-reference in place, so it has no distributed form.
            return union_node->isCorrelated() || union_node->hasRecursiveCTETable();
        }

        return false;
    }

    /// Decorrelation may clone the input plan of the query that evaluates a correlated subquery, its FROM
    /// subqueries included, and a parallel-replicas read step cannot be cloned.
    static bool evaluatesCorrelatedSubquery(const QueryNode & query_node)
    {
        std::vector<const IQueryTreeNode *> nodes;
        for (const auto & child : query_node.getChildren())
            if (child)
                nodes.push_back(child.get());

        while (!nodes.empty())
        {
            const auto * node = nodes.back();
            nodes.pop_back();

            if (const auto * subquery = node->as<QueryNode>())
            {
                if (subquery->isCorrelated())
                    return true;
            }
            else if (const auto * union_node = node->as<UnionNode>())
            {
                if (union_node->isCorrelated())
                    return true;
            }
            else
            {
                for (const auto & child : node->getChildren())
                    if (child)
                        nodes.push_back(child.get());
            }
        }

        return false;
    }

    /// `canUseParallelReplicasOnInitiator` is false on a follower, which must keep reading the ranges
    /// it was assigned, and it reads this node's own context, which a nested `SETTINGS` clause can change.
    /// The clones of a non-materialized CTE share one context, so the node gets a copy of its own.
    static void disable(ContextMutablePtr & node_context)
    {
        if (!node_context->canUseParallelReplicasOnInitiator())
            return;

        node_context = Context::createCopy(node_context);
        node_context->setSetting("allow_experimental_parallel_reading_from_replicas", String("0"));
    }

    const bool dry_run;
    size_t blockers_seen = 0;
    size_t blockers_on_path = 0;
    std::vector<Scope> scopes;
    bool kept_in_some_query = false;
    bool reads_opaque_storage = false;
};


void DisableParallelReplicasPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    DisableParallelReplicasVisitor visitor(context, /*dry_run=*/false);
    visitor.visit(query_tree_node);
}

bool disablesParallelReplicasInWholeTree(const QueryTreeNodePtr & query_tree_node)
{
    /// The same visitor walks the tree in the same order as the pass, so the answer cannot drift from what it does.
    auto node = query_tree_node;
    const auto & context = node->as<QueryNode>() ? node->as<QueryNode &>().getContext() : node->as<UnionNode &>().getContext();
    DisableParallelReplicasVisitor visitor(context, /*dry_run=*/true);
    visitor.visit(node);
    return visitor.disablesEverywhere();
}

}
