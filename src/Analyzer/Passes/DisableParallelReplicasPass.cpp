#include <Analyzer/Passes/DisableParallelReplicasPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>
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

        if (is_correlated)
            return;

        if (auto * query_node = node->as<QueryNode>())
            if (query_node->isCorrelated())
                is_correlated = true;

        if (auto * union_node = node->as<UnionNode>())
            if (union_node->isCorrelated())
                is_correlated = true;
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!is_correlated)
        {
            /// Left before `is_correlated` latched, so it keeps parallel replicas. The flag is never reset, so
            /// every query and union left after that, the root and siblings walked later included, loses them.
            if (node->as<QueryNode>() || node->as<UnionNode>())
                kept_in_some_query = true;
            return;
        }

        if (dry_run)
            return;

        if (auto * query_node = node->as<QueryNode>())
            query_node->getMutableContext()->setSetting("allow_experimental_parallel_reading_from_replicas", String("0"));
        if (auto * union_node = node->as<UnionNode>())
            union_node->getMutableContext()->setSetting("allow_experimental_parallel_reading_from_replicas", String("0"));
    }

    /// Latched with no query or union left before it, so the pass turns parallel replicas off in every one of them,
    /// and no opaque storage, which might still use them, was read.
    [[nodiscard]] bool disablesEverywhere() const { return is_correlated && !kept_in_some_query && !reads_opaque_storage; }

private:
    const bool dry_run;
    bool is_correlated = false;
    bool kept_in_some_query = false;
    bool reads_opaque_storage = false;
};


void DisableParallelReplicasPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    if (!context->canUseParallelReplicasOnInitiator())
        return;

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
