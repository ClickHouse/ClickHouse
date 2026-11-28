#include <Analyzer/Passes/DisableParallelReplicasPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/IStorage.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageView.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_allow_view_over_mergetree;
    extern const SettingsBool parallel_replicas_plan_based;
}

/// Plan-based parallel replicas keep the read of a temporary table on the initiator.
static bool canUseParallelReplicas(const ContextPtr & context)
{
    return (context->canUseParallelReplicasOnInitiator() && !context->getSettingsRef()[Setting::parallel_replicas_plan_based])
        || context->canUseParallelReplicasCustomKey();
}

/// Only `Memory` temporary tables are sent to the replicas.
class FindTemporaryTableNotSentToReplicasVisitor : public InDepthQueryTreeVisitorWithContext<FindTemporaryTableNotSentToReplicasVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<FindTemporaryTableNotSentToReplicasVisitor>;

    explicit FindTemporaryTableNotSentToReplicasVisitor(ContextPtr context)
        : Base(context)
        , parallel_replicas_possible(canUseParallelReplicas(context))
    {}

    [[nodiscard]] bool found() const { return parallel_replicas_possible && reads_temporary_table; }

    bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr &) const { return !found(); }

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (node->as<QueryNode>() || node->as<UnionNode>())
        {
            parallel_replicas_possible |= canUseParallelReplicas(getContext());
            return;
        }

        const auto * table_node = node->as<TableNode>();
        if (reads_temporary_table || !table_node)
            return;

        const auto & storage = table_node->getStorage();
        if (storage->getStorageID().database_name != DatabaseCatalog::TEMPORARY_DATABASE || typeid_cast<const StorageMemory *>(storage.get()))
            return;

        if (typeid_cast<const StorageView *>(storage.get()) && !getSettings()[Setting::parallel_replicas_allow_view_over_mergetree])
            return;

        reads_temporary_table = true;
    }

private:
    /// A subquery's own `SETTINGS` can enable parallel replicas that the outer query does not use.
    bool parallel_replicas_possible;
    bool reads_temporary_table = false;
};

class DisableParallelReplicasVisitor : public InDepthQueryTreeVisitorWithContext<DisableParallelReplicasVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<DisableParallelReplicasVisitor>;

    /// With `dry_run`, only records whether some query or union keeps parallel replicas, see `disablesParallelReplicasInWholeTree`.
    DisableParallelReplicasVisitor(ContextPtr context, bool dry_run_, bool disable_)
        : Base(std::move(context))
        , dry_run(dry_run_)
        , disable(disable_)
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

        if (disable)
            return;

        if (auto * query_node = node->as<QueryNode>())
            if (query_node->isCorrelated())
                disable = true;

        if (auto * union_node = node->as<UnionNode>())
            if (union_node->isCorrelated())
                disable = true;
    }

    void leaveImpl(QueryTreeNodePtr & node)
    {
        if (!disable)
        {
            /// Left before `disable` latched, so it keeps parallel replicas. The flag is never reset, so
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
    [[nodiscard]] bool disablesEverywhere() const { return disable && !kept_in_some_query && !reads_opaque_storage; }

private:
    const bool dry_run;
    bool disable;
    bool kept_in_some_query = false;
    bool reads_opaque_storage = false;
};


void DisableParallelReplicasPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    FindTemporaryTableNotSentToReplicasVisitor finder(context);
    finder.visit(query_tree_node);
    const bool reads_temporary_table_not_sent = finder.found();

    if (!context->canUseParallelReplicasOnInitiator() && !reads_temporary_table_not_sent)
        return;

    DisableParallelReplicasVisitor visitor(context, /*dry_run=*/false, /*disable_=*/reads_temporary_table_not_sent);
    visitor.visit(query_tree_node);
}

bool disablesParallelReplicasInWholeTree(const QueryTreeNodePtr & query_tree_node)
{
    /// The same visitor walks the tree in the same order as the pass, so the answer cannot drift from what it does.
    auto node = query_tree_node;
    const auto & context = node->as<QueryNode>() ? node->as<QueryNode &>().getContext() : node->as<UnionNode &>().getContext();
    DisableParallelReplicasVisitor visitor(context, /*dry_run=*/true, /*disable_=*/false);
    visitor.visit(node);
    return visitor.disablesEverywhere();
}

}
