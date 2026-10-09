#include <Analyzer/Passes/DisableParallelReplicasPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableNode.h>
#include <Analyzer/Utils.h>

#include <Common/typeid_cast.h>
#include <Core/Settings.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Storages/StorageMemory.h>
#include <Storages/StorageView.h>

namespace DB
{
namespace Setting
{
    extern const SettingsUInt64 allow_experimental_parallel_reading_from_replicas;
    extern const SettingsBool parallel_replicas_allow_view_over_mergetree;
}

/// Only `Memory` temporary tables are sent to the replicas, so a query that would make a replica read another
/// temporary table runs without parallel replicas.
class FindTemporaryTableNotSentToReplicasVisitor : public InDepthQueryTreeVisitorWithContext<FindTemporaryTableNotSentToReplicasVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<FindTemporaryTableNotSentToReplicasVisitor>;
    using Base::Base;

    bool needChildVisit(QueryTreeNodePtr &, QueryTreeNodePtr &) const { return !found; }

    void enterImpl(QueryTreeNodePtr & node)
    {
        const auto * table_node = node->as<TableNode>();
        if (found || !table_node)
            return;

        const auto & storage = table_node->getStorage();
        if (storage->getStorageID().database_name != DatabaseCatalog::TEMPORARY_DATABASE || typeid_cast<const StorageMemory *>(storage.get()))
            return;

        if (typeid_cast<const StorageView *>(storage.get()) && !getSettings()[Setting::parallel_replicas_allow_view_over_mergetree])
            return;

        found = true;
    }

    bool found = false;
};

class DisableParallelReplicasVisitor : public InDepthQueryTreeVisitorWithContext<DisableParallelReplicasVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<DisableParallelReplicasVisitor>;

    DisableParallelReplicasVisitor(ContextPtr context, bool disable_)
        : Base(std::move(context))
        , disable(disable_)
    {}

    void enterImpl(QueryTreeNodePtr & node)
    {
        if (disable)
            return;

        if (auto * query_node = node->as<QueryNode>())
            if (query_node->isCorrelated())
                disable = true;

        if (auto * union_node = node->as<UnionNode>())
            if (union_node->isCorrelated())
                disable = true;
    }

    void leaveImpl(QueryTreeNodePtr & node) const
    {
        if (!disable)
            return;

        if (auto * query_node = node->as<QueryNode>())
            query_node->getMutableContext()->setSetting("allow_experimental_parallel_reading_from_replicas", String("0"));
        if (auto * union_node = node->as<UnionNode>())
            union_node->getMutableContext()->setSetting("allow_experimental_parallel_reading_from_replicas", String("0"));
    }

private:
    bool disable = false;
};


void DisableParallelReplicasPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    bool reads_temporary_table_not_sent = false;
    if (context->canUseParallelReplicasOnInitiator() || context->canUseParallelReplicasCustomKey())
    {
        FindTemporaryTableNotSentToReplicasVisitor finder(context);
        finder.visit(query_tree_node);
        reads_temporary_table_not_sent = finder.found;
    }

    if (!context->canUseParallelReplicasOnInitiator() && !reads_temporary_table_not_sent)
        return;

    DisableParallelReplicasVisitor visitor(context, reads_temporary_table_not_sent);
    visitor.visit(query_tree_node);
}

}
