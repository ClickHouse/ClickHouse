#include <Analyzer/Passes/DisableParallelReplicasPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/UnionNode.h>
#include <Analyzer/Utils.h>

#include <Core/Settings.h>

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
        if (must_disable)
            return;

        if (auto * query_node = node->as<QueryNode>())
            if (query_node->isCorrelated())
                must_disable = true;

        if (auto * union_node = node->as<UnionNode>())
        {
            if (union_node->isCorrelated())
                must_disable = true;

            /// A recursive CTE is initiator-local: its source owns a pair of temporary tables, rotates
            /// them between iterations and rewrites the CTE's self-reference in place, so the
            /// fixed-point loop has no distributed form.
            if (union_node->hasRecursiveCTETable())
                must_disable = true;
        }
    }

    void leaveImpl(QueryTreeNodePtr & node) const
    {
        if (!must_disable)
            return;

        if (auto * query_node = node->as<QueryNode>())
            query_node->getMutableContext()->setSetting("allow_experimental_parallel_reading_from_replicas", String("0"));
        if (auto * union_node = node->as<UnionNode>())
            union_node->getMutableContext()->setSetting("allow_experimental_parallel_reading_from_replicas", String("0"));
    }

private:
    bool must_disable = false;
};


void DisableParallelReplicasPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    if (!context->canUseParallelReplicasOnInitiator())
        return;

    DisableParallelReplicasVisitor visitor(context);
    visitor.visit(query_tree_node);
}

}
