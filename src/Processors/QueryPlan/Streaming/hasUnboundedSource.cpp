#include <Processors/QueryPlan/Streaming/hasUnboundedSource.h>

#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/SourceStepWithFilter.h>

#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

bool hasUnboundedSource(const QueryPlan & query_plan)
{
    if (!query_plan.isInitialized())
        return false;

    VectorWithMemoryTracking<const QueryPlan::Node *> nodes{query_plan.getRootNode()};
    while (!nodes.empty())
    {
        const auto * node = nodes.back();
        nodes.pop_back();

        if (const auto * source = dynamic_cast<const SourceStepWithFilter *>(node->step.get()))
        {
            const auto & modifiers = source->getQueryInfo().table_expression_modifiers;
            if (modifiers && modifiers->hasStream() && modifiers->getStreamSettings()->subscribe_for_updates)
                return true;
        }

        nodes.insert(nodes.end(), node->children.begin(), node->children.end());
    }

    return false;
}

}
