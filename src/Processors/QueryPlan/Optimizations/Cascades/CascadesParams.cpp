#include <Processors/QueryPlan/Optimizations/Cascades/CascadesParams.h>

#include <Interpreters/Context.h>
#include <IO/ReadHelpers.h>

namespace DB
{

size_t getCascadesClusterNodeCountParam(ContextPtr context)
{
    if (context->getQueryParameters().contains(CascadesParams::CLUSTER_NODE_COUNT))
    {
        size_t value = parse<size_t>(context->getQueryParameters().at(CascadesParams::CLUSTER_NODE_COUNT));
        if (value > 0)
            return value;
    }
    return 0;
}

size_t getCascadesTaskLimitParam(ContextPtr context, size_t default_limit)
{
    /// The override lowers the budget for tests of the fail-closed path. In the join enumeration prototype it may also
    /// raise it, so that the search over every split of large join graphs can finish.
    if (context->getQueryParameters().contains(CascadesParams::TASK_LIMIT))
    {
        size_t value = parse<size_t>(context->getQueryParameters().at(CascadesParams::TASK_LIMIT));
        if (value > 0)
            return value;
    }
    return default_limit;
}

}
