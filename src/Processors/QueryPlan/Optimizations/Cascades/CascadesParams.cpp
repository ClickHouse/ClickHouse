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
    /// The override can only lower the budget (it exists so tests can force the fail-closed path).
    /// It must never raise the limit above the built-in cap: the task budget is the optimizer's
    /// work guard, so an unbounded override would let a single query spin the optimizer without
    /// bound. Values above the cap are clamped to it.
    if (context->getQueryParameters().contains(CascadesParams::TASK_LIMIT))
    {
        size_t value = parse<size_t>(context->getQueryParameters().at(CascadesParams::TASK_LIMIT));
        if (value > 0)
            return value < default_limit ? value : default_limit;
    }
    return default_limit;
}

}
