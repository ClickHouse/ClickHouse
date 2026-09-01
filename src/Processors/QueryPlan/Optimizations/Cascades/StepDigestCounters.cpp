#include <Processors/QueryPlan/Optimizations/Cascades/StepDigestCounters.h>

namespace DB
{

thread_local StepDigestCounters * CurrentStepDigestCounters::current = nullptr;

CurrentStepDigestCounters::CurrentStepDigestCounters(StepDigestCounters & counters)
    : previous(current)
{
    current = &counters;
}

CurrentStepDigestCounters::~CurrentStepDigestCounters()
{
    current = previous;
}

}
