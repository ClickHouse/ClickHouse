#include <Processors/Sources/ReadFromDistributedPlanSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Common/CurrentThread.h>

namespace DB
{

Chunk ReadFromDistributedPlanSource::generate()
{
    if (!executed)
    {
        executeDistributedQuery(unique_query_id, distributed_query_plan, CurrentThread::getQueryContext());
        executed = true;
    }

    return {};
}

}
