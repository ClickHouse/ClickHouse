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
    if (!started)
    {
        started = true;
        executeDistributedQuery(unique_query_id, distributed_query_plan, task_to_host_map, CurrentThread::getQueryContext(), cancellation_flag);
    }

    return {};
}

void ReadFromDistributedPlanSource::onCancel() noexcept
{
    *cancellation_flag = true;
}

}
