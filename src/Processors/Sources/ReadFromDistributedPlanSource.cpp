#include <Processors/Sources/ReadFromDistributedPlanSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Common/CurrentThread.h>

namespace DB
{

std::optional<Chunk> ReadFromDistributedPlanSource::tryGenerate()
{
    if (!started)
    {
        started = true;
        distributed_query_executor = createDistributedQueryExecutor(unique_query_id, distributed_query_plan, task_to_host_map, CurrentThread::tryGetQueryContext(), cancellation_flag);
        distributed_query_executor->start();
    }

    try
    {
        const bool query_finished = distributed_query_executor->execute();
        if (query_finished)
        {
            distributed_query_executor->cleanup();
            return Chunk();
        }
    }
    catch (...)
    {
        distributed_query_executor->cleanup();
        throw;
    }

    return std::nullopt;
}

void ReadFromDistributedPlanSource::onCancel() noexcept
{
    *cancellation_flag = true;
}

}
