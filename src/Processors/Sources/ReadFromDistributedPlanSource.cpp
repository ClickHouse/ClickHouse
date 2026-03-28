#include <Processors/Sources/ReadFromDistributedPlanSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Common/CurrentThread.h>
#include <Common/logger_useful.h>

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
        LOG_TRACE(getLogger("ReadFromDistributedPlanSource"), "execute() returned {}", query_finished ? "finished" : "running");
        if (!query_finished)
            return Chunk(); /// Not finished yet — return empty chunk to be called again

        distributed_query_executor->cleanup();
        return {}; /// Finished — signal ISource to close the output
    }
    catch (...)
    {
        distributed_query_executor->cleanup();
        throw;
    }
}

void ReadFromDistributedPlanSource::onCancel() noexcept
{
    *cancellation_flag = true;
}

}
