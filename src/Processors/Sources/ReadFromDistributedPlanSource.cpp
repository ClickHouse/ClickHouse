#include <Processors/Sources/ReadFromDistributedPlanSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>

namespace DB
{

void ReadFromDistributedPlanSource::cleanupLocked()
{
    if (cleaned_up)
        return;
    /// Mark cleaned up before the call so a throwing cleanup is not retried.
    cleaned_up = true;
    if (distributed_query_executor)
        distributed_query_executor->cleanup();
}

std::optional<Chunk> ReadFromDistributedPlanSource::tryGenerate()
{
    std::lock_guard lock(executor_mutex);

    /// Cancelled (via onCancel) or already finished - stop without launching/continuing work.
    if (cleaned_up || *cancellation_flag)
    {
        cleanupLocked();
        return std::nullopt;
    }

    try
    {
        if (!started)
        {
            started = true;
            distributed_query_executor = createDistributedQueryExecutor(
                unique_query_id, distributed_query_plan, task_to_host_map, CurrentThread::tryGetQueryContext(), cancellation_flag);
            distributed_query_executor->start();
        }

        if (distributed_query_executor->execute())
        {
            cleanupLocked();
            return std::nullopt;
        }
    }
    catch (...)
    {
        cleanupLocked();
        throw;
    }

    return Chunk();
}

void ReadFromDistributedPlanSource::onCancel() noexcept
{
    /// Signal first (lock-free) so an in-flight start()/execute() returns promptly, then tear down
    /// under the lock. Without active cleanup, cancellation is only seen on the next tryGenerate,
    /// which may never come once the pipeline is cancelled.
    *cancellation_flag = true;
    try
    {
        std::lock_guard lock(executor_mutex);
        cleanupLocked();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
