#include <Processors/Sources/ReadFromDistributedPlanSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <QueryPipeline/DistributedPlanExecutor.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>

#if defined(OS_LINUX) || defined(OS_DARWIN)
#include <Common/Epoll.h>
#endif

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
    if (cleaned_up || cancellation->isCancelled())
    {
        cleanupLocked();
        /// A failing task cancels the query too. Report its exception instead of reporting end of
        /// data, which would turn the failure into a silently truncated result.
        cancellation->rethrowIfFailed();
        return std::nullopt;
    }

    try
    {
        if (!started)
        {
            started = true;
            distributed_query_executor = createDistributedQueryExecutor(
                unique_query_id, distributed_query_plan, task_to_host_map, CurrentThread::tryGetQueryContext(), cancellation);
            distributed_query_executor->start();
        }

#if defined(OS_LINUX) || defined(OS_DARWIN)
        /// `prepare` waits for the stages in the async queue, so do not block an execution thread here.
        const UInt64 stage_poll_timeout_ms = 0;
#else
        const UInt64 stage_poll_timeout_ms = 100;
#endif
        if (distributed_query_executor->execute(stage_poll_timeout_ms))
        {
            cleanupLocked();
            return std::nullopt;
        }
    }
    catch (...)
    {
        /// Record the failure before the cleanup cancels the exchanges, so their consumers
        /// rethrow this root cause.
        cancellation->recordCurrentException();
        cleanupLocked();
        throw;
    }

#if defined(OS_LINUX) || defined(OS_DARWIN)
    waiting_for_stages = true;
#endif

    return Chunk();
}

IProcessor::Status ReadFromDistributedPlanSource::prepare()
{
    auto status = ISource::prepare();

#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// `Ready` means `work` would be called next, but the stages are still running and there is
    /// nothing for it to do until the poll interval elapses. Give the execution thread back.
    if (status == Status::Ready && waiting_for_stages)
        return Status::Async;
#endif

    return status;
}

#if defined(OS_LINUX) || defined(OS_DARWIN)
std::tuple<int, uint32_t, Int64> ReadFromDistributedPlanSource::scheduleForEvent()
{
    stage_poll_timer.setRelative(stage_poll_interval_us);
    return {stage_poll_timer.getDescriptor(), EPOLLIN | EPOLLERR, -1};
}

void ReadFromDistributedPlanSource::onAsyncJobReady()
{
    stage_poll_timer.drain();
    waiting_for_stages = false;
}
#endif

void ReadFromDistributedPlanSource::onCancel() noexcept
{
    /// Signal first (lock-free) so an in-flight start()/execute() returns promptly, then tear down
    /// under the lock. Without active cleanup, cancellation is only seen on the next tryGenerate,
    /// which may never come once the pipeline is cancelled.
    cancellation->cancel();
    try
    {
        /// Wake exchange waiters before taking the lock: the lock holder itself may be blocked
        /// on an exchange or on a stage whose tasks are, and would never release it otherwise.
        cancelDistributedQueryInMemoryExchanges(unique_query_id, cancellation->getFailure());
        std::lock_guard lock(executor_mutex);
        cleanupLocked();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
