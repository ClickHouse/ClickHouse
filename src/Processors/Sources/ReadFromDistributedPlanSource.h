#pragma once

#include <memory>
#include <mutex>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/ISource.h>
#include <Core/Types_fwd.h>
#include <QueryPipeline/DistributedPlanExecutor.h>

namespace DB
{

class TaskToHostMap;
using TaskToHostMapPtr = std::shared_ptr<const TaskToHostMap>;

/// Wraps distributed plan execution.
/// It is used in pipeline on distributed query initiator.
class ReadFromDistributedPlanSource final : public ISource
{
public:
    ReadFromDistributedPlanSource(
        SharedHeader header_,
        const UUID & unique_query_id_,
        DistributedQueryPlan distributed_query_plan_,
        TaskToHostMapPtr task_to_host_map_)
        : ISource(std::move(header_))
        , unique_query_id(unique_query_id_)
        , distributed_query_plan(std::move(distributed_query_plan_))
        , task_to_host_map(std::move(task_to_host_map_))
    {
    }

    String getName() const override { return "ReadFromDistributedPlanSource"; }

    Status prepare() override;

#if defined(OS_LINUX) || defined(OS_DARWIN)
    std::tuple<int, uint32_t, Int64> scheduleForEvent() override;
    void onAsyncJobReady() override;
#endif

private:
    std::optional<Chunk> tryGenerate() override;
    void onCancel() noexcept override;

    /// Tear down the executor once. Must be called with `executor_mutex` held.
    void cleanupLocked();

    const UUID unique_query_id;
    const DistributedQueryPlan distributed_query_plan;
    TaskToHostMapPtr task_to_host_map;

    /// Guards the executor lifecycle (create/start/execute/cleanup) so that `onCancel`,
    /// which may run on another thread, never races with `tryGenerate`.
    std::mutex executor_mutex;
    std::unique_ptr<DistributedQueryPlanExecutor> distributed_query_executor;
    bool started = false;
    bool cleaned_up = false;

    /// Set from `onCancel` (and observed by the executor) to stop remote work promptly. The executor
    /// also records a failing task's exception here, so `tryGenerate` can tell a plain cancellation
    /// from a failure and report the latter.
    DistributedQueryCancellationPtr cancellation = std::make_shared<DistributedQueryCancellation>();

    /// The executor notifies this every time a task reaches a terminal state, and `onCancel` notifies
    /// it too, so a parked source wakes as soon as `execute` can make progress.
    StageWakeupPtr stage_wakeup = std::make_shared<WakeupFd>();

#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// This source only dispatches the plan's stages and waits for them; the query result arrives
    /// through a second source of the same pipeline. On a streaming exchange nothing runs until
    /// that second source connects to the `main` task's sink, and a sink without a connection does
    /// not ask its input for data, so the whole plan is blocked on it. Waiting inside `work` would
    /// hold an execution thread, and a pipeline that has only one of them would never reach the
    /// result source. So park in the executor's async queue instead and let it re-dispatch us.
    ///
    /// The park ends on `stage_wakeup`, and this interval is only a backstop for a state change that
    /// does not notify it.
    static constexpr Int64 stage_poll_interval_ms = 100;
    /// True while the last `tryGenerate` left the stages running, i.e. there is nothing to do until
    /// the wake-up arrives. Reset in `onAsyncJobReady`, right before the re-dispatch calls `work`.
    bool waiting_for_stages = false;
#endif
};

}
