#include <Processors/Transforms/AdaptiveAggregationCoalescingTransform.h>
#include <Processors/Transforms/AggregatingTransform.h>
#include <Common/MemoryTrackerSwitcher.h>
#include <Common/MemoryTrackerUtils.h>
#include <Interpreters/AdaptiveAggregationChunkInfo.h>
#include <Interpreters/AdaptiveAggregationImpl.h>

namespace DB
{

AdaptiveAggregationCoalescingTransform::AdaptiveAggregationCoalescingTransform(
    std::shared_ptr<AggregatingTransformParams> params_, AdaptiveAggregationSessionPtr session_)
    : IProcessor({params_->aggregator.getAdaptiveStagedHeader()}, {params_->aggregator.getAdaptiveStagedHeader()})
    , params(std::move(params_))
    , session(std::move(session_))
    , coalescer(params->aggregator.hasAdaptiveCountPayload())
{
}

IProcessor::Status AdaptiveAggregationCoalescingTransform::prepare()
{
    auto & input = inputs.front();
    auto & output = outputs.front();
    if (isCancelled() || output.isFinished())
    {
        session->cancel();
        input.close();
        if (input.hasData())
            input.pullData(/*set_not_needed=*/true);
        current_chunk.clear();
        ready_chunk.clear();
        coalescer = StagedChunkCoalescer(params->aggregator.hasAdaptiveCountPayload());
        return Status::Finished;
    }

    /// A publisher acknowledges only after registering its input. In particular, pushing a chunk
    /// must not also renew this transform's input demand in the same call.
    if (!output.canPush())
        return Status::PortFull;
    if (ready_chunk)
    {
        output.push(std::move(ready_chunk));
        return Status::PortFull;
    }
    if (has_input || flush_pending)
        return Status::Ready;
    if (input_finished)
    {
        output.finish();
        return Status::Finished;
    }
    if (input.isFinished())
    {
        input_finished = true;
        flush_pending = true;
        use_own_memory_tracker = false;
        return Status::Ready;
    }
    input.setNeeded();
    if (!input.hasData())
        return Status::NeedData;
    current_chunk = input.pull(/*set_not_needed=*/true);
    has_input = true;
    return Status::Ready;
}

void AdaptiveAggregationCoalescingTransform::work()
{
    if (has_input)
        use_own_memory_tracker = current_chunk.getChunkInfos().getSafe<StagedKeysInfo>()->use_own_memory_tracker;
    std::optional<MemoryTrackerSwitcher> memory_tracker_switcher;
    if (use_own_memory_tracker)
        memory_tracker_switcher.emplace(params->aggregator.getMemoryTracker());

    if (flush_pending)
    {
        ready_chunk = coalescer.flush();
        flush_pending = false;
    }
    else
    {
        chassert(has_input);
        if (current_chunk.getNumRows())
            ready_chunk = coalescer.add(std::move(current_chunk));
        current_chunk.clear();
        has_input = false;
        const auto threshold = params->params.max_bytes_before_external_group_by;
        flush_pending = !coalescer.empty() && threshold && getCurrentQueryMemoryUsage() > static_cast<Int64>(threshold);
    }
    if (ready_chunk)
        ready_chunk.getChunkInfos().get<StagedKeysInfo>()->use_own_memory_tracker = use_own_memory_tracker;
}

void AdaptiveAggregationCoalescingTransform::onCancel() noexcept
{
    session->cancel();
}

}
