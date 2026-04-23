#include <Storages/MergeTree/Streaming/MergeTreeCommitOrderSequentialSource.h>

#include <Common/assert_cast.h>
#include <Core/Block.h>
#include <Processors/Port.h>
#include <QueryPipeline/Pipe.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/Streaming/CommitOrderStrategy.h>

namespace DB
{

MergeTreeCommitOrderSequentialSource::MergeTreeCommitOrderSequentialSource(
    SharedHeader header_,
    const MergeTreeData & storage_,
    StorageSnapshotPtr storage_snapshot_,
    Names columns_to_read_,
    RangesInDataPartStreamSubscriptionPtr subscription_,
    ContextPtr context_)
    : IProcessor({}, {Block(*header_)})
    , header(std::move(header_))
    , storage(storage_)
    , storage_snapshot(std::move(storage_snapshot_))
    , columns_to_read(std::move(columns_to_read_))
    , subscription(std::move(subscription_))
    , context(std::move(context_))
{
}

bool MergeTreeCommitOrderSequentialSource::fillPendingFromSubscription()
{
    if (!pending.empty())
        return true;

    /// Only call `extractAll` when the queue is non-empty or disabled — otherwise it
    /// blocks on the eventfd read. Phase D never reaches the "empty & not disabled"
    /// state because the subscription is disabled right after seeding.
    if (subscription->isEmpty())
        return subscription->isDisabled() ? false : false;

    pending = subscription->extractAll();
    return !pending.empty();
}

IProcessor::Status MergeTreeCommitOrderSequentialSource::prepare()
{
    auto & output = outputs.front();

    if (output.isFinished())
    {
        subscription->disable();
        return Status::Finished;
    }

    /// Post-splice cycle hasn't been scheduled yet.
    if (need_update_pipeline)
    {
        if (!fillPendingFromSubscription())
        {
            /// Nothing to do, nothing coming — flush out.
            if (current_sub_pipeline.empty())
            {
                output.finish();
                return Status::Finished;
            }
            /// Schedule one last cycle to remove the finished sub-pipeline.
            return Status::UpdatePipeline;
        }

        return Status::UpdatePipeline;
    }

    if (!output.canPush())
        return Status::PortFull;

    chassert(!inputs.empty());
    auto & input = inputs.back();

    if (input.isFinished())
    {
        /// Current sub-pipeline drained. Schedule teardown + next splice.
        need_update_pipeline = true;
        return Status::UpdatePipeline;
    }

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    output.push(input.pull(/*set_not_needed=*/ true));
    return Status::PortFull;
}

IProcessor::PipelineUpdate MergeTreeCommitOrderSequentialSource::updatePipeline()
{
    PipelineUpdate update;

    /// Tear down the previous cycle's sub-pipeline (if any).
    if (!current_sub_pipeline.empty())
    {
        chassert(!inputs.empty());
        auto & input = inputs.back();
        if (input.isConnected())
            disconnect(input.getOutputPort(), input);
        update.to_remove = std::exchange(current_sub_pipeline, {});
    }

    need_update_pipeline = false;

    /// Exhausted and no new parts → nothing to splice in.
    if (pending.empty())
    {
        outputs.front().finish();
        return update;
    }

    auto ranges = std::move(pending.front());
    pending.pop_front();

    auto strategy = chooseCommitOrderReadStrategy(ranges, *storage_snapshot->metadata);

    const auto & snapshot_data = assert_cast<const MergeTreeData::SnapshotData &>(*storage_snapshot->data);
    auto alter_conversions = MergeTreeData::getAlterConversionsForPart(
        ranges.data_part, snapshot_data.mutations_snapshot, context);

    Pipe sub_pipe = createCommitOrderReadStream(
        storage,
        storage_snapshot,
        std::move(alter_conversions),
        columns_to_read,
        strategy,
        context);

    chassert(sub_pipe.numOutputPorts() == 1);
    auto * sub_output = sub_pipe.getOutputPort(0);
    auto sub_processors = Pipe::detachProcessors(std::move(sub_pipe));

    /// Reuse the input-port slot across cycles if present; otherwise create it.
    if (inputs.empty())
        inputs.emplace_back(*header, this);

    auto & input = inputs.back();
    connect(*sub_output, input);
    input.reopen();
    input.setNeeded();

    current_sub_pipeline = sub_processors;
    for (auto & proc : sub_processors)
        update.to_add.push_back(std::move(proc));

    return update;
}

void MergeTreeCommitOrderSequentialSource::onCancel() noexcept
{
    subscription->disable();
}

}
