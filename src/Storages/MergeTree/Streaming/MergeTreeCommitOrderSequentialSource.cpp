#include <Storages/MergeTree/Streaming/MergeTreeCommitOrderSequentialSource.h>
#include <Storages/MergeTree/RangesInDataPart.h>

#include <QueryPipeline/Pipe.h>

#include <Processors/Port.h>

#include <Core/Block.h>

#include <Common/logger_useful.h>

namespace DB
{

MergeTreeCommitOrderSequentialSource::MergeTreeCommitOrderSequentialSource(
    SharedHeader header_,
    CommitOrderReadStrategyPtr read_strategy_,
    RangesInDataPartStreamSubscriptionPtr subscription_)
    : IProcessor({}, {Block(*header_)})
    , header(std::move(header_))
    , read_strategy(std::move(read_strategy_))
    , subscription(std::move(subscription_))
    , log(getLogger("MergeTreeCommitOrderSequentialSource"))
{
}

IProcessor::Status MergeTreeCommitOrderSequentialSource::prepare()
{
    auto & output = outputs.front();

    if (output.isFinished())
        return Status::Finished;

    if (!output.canPush())
        return Status::PortFull;

    const bool need_build_new_pipeline = inputs.empty() || (!inputs.front().hasData() && inputs.front().isFinished());
    if (need_build_new_pipeline)
    {
        if (!subscription->isEmpty())
            pending.splice(pending.end(), subscription->extractAll());

        if (!pending.empty())
            return Status::UpdatePipeline;

        if (subscription->isDisabled())
        {
            output.finish();
            return Status::Finished;
        }

        return subscription->fd().has_value() ? Status::Async : Status::Ready;
    }

    chassert(!inputs.empty() && !inputs.front().isFinished());
    auto & input = inputs.front();

    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    output.push(input.pull(/*set_not_needed=*/true));
    return Status::PortFull;
}

void MergeTreeCommitOrderSequentialSource::work()
{
    chassert(!subscription->fd().has_value());
    pending.splice(pending.end(), subscription->extractAll());
}

int MergeTreeCommitOrderSequentialSource::schedule()
{
    auto fd = subscription->fd();
    chassert(fd.has_value());
    return *fd;
}

IProcessor::PipelineUpdate MergeTreeCommitOrderSequentialSource::updatePipeline()
{
    PipelineUpdate update;

    /// Tear down the previous cycle's sub-pipeline (if any).
    if (!current_sub_pipeline.empty())
    {
        chassert(!inputs.empty());
        chassert(inputs.front().isConnected());

        auto & input = inputs.front();
        disconnect(input.getOutputPort(), input);

        update.to_remove = std::exchange(current_sub_pipeline, {});
    }

    /// Reuse the input-port slot across cycles if present; otherwise create it.
    if (inputs.empty())
        inputs.emplace_back(*header, this);

    auto ranges = std::move(pending.front());
    Pipe sub_pipe = read_strategy->createReadStream(ranges);
    pending.pop_front();

    chassert(sub_pipe.numOutputPorts() == 1);
    auto * sub_output = sub_pipe.getOutputPort(0);
    auto sub_processors = Pipe::detachProcessors(std::move(sub_pipe));

    auto & input = inputs.front();
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
