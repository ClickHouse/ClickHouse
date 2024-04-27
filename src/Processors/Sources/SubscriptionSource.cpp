#include <optional>

#include <Common/logger_useful.h>

#include <Processors/Sources/SubscriptionSource.h>

#include <Storages/Streaming/Subscription.h>

namespace DB
{

SubscriptionSource::SubscriptionSource(Block storage_sample_, StreamSubscriptionPtr subscription_)
    : ISource(std::move(storage_sample_)), subscription(std::move(subscription_)), fd(subscription->fd())
{
}

IProcessor::Status SubscriptionSource::prepare()
{
    if (finished)
        return Status::Finished;

    if (is_async_state)
        return Status::Async;

    auto base_status = ISource::prepare();

    if (base_status == Status::Finished)
        finished = true;

    return base_status;
}

std::optional<Chunk> SubscriptionSource::tryGenerate()
{
    is_async_state = false;

    if (isCancelled() || finished)
        return std::nullopt;

    if (cached_data.empty())
    {
        if (fd.has_value() && subscription->isEmpty())
        {
            is_async_state = true;
            return Chunk();
        }

        LOG_DEBUG(log, "extracting new batch");
        auto new_blocks = subscription->extractAll();
        cached_data.splice(cached_data.end(), new_blocks);
    }

    LOG_DEBUG(log, "cached blocks size: {}", cached_data.size());

    if (!cached_data.empty())
    {
        Block new_block = std::move(cached_data.front());
        cached_data.pop_front();
        return ProjectBlock(std::move(new_block));
    }

    return Chunk();
}

int SubscriptionSource::schedule()
{
    chassert(fd.has_value());
    LOG_DEBUG(log, "waiting on descriptor: {}", fd.value());
    return fd.value();
}

void SubscriptionSource::onUpdatePorts()
{
    if (getPort().isFinished())
    {
        LOG_DEBUG(log, "output port is finished, disabling subscription");
        finished = true;
        subscription->disable();
    }
}

void SubscriptionSource::onCancel()
{
    LOG_DEBUG(log, "query is cancelled, disabling subscription");
    finished = true;
    subscription->disable();
}

Chunk SubscriptionSource::ProjectBlock(Block block)
{
    Block metadata_block = block.cloneEmpty();

    /// if stream changed, we must recalculate converting actions
    if (!blocksHaveEqualStructure(subscription_stream_metadata, metadata_block))
    {
        LOG_INFO(log, "Recalculating converting actions for new metadata: {}", metadata_block.dumpStructure());

        auto convert_actions_dag = ActionsDAG::makeConvertingActions(
            metadata_block.getColumnsWithTypeAndName(),
            getPort().getHeader().getColumnsWithTypeAndName(),
            ActionsDAG::MatchColumnsMode::Name);

        subscription_stream_metadata = std::move(metadata_block);
        stream_converter = std::make_shared<ExpressionActions>(std::move(convert_actions_dag));
    }

    chassert(stream_converter != nullptr);
    stream_converter->execute(block);

    return Chunk(block.getColumns(), block.rows());
}

}
