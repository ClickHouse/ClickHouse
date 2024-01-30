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
    Status base_status = ISource::prepare();

    // if needs to generate new chunk and we have not any cached data
    // fallback to Async to wait on subscription if possible
    if (base_status == Status::Ready && cached_data.empty() && fd.has_value())
        return Status::Async;

    return base_status;
}

std::optional<Chunk> SubscriptionSource::tryGenerate()
{
    if (isCancelled())
        return std::nullopt;

    if (cached_data.empty())
    {
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
        subscription->disable();
    }
}

void SubscriptionSource::onCancel()
{
    LOG_DEBUG(log, "query is cancelled, disabling subscription");
    subscription->disable();
}

Chunk SubscriptionSource::ProjectBlock(Block block) const
{
    const Block& header = getPort().getHeader();
    Block projection;

    for (const auto& header_column : header.getColumnsWithTypeAndName())
        projection.insert(block.getByName(header_column.name));

    return Chunk(projection.getColumns(), projection.rows());
}

}
