#include <optional>

#include <Common/logger_useful.h>

#include <Processors/Sources/SubscriptionSource.h>

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
    if (base_status == Status::Ready && subscriber_chunks.empty() && fd.has_value())
        return Status::Async;

    return base_status;
}

std::optional<Chunk> SubscriptionSource::tryGenerate()
{
    if (isCancelled())
        return std::nullopt;

    if (subscriber_chunks.empty())
    {
        LOG_DEBUG(log, "extracting new chunk batch");
        auto new_chunks = subscription->extractAll();
        subscriber_chunks.splice(subscriber_chunks.end(), new_chunks);
    }

    LOG_DEBUG(log, "cached chunks size: {}", subscriber_chunks.size());

    if (!subscriber_chunks.empty())
    {
        Chunk new_chunk = std::move(subscriber_chunks.front());
        subscriber_chunks.pop_front();
        return new_chunk;
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
        subscription->cancel();
    }
}

void SubscriptionSource::onCancel()
{
    LOG_DEBUG(log, "query is cancelled, disabling subscription");
    subscription->cancel();
}

}
