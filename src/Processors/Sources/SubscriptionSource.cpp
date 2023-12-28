#include <optional>

#include <Common/logger_useful.h>

#include <Processors/Sources/SubscriptionSource.h>

namespace DB
{

SubscriptionSource::SubscriptionSource(Block storage_sample_, SubscriberPtr subscriber_)
    : ISource(std::move(storage_sample_)), subscriber(std::move(subscriber_)), fd(subscriber->fd())
{
}

IProcessor::Status SubscriptionSource::prepare()
{
    if (isCancelled())
    {
        getPort().finish();
        return Status::Finished;
    }

    if (!has_input && subscriber_chunks.empty() && fd.has_value())
        return Status::Async;

    return ISource::prepare();
}

std::optional<Chunk> SubscriptionSource::tryGenerate()
{
    if (isCancelled())
        return std::nullopt;

    if (subscriber_chunks.empty())
    {
        LOG_DEBUG(&Poco::Logger::get("SubscriptionSource"), "extracting new chunk batch");
        auto new_chunks = subscriber->extractAll();
        subscriber_chunks.splice(subscriber_chunks.end(), new_chunks);
    }

    LOG_DEBUG(&Poco::Logger::get("SubscriptionSource"), "cached chunks size: {}", subscriber_chunks.size());

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
    LOG_DEBUG(&Poco::Logger::get("SubscriptionSource"), "waiting on descriptor: {}", fd.value());
    return fd.value();
}

void SubscriptionSource::onCancel()
{
    LOG_DEBUG(&Poco::Logger::get("SubscriptionSource"), "cancelling subscription");
    subscriber->cancel();
}

}
