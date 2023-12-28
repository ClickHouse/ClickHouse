#include <base/defines.h>

#include <Common/logger_useful.h>

#include <Processors/Streaming/SubscribersNotifierProcessor.h>

namespace DB
{

SubscribersNotifierProcessor::SubscribersNotifierProcessor(const Block & header_, size_t num_streams, SubscriptionQueue & queue)
    : IProcessor(InputPorts(num_streams, header_), OutputPorts(num_streams, header_))
    , input(inputs.front())
    , output(outputs.front())
    , subscription_queue{queue}
{
}

IProcessor::Status SubscribersNotifierProcessor::prepare()
{
    /// check ports are finished
    if (output.isFinished() || input.isFinished())
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// check can push chunk
    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// push already pushed to subscribers chunk
    if (subscriber_chunk.has_value())
    {
        output.push(std::move(subscriber_chunk.value()));
        subscriber_chunk = std::nullopt;
    }

    /// request next chunk
    if (!input.hasData())
    {
        input.setNeeded();
        return Status::NeedData;
    }

    /// got new chunk, push it to subscribers
    subscriber_chunk = input.pull(true);

    return Status::Ready;
}

void SubscribersNotifierProcessor::work()
{
    chassert(subscriber_chunk.has_value());
    subscription_queue.pushChunk(subscriber_chunk->clone());
}

}
