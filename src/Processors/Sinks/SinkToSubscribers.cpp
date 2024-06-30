#include <Processors/Sinks/SinkToSubscribers.h>

#include <Storages/Streaming/QueueStreamSubscription.h>

namespace DB
{

SinkToSubscribers::SinkToSubscribers(const Block & header_, StreamSubscriptionManager & subscription_manager_)
    : SinkToStorage(header_), subscription_manager{subscription_manager_}
{
}

void SinkToSubscribers::consume(Chunk chunk)
{
    Block block = getHeader().cloneWithColumns(chunk.detachColumns());

    auto single_push = [&block](const StreamSubscriptionPtr & subscription) { subscription->as<QueueStreamSubscription<Block>>()->push(block); };

    subscription_manager.executeOnEachSubscription(single_push);
}

}
