#include <Processors/Sinks/SinkToSubscribers.h>

namespace DB
{

SinkToSubscribers::SinkToSubscribers(const Block & header_, StreamSubscriptionManager & subscription_manager_)
    : SinkToStorage(header_)
    , subscription_manager{subscription_manager_} {}

void SinkToSubscribers::consume(Chunk chunk)
{
    Block block = getHeader().cloneWithColumns(chunk.detachColumns());
    subscription_manager.pushToAll(std::move(block));
}

}
