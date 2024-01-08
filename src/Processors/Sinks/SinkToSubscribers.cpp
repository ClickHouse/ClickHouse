#include <Processors/Sinks/SinkToSubscribers.h>

namespace DB
{

SinkToSubscribers::SinkToSubscribers(const Block & header_, StreamSubscriptionManager & subscription_manager_)
    : SinkToStorage(header_)
    , subscription_manager{subscription_manager_} {}

void SinkToSubscribers::consume(Chunk chunk)
{
    subscription_manager.pushChunk(std::move(chunk));
}

}
