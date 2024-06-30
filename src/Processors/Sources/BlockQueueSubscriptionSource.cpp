#include <Processors/Sources/BlockQueueSubscriptionSource.h>

namespace DB
{

BlockQueueSubscriptionSource::BlockQueueSubscriptionSource(Block storage_sample_, StreamSubscriptionPtr subscription_)
    : QueueSubscriptionSourceAdapter<Block>(std::move(storage_sample_), *subscription_->as<QueueStreamSubscription<Block>>(), getName())
    , subscription_holder(std::move(subscription_))
    , transformer(getPort().getHeader())
{
}

Chunk BlockQueueSubscriptionSource::useCachedData()
{
    if (cached_data.empty())
    {
        need_new_data = true;
        return Chunk();
    }

    Block new_block = std::move(cached_data.front());
    cached_data.pop_front();

    if (cached_data.empty())
        need_new_data = true;

    transformer.transform(new_block);
    return Chunk(new_block.getColumns(), new_block.rows());
}

}
