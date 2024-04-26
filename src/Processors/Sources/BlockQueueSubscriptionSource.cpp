#include <Processors/Sources/BlockQueueSubscriptionSource.h>

namespace DB
{

BlockQueueSubscriptionSource::BlockQueueSubscriptionSource(Block storage_sample_, StreamSubscriptionPtr subscription_)
    : QueueSubscriptionSourceAdapter<Block>(std::move(storage_sample_), *subscription_->as<QueueStreamSubscription<Block>>(), getName())
    , subscription_holder(std::move(subscription_))
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

    return ProjectBlock(std::move(new_block));
}

Chunk BlockQueueSubscriptionSource::ProjectBlock(Block block)
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
