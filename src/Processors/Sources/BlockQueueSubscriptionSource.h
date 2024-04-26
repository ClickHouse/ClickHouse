#pragma once

#include <Interpreters/ExpressionActions.h>

#include <Processors/Sources/QueueSubscriptionSourceAdapter.h>

namespace DB
{

/// Source from Streaming Subscription.
class BlockQueueSubscriptionSource final : public QueueSubscriptionSourceAdapter<Block>
{
public:
    BlockQueueSubscriptionSource(Block storage_sample_, StreamSubscriptionPtr subscription_);
    ~BlockQueueSubscriptionSource() override = default;

    String getName() const override { return "BlockQueueSubscriptionSource"; }

protected:
    Chunk useCachedData() override;

    /// Converts block from subscription to output header metadata chunk.
    /// It is possible for sinks to change chunk somehow before pushing it to subscribers
    /// or it can be an alter table metadata change
    Chunk ProjectBlock(Block block);

private:
    StreamSubscriptionPtr subscription_holder;

    Block subscription_stream_metadata;
    ExpressionActionsPtr stream_converter;
};

}
