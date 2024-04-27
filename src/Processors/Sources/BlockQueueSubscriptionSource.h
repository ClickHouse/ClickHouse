#pragma once

#include <Interpreters/ExpressionActions.h>

#include <Processors/Sources/QueueSubscriptionSourceAdapter.h>

#include <Storages/Streaming/DynamicBlockTransformer.h>

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

private:
    StreamSubscriptionPtr subscription_holder;
    DynamicBlockTransformer transformer;
};

}
