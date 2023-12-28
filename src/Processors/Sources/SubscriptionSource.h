#pragma once

#include <optional>

#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>

#include <Storages/SubscriptionQueue.h>

namespace DB
{

/// Source from Streaming Subscription.
class SubscriptionSource final : public ISource
{
public:
    SubscriptionSource(Block storage_sample_, SubscriberPtr subscriber_);
    ~SubscriptionSource() override = default;

    String getName() const override { return "Subscription"; }

    Status prepare() override;
    int schedule() override;

protected:
    std::optional<Chunk> tryGenerate() override;

    void onCancel() override;

private:
    SubscriberPtr subscriber;
    std::optional<int> fd;

    std::list<Chunk> subscriber_chunks;
};

}
