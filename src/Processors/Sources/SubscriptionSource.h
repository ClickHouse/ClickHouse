#pragma once

#include <optional>

#include <Processors/ISource.h>
#include <QueryPipeline/Pipe.h>

#include <Storages/Streaming/Subscription.h>

namespace DB
{

/// Source from Streaming Subscription.
class SubscriptionSource final : public ISource
{
public:
    SubscriptionSource(Block storage_sample_, StreamSubscriptionPtr subscription_);
    ~SubscriptionSource() override = default;

    String getName() const override { return "Subscription"; }

    Status prepare() override;
    int schedule() override;

protected:
    std::optional<Chunk> tryGenerate() override;

    void onCancel() override;

private:
    StreamSubscriptionPtr subscription;
    std::optional<int> fd;

    std::list<Chunk> subscriber_chunks;
};

}
