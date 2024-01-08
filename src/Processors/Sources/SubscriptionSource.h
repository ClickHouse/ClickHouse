#pragma once

#include <optional>

#include <Poco/Logger.h>

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

    /// Stop reading from subscription if output port was finished.
    void onUpdatePorts() override;

    /// Stop reading from subscription if query was cancelled.
    void onCancel() override;

protected:
    std::optional<Chunk> tryGenerate() override;

private:
    StreamSubscriptionPtr subscription;
    std::optional<int> fd;

    std::list<Chunk> subscriber_chunks;

    Poco::Logger * log = &Poco::Logger::get("SubscriptionSource");
};

}
