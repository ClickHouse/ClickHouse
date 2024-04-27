#pragma once

#include <optional>

#include <Poco/Logger.h>

#include <Processors/ISource.h>

#include <QueryPipeline/Pipe.h>

#include <Interpreters/ExpressionActions.h>

#include <Storages/Streaming/Subscription_fwd.h>

namespace DB
{

/// Source from Streaming Subscription.
class SubscriptionSource final : public ISource
{
public:
    SubscriptionSource(Block storage_sample_, StreamSubscriptionPtr subscription_);
    ~SubscriptionSource() override = default;

    String getName() const override { return "SubscriptionSource"; }

    Status prepare() override;
    int schedule() override;

    /// Stop reading from subscription if output port was finished.
    void onUpdatePorts() override;

    /// Stop reading from subscription if query was cancelled.
    void onCancel() override;

protected:
    /// Converts block from subscription to output header metadata chunk.
    /// It is possible for sinks to change chunk somehow before pushing it to subscribers
    /// or it can be an alter table metadata change
    Chunk ProjectBlock(Block block);

    std::optional<Chunk> tryGenerate() override;

private:
    StreamSubscriptionPtr subscription;
    std::optional<int> fd;

    BlocksList cached_data;
    bool is_async_state = false;

    Block subscription_stream_metadata;
    ExpressionActionsPtr stream_converter;

    Poco::Logger * log = &Poco::Logger::get("SubscriptionSource");
};

}
