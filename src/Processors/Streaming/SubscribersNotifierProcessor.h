#pragma once

#include <optional>

#include <Processors/IProcessor.h>
#include <Storages/Streaming/SubscriptionManager.h>

namespace DB
{

class SubscribersNotifierProcessor final : public IProcessor
{
public:
    SubscribersNotifierProcessor(const Block & header_, StreamSubscriptionManager & subscription_manager_);

    String getName() const override { return "SubscribersNotifierProcessor"; }

    Status prepare() override;
    void work() override;

private:
    InputPort & input;
    OutputPort & output;

    StreamSubscriptionManager & subscription_manager;
    std::optional<Chunk> subscriber_chunk;
};

}
