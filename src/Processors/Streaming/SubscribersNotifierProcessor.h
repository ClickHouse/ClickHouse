#pragma once

#include <optional>

#include <Processors/IProcessor.h>
#include <Storages/SubscriptionQueue.h>

namespace DB
{

class SubscribersNotifierProcessor final : public IProcessor
{
public:
    SubscribersNotifierProcessor(const Block & header_, size_t num_streams, SubscriptionQueue & queue);

    String getName() const override { return "SubscribersNotifierProcessor"; }

    Status prepare() override;
    void work() override;

private:
    InputPort & input;
    OutputPort & output;

    SubscriptionQueue & subscription_queue;
    std::optional<Chunk> subscriber_chunk;
};

}
