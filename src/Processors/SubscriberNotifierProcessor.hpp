#pragma once

#include <optional>

#include <Processors/IProcessor.h>
#include <Storages/SubscriptionQueue.hpp>

namespace DB
{

class SubscriberNotifierProcessor final : public IProcessor
{
public:
    SubscriberNotifierProcessor(const Block & header_, size_t num_streams, SubscriptionQueue& queue);

    String getName() const override { return "SubscriberNotifierProcessor"; }

    Status prepare() override;
    void work() override;

private:
    InputPort & input;
    OutputPort & output;

    SubscriptionQueue& subscription_queue;
    std::optional<Chunk> subscriber_chunk;
};

}
