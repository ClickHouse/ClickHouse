#pragma once

#include <Processors/Sinks/SinkToStorage.h>

#include <Storages/Streaming/SubscriptionManager.h>

namespace DB
{

class SinkToSubscribers final : public SinkToStorage
{
public:
    SinkToSubscribers(const Block & header_, StreamSubscriptionManager & subscription_manager_);

    String getName() const override { return "SinkToSubscribers"; }

    void consume (Chunk chunk) override;

private:
    StreamSubscriptionManager & subscription_manager;
};

}
