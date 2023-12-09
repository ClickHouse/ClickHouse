#pragma once

#include <optional>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Core/SortDescription.h>
#include <Storages/SubscriptionQueue.hpp>

namespace DB
{

class StreamingAdapter final : public IProcessor
{
public:
    StreamingAdapter(const Block & header_, size_t num_streams, SubscriberPtr subscriber);

    String getName() const override { return "StreamingAdapter"; }

    Status prepare() override;
    void work() override;
    int schedule() override;

    // InputPort & getInputPort() { return inputs.front(); }
    // OutputPort & getOutputPort() { return outputs.front(); }

private:
    enum class PortsDataState {
        ReadingFromStorage,
        ReadingFromSubscription,
        Finished,
    };

    struct PortsData
    {
        std::optional<Chunk> subscriber_chunk;
        bool need_next_subscriber_chunk = false;

        InputPort * input_port = nullptr;
        OutputPort * output_port = nullptr;

        PortsDataState state = PortsDataState::ReadingFromStorage;
    };

    void updateState(PortsData& data);
    Status prepareStoragePair(PortsData& data);
    Status prepareSubscriptionPair(PortsData& data);

    SubscriberPtr subscriber;
    std::list<Chunk> subscriber_chunks;

    std::vector<PortsData> ports_data;
};

}
