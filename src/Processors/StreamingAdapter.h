#pragma once

#include <optional>
#include <Core/SortDescription.h>
#include <Processors/IProcessor.h>
#include <Processors/RowsBeforeLimitCounter.h>
#include <Storages/SubscriptionQueue.h>

namespace DB
{

class StreamingAdapter final : public IProcessor
{
public:
    StreamingAdapter(const Block & header_, size_t num_streams, Block sample, SubscriberPtr sub);

    String getName() const override { return "StreamingAdapter"; }

    Status prepare() override;
    void work() override;
    int schedule() override;

    void onCancel() override;

private:
    enum class PortsDataState
    {
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

    Chunk FilterStorageChunk(Chunk chunk, const Block& header);

    void updateState(PortsData & data);
    Status prepareStoragePair(PortsData & data);
    Status prepareSubscriptionPair(PortsData & data);

    Block storage_sample;
    SubscriberPtr subscriber;
    std::list<Chunk> subscriber_chunks;

    std::optional<int> fd;
    std::vector<PortsData> ports_data;
};

}
