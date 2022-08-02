#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/NATS/StorageNATS.h>


namespace DB
{

class IOutputFormat;
using IOutputFormatPtr = std::shared_ptr<IOutputFormat>;

class NATSSink : public SinkToStorage
{
public:
    explicit NATSSink(StorageNATS & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_, ProducerBufferPtr buffer_);

    void onStart() override;
    void consume(Chunk chunk) override;
    void onFinish() override;
    void onException() override;
    void onCancel() override;

    String getName() const override { return "NATSSink"; }

private:
    void finalize();

    StorageNATS & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    ProducerBufferPtr buffer;
    IOutputFormatPtr format;

    std::mutex cancel_mutex;
    bool cancelled = false;
};
}
