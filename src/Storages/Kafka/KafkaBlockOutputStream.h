#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/Kafka/StorageKafka.h>

namespace DB
{

class IOutputFormat;
using IOutputFormatPtr = std::shared_ptr<IOutputFormat>;

class KafkaSink : public SinkToStorage
{
public:
    explicit KafkaSink(
        StorageKafka & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const ContextPtr & context_);

    void consume(Chunk chunk) override;
    void onStart() override;
    void onFinish() override;
    String getName() const override { return "KafkaSink"; }

    ///void flush() override;

private:
    StorageKafka & storage;
    StorageMetadataPtr metadata_snapshot;
    const ContextPtr context;
    ProducerBufferPtr buffer;
    IOutputFormatPtr format;
};

}
