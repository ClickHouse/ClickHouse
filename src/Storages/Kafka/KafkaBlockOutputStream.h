#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/Kafka/StorageKafka.h>

namespace DB
{

class KafkaSink : public SinkToStorage
{
public:
    explicit KafkaSink(
        StorageKafka & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::shared_ptr<const Context> & context_);

    void consume(Chunk chunk) override;
    void onStart() override;
    void onFinish() override;
    String getName() const override { return "KafkaSink"; }

    ///void flush() override;

private:
    StorageKafka & storage;
    StorageMetadataPtr metadata_snapshot;
    const std::shared_ptr<const Context> context;
    ProducerBufferPtr buffer;
    BlockOutputStreamPtr child;
};

}
