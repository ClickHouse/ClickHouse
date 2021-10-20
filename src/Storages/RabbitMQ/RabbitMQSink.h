#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/RabbitMQ/StorageRabbitMQ.h>


namespace DB
{

class IOutputFormat;
using IOutputFormatPtr = std::shared_ptr<IOutputFormat>;

class RabbitMQSink : public SinkToStorage
{

public:
    explicit RabbitMQSink(StorageRabbitMQ & storage_, const StorageMetadataPtr & metadata_snapshot_, ContextPtr context_);

    void onStart() override;
    void consume(Chunk chunk) override;
    void onFinish() override;

    String getName() const override { return "RabbitMQSink"; }

private:
    StorageRabbitMQ & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    ProducerBufferPtr buffer;
    IOutputFormatPtr format;
};
}
