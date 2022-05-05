#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/RedisStreams/StorageRedisStreams.h>

namespace DB
{

class IOutputFormat;
using IOutputFormatPtr = std::shared_ptr<IOutputFormat>;

class RedisStreamsSink : public SinkToStorage
{
public:
    explicit RedisStreamsSink(
        StorageRedisStreams & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const ContextPtr & context_);

    void consume(Chunk chunk) override;
    void onStart() override;
    void onFinish() override;
    String getName() const override { return "RedisStreamsSink"; }

    ///void flush() override;

private:
    StorageRedisStreams & storage;
    StorageMetadataPtr metadata_snapshot;
    const ContextPtr context;
    ProducerBufferPtr buffer;
    IOutputFormatPtr format;
};

}
