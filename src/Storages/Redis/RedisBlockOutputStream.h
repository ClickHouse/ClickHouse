#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/Redis/StorageRedis.h>

namespace DB
{

class IOutputFormat;
using IOutputFormatPtr = std::shared_ptr<IOutputFormat>;

class RedisSink : public SinkToStorage
{
public:
    explicit RedisSink(
        StorageRedis & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const ContextPtr & context_);

    void consume(Chunk chunk) override;
    void onStart() override;
    void onFinish() override;
    String getName() const override { return "RedisSink"; }

    ///void flush() override;

private:
    StorageRedis & storage;
    StorageMetadataPtr metadata_snapshot;
    const ContextPtr context;
    ProducerBufferPtr buffer;
    IOutputFormatPtr format;
};

}
