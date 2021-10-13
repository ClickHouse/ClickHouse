#pragma once

#include <DataStreams/IBlockOutputStream.h>
#include <Storages/Kafka/StorageKafka.h>

namespace DB
{

class KafkaBlockOutputStream : public IBlockOutputStream
{
public:
    explicit KafkaBlockOutputStream(
        StorageKafka & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::shared_ptr<const Context> & context_);

    Block getHeader() const override;

    void writePrefix() override;
    void write(const Block & block) override;
    void writeSuffix() override;

    void flush() override;

private:
    StorageKafka & storage;
    StorageMetadataPtr metadata_snapshot;
    const std::shared_ptr<const Context> context;
    ProducerBufferPtr buffer;
    BlockOutputStreamPtr child;
};

}
