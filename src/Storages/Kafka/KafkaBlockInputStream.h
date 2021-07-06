#pragma once

#include <DataStreams/IBlockInputStream.h>

#include <Storages/Kafka/StorageKafka.h>
#include <Storages/Kafka/ReadBufferFromKafkaConsumer.h>


namespace Poco
{
    class Logger;
}
namespace DB
{

class KafkaBlockInputStream : public IBlockInputStream
{
public:
    KafkaBlockInputStream(
        StorageKafka & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::shared_ptr<Context> & context_,
        const Names & columns,
        Poco::Logger * log_,
        size_t max_block_size_,
        bool commit_in_suffix = true);
    ~KafkaBlockInputStream() override;

    String getName() const override { return storage.getName(); }
    Block getHeader() const override;

    void readPrefixImpl() override;
    Block readImpl() override;
    void readSuffixImpl() override;

    void commit();
    bool isStalled() const { return !buffer || buffer->isStalled(); }

private:
    StorageKafka & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Names column_names;
    Poco::Logger * log;
    UInt64 max_block_size;

    ConsumerBufferPtr buffer;
    bool broken = true;
    bool finished = false;
    bool commit_in_suffix;

    const Block non_virtual_header;
    const Block virtual_header;
    const HandleKafkaErrorMode handle_error_mode;
};

}
