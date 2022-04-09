#pragma once

#include <Processors/Sources/SourceWithProgress.h>

#include <Storages/Redis/StorageRedis.h>
#include <Storages/Redis/ReadBufferFromRedisConsumer.h>


namespace Poco
{
    class Logger;
}
namespace DB
{

class RedisStreamsSource : public SourceWithProgress
{
public:
    RedisStreamsSource(
        StorageRedis & storage_,
        const StorageMetadataPtr & metadata_snapshot_,
        const ContextPtr & context_,
        const Names & columns,
        Poco::Logger * log_,
        size_t max_block_size_);
    ~RedisStreamsSource() override;

    String getName() const override { return storage.getName(); }

    Chunk generate() override;

    void commit();
    bool isStalled() const { return !buffer || read_nothing; }

private:
    StorageRedis & storage;
    StorageMetadataPtr metadata_snapshot;
    ContextPtr context;
    Names column_names;
    Poco::Logger * log;
    UInt64 max_block_size;

    ConsumerBufferPtr buffer;
    bool read_nothing = true;
    bool is_finished = false;
    bool commit_in_suffix;

    const Block non_virtual_header;
    const Block virtual_header;

    Chunk generateImpl();
};

}
