#pragma once

#include <Processors/Sources/SourceWithProgress.h>

#include <Storages/RedisStreams/StorageRedisStreams.h>
#include <Storages/RedisStreams/ReadBufferFromRedisStreams.h>


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
        StorageRedisStreams & storage_,
        const StorageSnapshotPtr & storage_snapshot,
        const ContextPtr & context_,
        const Names & columns,
        Poco::Logger * log_,
        size_t max_block_size_);
    ~RedisStreamsSource() override;

    String getName() const override { return storage.getName(); }

    Chunk generate() override;

    void ack();
    bool isStalled() const { return !buffer || read_nothing; }

private:
    StorageRedisStreams & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    Poco::Logger * log;
    UInt64 max_block_size;

    ConsumerBufferPtr buffer;
    bool read_nothing = true;
    bool is_finished = false;

    const Block non_virtual_header;
    const Block virtual_header;

    Chunk generateImpl();
};

}
