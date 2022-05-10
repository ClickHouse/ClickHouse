#pragma once

#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/RedisStreams/StorageRedisStreams.h>


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
        size_t max_block_size_,
        bool ack_on_select_);
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

    /// Determines if we should acknowledge messages during select
    const bool ack_on_select;

    ConsumerBufferPtr buffer;
    bool read_nothing = true;
    bool is_finished = false;

    const Block non_virtual_header;
    const Block virtual_header;

    Chunk generateImpl();
};

}
