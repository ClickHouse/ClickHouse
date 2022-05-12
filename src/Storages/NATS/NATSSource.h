#pragma once

#include <Processors/Sources/SourceWithProgress.h>
#include <Storages/NATS/ReadBufferFromNATSConsumer.h>
#include <Storages/NATS/StorageNATS.h>


namespace DB
{

class NATSSource : public SourceWithProgress
{
public:
    NATSSource(
        StorageNATS & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        ContextPtr context_,
        const Names & columns,
        size_t max_block_size_);

    ~NATSSource() override;

    String getName() const override { return storage.getName(); }
    ConsumerBufferPtr getBuffer() { return buffer; }

    Chunk generate() override;

    bool queueEmpty() const { return !buffer || buffer->queueEmpty(); }

private:
    StorageNATS & storage;
    StorageSnapshotPtr storage_snapshot;
    ContextPtr context;
    Names column_names;
    const size_t max_block_size;

    bool is_finished = false;
    const Block non_virtual_header;
    const Block virtual_header;

    ConsumerBufferPtr buffer;

    NATSSource(
        StorageNATS & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        std::pair<Block, Block> headers,
        ContextPtr context_,
        const Names & columns,
        size_t max_block_size_);

    Chunk generateImpl();
};

}
