#pragma once

#include <Processors/Sinks/SinkToStorage.h>
#include <Storages/StorageInMemoryMetadata.h>


namespace DB
{

class Block;
class StorageMergeTree;


class MergeTreeSink : public SinkToStorage
{
public:
    MergeTreeSink(
        StorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        size_t max_parts_per_block_,
        ContextPtr context_);

    ~MergeTreeSink() override;

    String getName() const override { return "MergeTreeSink"; }
    void consume(Chunk chunk) override;
    void onStart() override;
    void onFinish() override;

private:
    StorageMergeTree & storage;
    StorageMetadataPtr metadata_snapshot;
    size_t max_parts_per_block;
    ContextPtr context;
    uint64_t chunk_dedup_seqnum = 0; /// input chunk ordinal number in case of dedup token

    /// We can delay processing for previous chunk and start writing a new one.
    struct DelayedChunk;
    std::unique_ptr<DelayedChunk> delayed_chunk;

    void finishDelayedChunk();
};

}
