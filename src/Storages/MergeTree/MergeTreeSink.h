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

    struct PrevPart;
    std::unique_ptr<PrevPart> prev_part;

    void finishPrevPart();
};

}
