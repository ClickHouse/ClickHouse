#pragma once
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/PatchParts/PatchPartsLock.h>

namespace DB
{

class StorageMergeTree;

class MergeTreeSinkPatch : public MergeTreeSink
{
public:
    MergeTreeSinkPatch(
        StorageMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        PlainLightweightUpdateHolder update_holder_,
        ContextPtr context_);

    String getName() const override { return "MergeTreeSinkPatch"; }

protected:
    PlainLightweightUpdateHolder update_holder;

    void finishDelayedChunk() override;
    TemporaryPartPtr writeNewTempPart(BlockWithPartition & block) override;
};

}
