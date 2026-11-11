#pragma once
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/PatchParts/PatchPartsLock.h>

namespace DB
{

class StorageMergeTree;

class MergeTreeSinkPatch final : public MergeTreeSink
{
public:
    MergeTreeSinkPatch(
        StorageMergeTree & storage_,
        PatchPartMetadata patch_metadata_,
        PlainLightweightUpdateHolder update_holder_,
        ContextPtr context_);

    String getName() const override { return "MergeTreeSinkPatch"; }

protected:
    PlainLightweightUpdateHolder update_holder;
    PatchPartMetadata patch_metadata;

    void finishDelayedChunk() override;
    TemporaryPartPtr writeNewTempPart(BlockWithPartition & block) override;
};

}
