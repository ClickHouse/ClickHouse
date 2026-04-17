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
        bool is_v2_format_,
        ContextPtr context_);

    String getName() const override { return "MergeTreeSinkPatch"; }

protected:
    PlainLightweightUpdateHolder update_holder;
    /// True iff this sink writes v2-format patches. The sort-key expression itself is *not*
    /// carried on the sink — readers recover it from the target table's current
    /// `StorageMetadataPtr` at apply time (see `MergeTreeData::getPatchPartMetadata`). All the
    /// sink needs to do is stamp the format-version byte in the `SourcePartsSetForPatch`.
    bool is_v2_format = false;

    void finishDelayedChunk() override;
    TemporaryPartPtr writeNewTempPart(BlockWithPartition & block) override;
};

}
