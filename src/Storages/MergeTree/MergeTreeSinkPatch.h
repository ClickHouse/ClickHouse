#pragma once
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/PatchParts/PatchPartsLock.h>

namespace DB
{

class StorageMergeTree;

class MergeTreeSinkPatch : public MergeTreeSink
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
    /// Format version + patch `StorageMetadataPtr` + (for v2) the sort-key prefix size captured at
    /// the UPDATE's callsite — bundled by `MergeTreeData::getPatchPartMetadata` so the sink can't
    /// be constructed with a patch metadata that disagrees with its sort-key shape. For v1
    /// `sorting_key_prefix_size` is unset; for v2 it equals the target table's sort-key column
    /// count at construction time, is persisted into `source_parts.dat` via
    /// `buildSourceSetForPatch`, and lets readers recover the patch's sort-key shape without
    /// walking the current target-table metadata. The sort-key expression itself is *not* carried
    /// on the sink — readers rebuild it from the target table's current `StorageMetadataPtr` and
    /// slice it to this length at apply time (see `MergeTreeData::getPatchPartMetadata`).
    PatchPartMetadata patch_metadata;

    void finishDelayedChunk() override;
    TemporaryPartPtr writeNewTempPart(BlockWithPartition & block) override;
};

}
