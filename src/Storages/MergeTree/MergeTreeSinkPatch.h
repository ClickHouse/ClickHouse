#pragma once
#include <Storages/MergeTree/MergeTreeSink.h>
#include <Storages/MergeTree/PatchParts/PatchPartsLock.h>

#include <optional>

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
        std::optional<UInt64> v2_sorting_key_prefix_size_,
        ContextPtr context_);

    String getName() const override { return "MergeTreeSinkPatch"; }

protected:
    PlainLightweightUpdateHolder update_holder;
    /// Populated iff this sink writes v2-format patches; holds the length of the semantic
    /// sort-key prefix captured at the UPDATE's callsite (it equals the target table's sort-key
    /// column count at that moment). Persisted into `source_parts.dat` so readers can recover
    /// the patch's sort-key shape without walking the current target-table metadata. The
    /// sort-key expression itself is *not* carried on the sink — readers rebuild it from the
    /// target table's current `StorageMetadataPtr` and slice it to this length at apply time
    /// (see `MergeTreeData::getPatchPartMetadata`).
    std::optional<UInt64> v2_sorting_key_prefix_size;

    void finishDelayedChunk() override;
    TemporaryPartPtr writeNewTempPart(BlockWithPartition & block) override;
};

}
