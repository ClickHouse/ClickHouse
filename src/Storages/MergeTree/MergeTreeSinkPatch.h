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
        String v2_sort_key_expr_list_sql_,
        std::vector<UInt8> v2_sort_key_reverse_flags_,
        ContextPtr context_);

    String getName() const override { return "MergeTreeSinkPatch"; }

protected:
    PlainLightweightUpdateHolder update_holder;
    /// Non-empty iff this sink writes v2-format patches. Stored on the sink rather than derived
    /// from metadata so `writeNewTempPart` can stamp the `SourcePartsSetForPatch` consistently.
    /// `v2_sort_key_expr_list_sql` is the SQL of the main table's sort-key expression list (e.g.
    /// `id`, or `cityHash64(id), bucket`); reverse flags are parallel to its top-level children.
    String v2_sort_key_expr_list_sql;
    std::vector<UInt8> v2_sort_key_reverse_flags;

    void finishDelayedChunk() override;
    TemporaryPartPtr writeNewTempPart(BlockWithPartition & block) override;
};

}
