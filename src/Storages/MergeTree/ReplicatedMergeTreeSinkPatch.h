#pragma once
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/MergeTree/PatchParts/PatchPartsLock.h>

namespace DB
{

class ReplicatedMergeTreeSinkPatch : public ReplicatedMergeTreeSink
{
public:
    ReplicatedMergeTreeSinkPatch(
        StorageReplicatedMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        LightweightUpdateHolderInKeeper update_holder_,
        String v2_sort_key_expr_list_sql_,
        std::vector<UInt8> v2_sort_key_reverse_flags_,
        ContextPtr context_);

    ~ReplicatedMergeTreeSinkPatch() override;

    String getName() const override { return "ReplicatedMergeTreeSinkPatch"; }

private:
    void finishDelayed(const ZooKeeperWithFaultInjectionPtr & zookeeper) override;
    TemporaryPartPtr writeNewTempPart(BlockWithPartition & block) override;
    UInt64 getDataVersionInPartition(const String & original_partition_id) const;

    LightweightUpdateHolderInKeeper update_holder;
    /// Non-empty iff this sink writes v2-format patches. See `MergeTreeSinkPatch` for details.
    String v2_sort_key_expr_list_sql;
    std::vector<UInt8> v2_sort_key_reverse_flags;
};

}
