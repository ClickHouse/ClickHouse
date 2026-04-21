#pragma once
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/MergeTree/PatchParts/PatchPartsLock.h>

#include <optional>

namespace DB
{

class ReplicatedMergeTreeSinkPatch : public ReplicatedMergeTreeSink
{
public:
    ReplicatedMergeTreeSinkPatch(
        StorageReplicatedMergeTree & storage_,
        StorageMetadataPtr metadata_snapshot_,
        LightweightUpdateHolderInKeeper update_holder_,
        std::optional<UInt64> v2_sorting_key_prefix_size_,
        ContextPtr context_);

    ~ReplicatedMergeTreeSinkPatch() override;

    String getName() const override { return "ReplicatedMergeTreeSinkPatch"; }

private:
    void finishDelayed(const ZooKeeperWithFaultInjectionPtr & zookeeper) override;
    TemporaryPartPtr writeNewTempPart(BlockWithPartition & block) override;
    UInt64 getDataVersionInPartition(const String & original_partition_id) const;

    LightweightUpdateHolderInKeeper update_holder;
    /// Semantic sort-key prefix length for v2 patches, unset for v1. See `MergeTreeSinkPatch`.
    std::optional<UInt64> v2_sorting_key_prefix_size;
};

}
