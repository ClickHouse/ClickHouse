#pragma once
#include <Storages/MergeTree/ReplicatedMergeTreeSink.h>
#include <Storages/MergeTree/PatchParts/PatchPartInfo.h>
#include <Storages/MergeTree/PatchParts/PatchPartsLock.h>

namespace DB
{

class ReplicatedMergeTreeSinkPatch : public ReplicatedMergeTreeSink
{
public:
    ReplicatedMergeTreeSinkPatch(
        StorageReplicatedMergeTree & storage_,
        PatchPartMetadata patch_metadata_,
        LightweightUpdateHolderInKeeper update_holder_,
        ContextPtr context_);

    ~ReplicatedMergeTreeSinkPatch() override;

    String getName() const override { return "ReplicatedMergeTreeSinkPatch"; }

private:
    void finishDelayed(const ZooKeeperWithFaultInjectionPtr & zookeeper) override;
    TemporaryPartPtr writeNewTempPart(BlockWithPartition & block) override;
    UInt64 getDataVersionInPartition(const String & original_partition_id) const;

    LightweightUpdateHolderInKeeper update_holder;
    /// Format version + patch `StorageMetadataPtr` + (for v2) the semantic sort-key prefix size.
    /// See `MergeTreeSinkPatch::patch_metadata`.
    PatchPartMetadata patch_metadata;
};

}
