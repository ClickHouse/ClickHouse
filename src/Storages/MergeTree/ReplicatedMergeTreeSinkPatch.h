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
        ContextPtr context_);

    ~ReplicatedMergeTreeSinkPatch() override;

    String getName() const override { return "ReplicatedMergeTreeSinkPatch"; }

private:
    void finishDelayed(const ZooKeeperWithFaultInjectionPtr & zookeeper) override;
    TemporaryPartPtr writeNewTempPart(BlockWithPartition & block) override;
    UInt64 getDataVersionInPartition(const String & original_partition_id) const;

    LightweightUpdateHolderInKeeper update_holder;
};

}
