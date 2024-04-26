#pragma once

#include <Processors/Chunk.h>
#include <Processors/Sources/QueueSubscriptionSourceAdapter.h>

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/RangesInDataPart.h>

namespace DB
{

struct PartitionIdChunkInfo : public ChunkInfo
{
    static constexpr size_t info_slot = 2;
    const String partition_id;

    explicit PartitionIdChunkInfo(String partition_id_);
};

class MergeTreePartSequentialReader
{
public:
    MergeTreePartSequentialReader(
        const Block & header_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        MarkCachePtr mark_cache_,
        Names columns_to_read_,
        RangesInDataPart part_ranges_,
        std::shared_ptr<PartitionIdChunkInfo> info_);

    bool hasSome() const;
    bool isEmpty() const;

    Chunk readNext();

private:
    const Block & header;
    const MergeTreeData & storage;
    const StorageSnapshotPtr & storage_snapshot;
    MarkCachePtr mark_cache;
    Names columns_to_read;
    RangesInDataPart part_ranges;
    std::shared_ptr<PartitionIdChunkInfo> info;

    MergeTreeReaderPtr reader;
    size_t initial_mark = 0;
    size_t current_mark = 0;
    size_t current_row = 0;
};

/// TODO:
class MergeTreePartitionSequentialSource : public QueueSubscriptionSourceAdapter<RangesInDataPart>
{
    void initNextReader();

public:
    MergeTreePartitionSequentialSource(
        const MergeTreeData & storage_, StorageSnapshotPtr storage_snapshot_, StreamSubscriptionPtr subscription_, Names columns_to_read_);

    ~MergeTreePartitionSequentialSource() override = default;

    String getName() const override { return "MergeTreePartitionSequentialSource"; }

protected:
    Chunk useCachedData() override;

private:
    const MergeTreeData & storage;
    StreamSubscriptionPtr subscription_holder;
    Names columns_to_read;

    StorageSnapshotPtr storage_snapshot;
    std::optional<MergeTreePartSequentialReader> reader;

    std::map<String, std::shared_ptr<PartitionIdChunkInfo>> partition_infos;
};

Pipe createMergeTreePartitionSequentialSource(
    const MergeTreeData & storage, const StorageSnapshotPtr & storage_snapshot, StreamSubscriptionPtr subscription, Names columns_to_read);

}
