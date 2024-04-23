#pragma once

#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RangesInDataPart.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>

namespace DB
{

/// TODO:
class MergeTreePartSequentialReader
{
public:
    MergeTreePartSequentialReader(
        const Block & header_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        MarkCachePtr mark_cache_,
        Names columns_to_read_,
        RangesInDataPart part_ranges_);

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

    MergeTreeReaderPtr reader;
    size_t initial_mark = 0;
    size_t current_mark = 0;
    size_t current_row = 0;
};

/// TODO:
class MergeTreePartitionSequentialSource : public ISource
{
    void initNextReader();

public:
    MergeTreePartitionSequentialSource(
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        RangesInDataParts parts_with_ranges_,
        Names columns_to_read_);

    ~MergeTreePartitionSequentialSource() override = default;

    String getName() const override { return "MergeTreePartitionSequentialSource"; }

protected:
    Chunk generate() override;

private:
    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;
    RangesInDataParts parts_with_ranges;
    MarkCachePtr mark_cache;
    Names columns_to_read;

    size_t next_part_to_read = 0;
    std::optional<MergeTreePartSequentialReader> reader;

    LoggerPtr log = getLogger("MergeTreePartitionSequentialSource");
};

Pipe createMergeTreePartitionSequentialSource(
    const MergeTreeData & storage, const StorageSnapshotPtr & storage_snapshot, RangesInDataParts parts_with_ranges, Names columns_to_read);

}
