#pragma once

#include <Storages/MergeTree/MergeTreeReadPool.h>
#include <Storages/StorageMergeTreeParts.h>

#include <mutex>

namespace DB
{

/// Read pool over parts that are not owned MergeTree parts: they are described by
/// `StorageMergeTreeParts::ReadFromPartsInfo` and reconstructed on the fly into
/// `BorrowedMergeTreeDataPartInfoForReader`.
///
/// It derives from MergeTreeReadPool to reuse its work distribution as is: threads take portions off
/// their own queue via cutRangesToRead() and steal from other queues once their own runs dry, and slow
/// reads lower the number of active threads through the inherited backoff.
///
class MergeTreeReadPoolStatelessParts : public MergeTreeReadPool
{
public:
    using ReadFromPartsInfo = StorageMergeTreeParts::ReadFromPartsInfo;
    using ReadFromPart = ReadFromPartsInfo::ReadFromPart;

    MergeTreeReadPoolStatelessParts(
        ReadFromPartsInfo read_from_parts_info_,
        const StorageSnapshotPtr & storage_snapshot_,
        const FilterDAGInfoPtr & row_level_filter_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_);

    String getName() const override { return "ReadPoolStatelessParts"; }

    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;

    /// Borrowed parts have no RangesInDataPart to describe, so this pool cannot be announced.
    RangesInDataPartsDescription buildAnnouncementDescriptions() const override;

protected:
    /// The task infos are built lazily, so the base class cannot read the size out of `per_part_infos`.
    /// Without per-column sizes there is nothing to size a task by, so every part gets the same value.
    size_t getMinMarksPerTask(size_t /*part_idx*/) const override { return min_marks_per_task; }

private:
    /// Mirrors MergeTreeReadPool::fillPerThreadInfo.
    void fillPerThreadInfoForBorrowedParts(size_t threads);

    /// Built once per part and shared by every task cut from that part.
    MergeTreeReadTaskInfoPtr getOrBuildTaskInfo(size_t part_index) const;

    /// Build a BorrowedMergeTreeDataPartInfoForReader for the part and assemble the read task info
    /// (columns, lightweight-delete step, virtual fields).
    MergeTreeReadTaskInfoPtr buildTaskInfoForPart(const ReadFromPart & part, size_t part_index) const;

    /// Read columns.txt / serialization.json / columns_substreams.txt / marks / checksums from disk:
    /// nothing but the part's location is known up front.
    MergeTreeDataPartInfoForReaderPtr buildReaderInfoFromDisk(
        const ReadFromPart & part, const VolumePtr & volume, const std::string & part_root, const std::string & part_name) const;

    const ReadFromPartsInfo read_from_parts_info;
    const NamesAndTypesList storage_columns;
    const NamesAndTypesList requested_columns;

    /// The defaults with the settings carried in `table_settings(...)` applied.
    const MergeTreeSettingsPtr storage_settings;

    const size_t min_marks_per_task;

    mutable std::vector<std::once_flag> part_info_built;
    mutable std::vector<MergeTreeReadTaskInfoPtr> per_part_task_infos;

    LoggerPtr logger = getLogger("MergeTreeReadPoolStatelessParts");
};

}
