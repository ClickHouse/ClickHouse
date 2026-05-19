#pragma once

#include <Storages/MergeTree/MergeTreePartInfo.h>
#include <Storages/MergeTree/MergeTreeReadPoolBase.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>

#include <set>

namespace DB
{

class MergeTreeReadPoolParallelReplicasInOrder : public MergeTreeReadPoolBase
{
public:
    MergeTreeReadPoolParallelReplicasInOrder(
        ParallelReadingExtension extension_,
        CoordinationMode mode_,
        RangesInDataParts parts_,
        MutationsSnapshotPtr mutations_snapshot_,
        VirtualFields shared_virtual_fields_,
        const IndexReadTasks & index_read_tasks_,
        bool has_hard_limit_below_one_block_,
        bool has_soft_limit_below_one_block_,
        const StorageSnapshotPtr & storage_snapshot_,
        const FilterDAGInfoPtr & row_level_filter_,
        const PrewhereInfoPtr & prewhere_info_,
        const ExpressionActionsSettings & actions_settings_,
        const MergeTreeReaderSettings & reader_settings_,
        const Names & column_names_,
        const PoolSettings & settings_,
        const MergeTreeReadTask::BlockSizeParams & params_,
        const ContextPtr & context_);

    String getName() const override { return "ReadPoolParallelReplicasInOrder"; }
    bool preservesOrderOfRanges() const override { return true; }
    void profileFeedback(ReadBufferFromFileBase::ProfileInfo) override {}
    MergeTreeReadTaskPtr getTask(size_t task_idx, MergeTreeReadTask * previous_task) override;

    /// Returns true if this (part, projection_name) is in the coordinator's authoritative set for
    /// this stream. Returns true if the coordinator didn't send an authoritative set (older protocol):
    /// fall back to the pre-existing behavior, which constructs all sources.
    bool wasSelectedByInitiator(const MergeTreePartInfo & info, const String & projection_name) const;

private:
    LoggerPtr log = getLogger("MergeTreeReadPoolParallelReplicasInOrder");
    const ParallelReadingExtension extension;
    const CoordinationMode mode;

    /// Hard limit case (no filter): we will stop reading exactly at the limit, so always emit
    /// single-range tasks to avoid reading more rows than necessary.
    const bool has_hard_limit_below_one_block;

    /// Soft limit case (WHERE + LIMIT): the limit is only an estimation since the filter may
    /// drop rows. Emit a single-range task only for the first call per part - if it didn't fill
    /// the limit, the filter is likely selective and we should switch to regular block size.
    const bool has_soft_limit_below_one_block;

    size_t min_marks_per_task{0};
    size_t min_marks_per_request{0};
    bool no_more_tasks{false};
    RangesInDataPartsDescription request;
    RangesInDataPartsDescription buffered_tasks;

    /// See the comment in MergeTreeReadPoolParallelReplicas::getTask method.
    bool failed_to_get_task{false};

    /// Authoritative parts for this stream as reported by the coordinator in the announcement
    /// response. Consumers whose part is not in this set finish immediately. Used to skip
    /// phantom consumers on followers (their pool was constructed over all local parts, but
    /// the coordinator's stream only owns a subset assigned by the snapshot replica).
    /// Only consulted if `authoritative_parts_received` is true; otherwise the initiator is on
    /// an older protocol and didn't send a response — fall back to the pre-existing behavior.
    bool authoritative_parts_received{false};
    /// Keyed by (info, projection_name). Mixed projection/base reads can produce streams whose
    /// authoritative set contains only projection entries for a given parent part — base
    /// consumers for the same parent part must still be pruned, so we can't key by `info` alone.
    std::set<std::pair<MergeTreePartInfo, String>> authoritative_parts;

    mutable std::mutex mutex;
    std::vector<size_t> per_part_marks_in_range;
};

};
