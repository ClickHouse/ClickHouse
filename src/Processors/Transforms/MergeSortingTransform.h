#pragma once

#include <Processors/Transforms/SortingTransform.h>
#include <Common/Logger.h>
#include <Core/SortDescription.h>
#include <Common/filesystemHelpers.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/TopKThresholdTracker.h>
#include <Common/ProfileEvents.h>


namespace ProfileEvents
{
    extern const Event ExternalSortMerge;
}

namespace DB
{

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

/// Takes sorted separate chunks of data. Sorts them.
/// Returns stream with globally sorted data.
class MergeSortingTransform final : public SortingTransform
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
    /// merge_mode - the mode of the merges that form each spilled run and that merge the rows still in memory
    /// at the end. With `MergeUniqueChunks` every input chunk must be unique on the sort description, and
    /// these merges keep one row per key; a merge step that consumes only duplicates yields a chunk without
    /// rows. The final merge of spilled runs keeps every row, so a key can appear once per run, and the
    /// consumer removes these adjacent repeats. `limit` must be 0 in this mode, because that merge would
    /// count the repeats.
    /// external_merge_event - counts the final merges of spilled runs, for the operator that the sort serves.
    MergeSortingTransform(
        SharedHeader header,
        const SortDescription & description_,
        size_t max_merged_block_size_,
        size_t max_block_bytes,
        UInt64 limit_,
        bool increase_sort_description_compile_attempts,
        size_t max_bytes_before_remerge_,
        double remerge_lowered_memory_bytes_ratio_,
        size_t max_bytes_in_block_before_external_sort_,
        size_t max_bytes_in_query_before_external_sort_,
        TemporaryDataOnDiskScopePtr tmp_data_,
        size_t min_free_disk_space_,
        TopKThresholdTrackerPtr threshold_tracker_ = nullptr,
        MergeSorter::Mode merge_mode_ = MergeSorter::Mode::PreserveRows,
        ProfileEvents::Event external_merge_event_ = ProfileEvents::ExternalSortMerge);

    String getName() const override { return "MergeSortingTransform"; }

protected:
    void consume(Chunk chunk) override;
    void serialize() override;
    void generate() override;

    PipelineUpdate updatePipeline() override;

private:
    size_t max_bytes_before_remerge;
    double remerge_lowered_memory_bytes_ratio;
    size_t max_bytes_in_block_before_external_sort;
    size_t max_bytes_in_query_before_external_sort;
    TemporaryDataOnDiskScopePtr tmp_data;
    size_t temporary_files_num = 0;
    size_t min_free_disk_space;
    size_t max_block_bytes;

    size_t sum_rows_in_blocks = 0;
    size_t sum_bytes_in_blocks = 0;

    LoggerPtr log = getLogger("MergeSortingTransform");

    /// If remerge doesn't save memory at least several times, mark it as useless and don't do it anymore.
    bool remerge_is_useful = true;

    /// Merge all accumulated blocks to keep no more than limit rows.
    void remerge();

    ProcessorPtr external_merging_sorted;

    TopKThresholdTrackerPtr threshold_tracker;

    const MergeSorter::Mode merge_mode;
    const ProfileEvents::Event external_merge_event;
};

}
