#pragma once

#include <Processors/QueryResultPreview.h>
#include <Processors/Transforms/SortingTransform.h>
#include <Common/Logger.h>
#include <Core/SortDescription.h>
#include <Common/filesystemHelpers.h>
#include <Interpreters/TemporaryDataOnDisk.h>
#include <Processors/TopKThresholdTracker.h>


namespace DB
{

class IVolume;
using VolumePtr = std::shared_ptr<IVolume>;

class MergeSortingTransform;

/// Shared state of the query result previews of one sorting (see `QueryResultPreview.h`).
struct SortingQueryResultPreviews
{
    QueryResultPreviewsControl control;

    /// The transforms of one sorting, registered at construction (pipeline building is
    /// single-threaded). The emitter of a round reads the accumulated chunks of every participant
    /// under `control.participantMutex` to merge them into one top-N preview.
    std::vector<MergeSortingTransform *> participants;

    SortingQueryResultPreviews(const QueryResultPreviewsSettings & settings_, size_t num_participants)
        : control(settings_, num_participants)
    {
        participants.reserve(num_participants);
    }
};

using SortingQueryResultPreviewsPtr = std::shared_ptr<SortingQueryResultPreviews>;

/// Takes sorted separate chunks of data. Sorts them.
/// Returns stream with globally sorted data.
class MergeSortingTransform final : public SortingTransform, public IQueryResultPreviewEmitter
{
public:
    /// limit - if not 0, allowed to return just first 'limit' rows in sorted order.
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
        SortingQueryResultPreviewsPtr query_result_previews_ = nullptr);

    String getName() const override { return "MergeSortingTransform"; }

    /// Preview chunks arriving from upstream (already sorted and cut by `PartialSortingTransform`)
    /// are passed along without touching the accumulated state.
    bool supportsQueryResultPreviews() const override { return true; }

    void activateQueryResultPreviews() override;

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

    /// Query result previews (see `QueryResultPreview.h`); nullptr when this sorting cannot emit
    /// them. Emission additionally requires activation by `QueryPipeline::complete`.
    SortingQueryResultPreviewsPtr query_result_previews;
    size_t preview_participant_index = 0;
    /// Set (under the participant mutex) when the accumulated chunks are moved into the final
    /// merge sorter, after which no consistent snapshot is possible anymore.
    bool preview_state_moved = false;

    void tryEmitQueryResultPreview(UInt64 num_rows, UInt64 num_bytes);
};

}
