#pragma once

#include <Storages/MergeTree/BackgroundTask.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/ColumnSizeEstimator.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <DataStreams/ColumnGathererStream.h>
#include <Compression/CompressedReadBufferFromFile.h>

#include <memory>
#include <list>

namespace DB
{

class MergeTask;
using MergeTaskPtr = std::shared_ptr<MergeTask>;

/**
 * Overview of the merge algorithm
 *
 * Each merge is executed sequentially block by block.
 * The main idea is to make a merge not a subroutine which is executed
 * in a thread pool and may occupy a thread for a period of time,
 * but to make a merge a coroutine which can suspend the execution
 * in some points and then resume the execution from this point.
 *
 * A perfect point where to suspend the execution is after the work over a block is finished.
 * The task itself will be executed via BackgroundJobExecutor.
 *
 * The interface of the task is simple.
 * The main method is `execute()` which will return true, if the task wants to be executed again and false otherwise.
 *
 * With this kind of task we can give a merge a priority.
 * A priority is simple - the lower the size of the merge, the higher priority.
 * So, if ClickHouse wants to merge some really big parts into a bigger part,
 * then it will be executed for a long time, because the result of the merge is not really needed immediately.
 * It is better to merge small parts as soon as possible.
*/
class MergeTask
{
public:

    MergeTask(
        FutureMergedMutatedPartPtr future_part_,
        StorageMetadataPtr metadata_snapshot_,
        MergeList::Entry & merge_entry_,
        TableLockHolder & holder_,
        time_t time_of_merge_,
        ContextPtr context_,
        ReservationSharedPtr space_reservation_,
        bool deduplicate_,
        Names deduplicate_by_columns_,
        MergeTreeData::MergingParams merging_params_,
        MergeTreeDataPartPtr parent_part_,
        String prefix_,
        MergeTreeData & data_,
        ActionBlocker & merges_blocker_,
        ActionBlocker & ttl_merges_blocker_)
        : future_part(future_part_)
        , metadata_snapshot(metadata_snapshot_)
        , merge_entry(merge_entry_)
        , holder(holder_)
        , time_of_merge(time_of_merge_)
        , context(context_)
        , space_reservation(std::move(space_reservation_))
        , deduplicate(deduplicate_)
        , deduplicate_by_columns(std::move(deduplicate_by_columns_))
        , merging_params(merging_params_)
        , parent_part(parent_part_)
        , prefix(prefix_)
        , data(data_)
        , merges_blocker(merges_blocker_)
        , ttl_merges_blocker(ttl_merges_blocker_)
        {}

    std::future<MergeTreeData::MutableDataPartPtr> getFuture()
    {
        return promise.get_future();
    }

    bool execute();

    void prepare();

private:
    void createMergedStream();

    MergeAlgorithm chooseMergeAlgorithm() const;

    bool executeHorizontalForBlock();
    void finalizeHorizontalPartOfTheMerge();
    void prepareVertical();
    bool executeVerticalMergeForAllColumns();

    void prepareVerticalMergeForOneColumn();
    bool executeVerticalMergeForOneColumn();
    void finalizeVerticalMergeForOneColumn();

    void finalizeVerticalMergeForAllColumns();

    void mergeMinMaxIndex();

    void prepareProjections();
    bool executeProjections();
    void finalizeProjections();

    void finalize();


    std::promise<MergeTreeData::MutableDataPartPtr> promise;

    /**
     * States of MergeTask state machine.
     * Transitions are from up to down.
     * But for vertical merge there are horizontal part of the merge and vertical part.
     * For horizontal there is horizontal part only.
     */
    enum class MergeTaskState
    {
        NEED_PREPARE,
        NEED_EXECUTE_HORIZONTAL,
        NEED_FINALIZE_HORIZONTAL,
        NEED_PREPARE_VERTICAL,
        NEED_EXECUTE_VERTICAL,
        NEED_FINISH_VERTICAL,
        NEED_MERGE_MIN_MAX_INDEX,

        NEED_PREPARE_PROJECTIONS,
        NEED_EXECUTE_PROJECTIONS,
        NEED_FINISH_PROJECTIONS,

        NEED_FINISH
    };
    MergeTaskState state{MergeTaskState::NEED_PREPARE};

    enum class VecticalMergeOneColumnState
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINISH
    };
    VecticalMergeOneColumnState vertical_merge_one_column_state{VecticalMergeOneColumnState::NEED_PREPARE};

    FutureMergedMutatedPartPtr future_part;
    StorageMetadataPtr metadata_snapshot;
    MergeList::Entry & merge_entry;
    TableLockHolder & holder;
    time_t time_of_merge;
    ContextPtr context;
    /// It is necessary, because of projections presense
    ReservationSharedPtr space_reservation;
    bool deduplicate;
    Names deduplicate_by_columns;
    MergeTreeData::MergingParams merging_params;
    MergeTreeDataPartPtr parent_part;
    String prefix;

    /// From MergeTreeDataMergerMutator

    MergeTreeData & data;
    Poco::Logger * log{&Poco::Logger::get("MergeTask")};

    ActionBlocker & merges_blocker;
    ActionBlocker & ttl_merges_blocker;


    /// Previously stack located variables

    NamesAndTypesList gathering_columns;
    NamesAndTypesList merging_columns;
    Names gathering_column_names;
    Names merging_column_names;

    NamesAndTypesList storage_columns;
    Names all_column_names;

    String new_part_tmp_path;

    size_t sum_input_rows_upper_bound{0};

    bool need_remove_expired_values{false};
    bool force_ttl{false};

    DiskPtr tmp_disk{nullptr};
    DiskPtr disk{nullptr};

    std::unique_ptr<MergeStageProgress> horizontal_stage_progress{nullptr};
    std::unique_ptr<MergeStageProgress> column_progress{nullptr};

    std::unique_ptr<TemporaryFile> rows_sources_file;
    std::unique_ptr<WriteBufferFromFileBase> rows_sources_uncompressed_write_buf{nullptr};
    std::unique_ptr<WriteBuffer> rows_sources_write_buf{nullptr};
    std::optional<ColumnSizeEstimator> column_sizes;

    SyncGuardPtr sync_guard{nullptr};
    MergeTreeData::MutableDataPartPtr new_data_part;
    CompressionCodecPtr compression_codec;

    MergeAlgorithm chosen_merge_algorithm{MergeAlgorithm::Undecided};

    std::shared_ptr<MergedBlockOutputStream> to;
    BlockInputStreamPtr merged_stream;

    bool blocks_are_granules_size{false};

    /// Variables that are needed for horizontal merge execution

    size_t rows_written{0};
    size_t initial_reservation{0};
    UInt64 watch_prev_elapsed{0};

    std::function<bool()> is_cancelled;

    bool need_sync{false};
    bool read_with_direct_io{false};


    MergeTreeData::DataPart::Checksums checksums_gathered_columns;

    std::list<DB::NameAndTypePair>::const_iterator it_name_and_type;
    size_t column_num_for_vertical_merge{0};
    size_t gathering_column_names_size{0};

    /// This class has no default constructor, so we wrap it with unique_ptr
    std::unique_ptr<CompressedReadBufferFromFile> rows_sources_read_buf{nullptr};
    IMergedBlockOutputStream::WrittenOffsetColumns written_offset_columns;
    std::unique_ptr<MergedColumnOnlyOutputStream> column_to;

    BlockInputStreams column_part_streams;
    std::unique_ptr<ColumnGathererStream> column_gathered_stream;

    size_t column_elems_written = 0;
    Float64 progress_before = 0;


    using MergeTasks = std::deque<MergeTaskPtr>;
    MergeTasks tasks_for_projections;
    MergeTasks::iterator projections_iterator;
};

/// FIXME
[[ maybe_unused]] static MergeTreeData::MutableDataPartPtr executeHere(MergeTaskPtr task)
{
    while (task->execute()) {}
    return task->getFuture().get();
}

}
