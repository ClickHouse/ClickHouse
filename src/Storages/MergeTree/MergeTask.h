#pragma once

#include <Storages/MergeTree/IExecutableTask.h>
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
        {
            (void)future_part_;
            (void)metadata_snapshot_;
            (void)merge_entry_;
            (void)holder_;
            (void)time_of_merge_;
            (void)context_;
            (void)space_reservation_;
            (void)deduplicate_;
            (void)deduplicate_by_columns_;
            (void)merging_params_;
            (void)parent_part_;
            (void)prefix_;
            (void)data_;
            (void)merges_blocker_;
            (void)ttl_merges_blocker_;
        }

    std::future<MergeTreeData::MutableDataPartPtr> getFuture()
    {
        return global_ctx->promise.get_future();
    }

    bool execute();

private:
    struct IStage;
    using StagePtr = std::shared_ptr<IStage>;

    struct IStageRuntimeContext {};
    using StageRuntimeContextPtr = std::shared_ptr<IStageRuntimeContext>;

    struct IStage
    {
        virtual void setRuntimeContext() = 0;
        virtual StageRuntimeContextPtr getContextForNextStage() = 0;
        virtual bool execute() = 0;
        virtual ~IStage() = default;
    };

    struct GlobalRuntimeContext : public IStageRuntimeContext
    {
        MergeList::Entry * merge_entry;
        MergeTreeData * data;
        ActionBlocker * merges_blocker;
        ActionBlocker * ttl_merges_blocker;
        StorageMetadataPtr metadata_snapshot;
        FutureMergedMutatedPartPtr future_part;
        MergeTreeDataPartPtr parent_part;
        ContextPtr context;
        time_t time_of_merge;
        TableLockHolder & holder;
        ReservationSharedPtr space_reservation;
        bool deduplicate;
        Names deduplicate_by_columns;

        NamesAndTypesList gathering_columns;
        NamesAndTypesList merging_columns;
        Names gathering_column_names;
        Names merging_column_names;
        NamesAndTypesList storage_columns;
        Names all_column_names;
        MergeTreeData::DataPart::Checksums checksums_gathered_columns;

        MergeAlgorithm chosen_merge_algorithm{MergeAlgorithm::Undecided};
        bool need_sync{false};
        size_t gathering_column_names_size{0};

        std::shared_ptr<MergedBlockOutputStream> to;
        BlockInputStreamPtr merged_stream;

        SyncGuardPtr sync_guard{nullptr};
        MergeTreeData::MutableDataPartPtr new_data_part;

        std::promise<MergeTreeData::MutableDataPartPtr> promise;

        IMergedBlockOutputStream::WrittenOffsetColumns written_offset_columns;
    };

    using GlobalRuntineContextPtr = std::shared_ptr<GlobalRuntimeContext>;

    struct PrepareStageRuntimeContext : public IStageRuntimeContext
    {
        String prefix;

        DiskPtr tmp_disk{nullptr};
        DiskPtr disk{nullptr};

        MergeTreeData * data;
        StorageMetadataPtr metadata_snapshot;

        MergeTreeData::MergingParams merging_params;
        bool need_remove_expired_values{false};
        bool force_ttl{false};
        MergeTreeData::MutableDataPartPtr new_data_part;
        CompressionCodecPtr compression_codec;
        size_t sum_input_rows_upper_bound{0};
        size_t sum_compressed_bytes_upper_bound{0};

        std::unique_ptr<MergeStageProgress> horizontal_stage_progress{nullptr};
        std::unique_ptr<MergeStageProgress> column_progress{nullptr};

        std::unique_ptr<TemporaryFile> rows_sources_file;
        std::unique_ptr<WriteBufferFromFileBase> rows_sources_uncompressed_write_buf{nullptr};
        std::unique_ptr<WriteBuffer> rows_sources_write_buf{nullptr};
        std::optional<ColumnSizeEstimator> column_sizes;

        SyncGuardPtr sync_guard{nullptr};
        bool blocks_are_granules_size{false};

        std::shared_ptr<MergedBlockOutputStream> to;
        BlockInputStreamPtr merged_stream;

        size_t rows_written{0};
        size_t initial_reservation{0};
        bool read_with_direct_io{false};
        UInt64 watch_prev_elapsed{0};

        std::function<bool()> is_cancelled;


        Poco::Logger * log{&Poco::Logger::get("MergeTask::PrepareStage")};
    };

    using PrepareStageRuntimeContextPtr = std::shared_ptr<PrepareStageRuntimeContext>;

    struct PrepareStage : public IStage
    {
        bool execute() override;
        StageRuntimeContextPtr getContextForNextStage() override;


        MergeAlgorithm chooseMergeAlgorithm() const;
        void createMergedStream();

        PrepareStageRuntimeContextPtr ctx;
        GlobalRuntineContextPtr global_ctx;
    };

    struct ExecuteAndFinalizeHorizontalPartRuntimeContext : public IStageRuntimeContext
    {
        /// Dependencies from previous stages
        std::function<bool()> is_cancelled;
        std::shared_ptr<MergedBlockOutputStream> to;
        BlockInputStreamPtr merged_stream;
        MergeTreeData * data;
        MergeList::Entry * merge_entry;
        ActionBlocker * merges_blocker;
        ActionBlocker * ttl_merges_blocker;
        bool need_remove_expired_values{false};
        size_t sum_input_rows_upper_bound{0};
        size_t rows_written{0};
        size_t initial_reservation{0};

        std::list<DB::NameAndTypePair>::const_iterator it_name_and_type;
        size_t column_num_for_vertical_merge{0};
        size_t gathering_column_names_size{0};

        /// Dependencies for next stages
        bool need_sync{false};
    };

    using ExecuteAndFinalizeHorizontalPartRuntimeContextPtr = std::shared_ptr<ExecuteAndFinalizeHorizontalPartRuntimeContext>;


    struct ExecuteAndFinalizeHorizontalPart : public IStage
    {
        bool execute() override;

        ExecuteAndFinalizeHorizontalPartRuntimeContextPtr ctx;
        GlobalRuntineContextPtr global_ctx;
    };


    struct VerticalMergeRuntimeContext : public IStageRuntimeContext
    {
        std::unique_ptr<WriteBuffer> rows_sources_write_buf{nullptr};
        std::unique_ptr<WriteBufferFromFileBase> rows_sources_uncompressed_write_buf{nullptr};
        std::unique_ptr<TemporaryFile> rows_sources_file;
        std::optional<ColumnSizeEstimator> column_sizes;
        FutureMergedMutatedPartPtr future_part;
        MergeTreeData::MutableDataPartPtr new_data_part;
        CompressionCodecPtr compression_codec;
        DiskPtr tmp_disk{nullptr};
        std::list<DB::NameAndTypePair>::const_iterator it_name_and_type;
        size_t column_num_for_vertical_merge{0};

        bool read_with_direct_io{false};
        UInt64 watch_prev_elapsed{0};
        std::shared_ptr<MergedBlockOutputStream> to;
        bool need_sync{false};
        size_t rows_written{0};

        enum class State
        {
            NEED_PREPARE,
            NEED_EXECUTE,
            NEED_FINISH
        };
        State vertical_merge_one_column_state{State::NEED_PREPARE};

        Float64 progress_before = 0;
        std::unique_ptr<MergeStageProgress> column_progress{nullptr};
        std::unique_ptr<MergedColumnOnlyOutputStream> column_to{nullptr};
        size_t column_elems_written{0};

        /// Dependencies for next stages
        BlockInputStreams column_part_streams;
        std::unique_ptr<ColumnGathererStream> column_gathered_stream;
        std::unique_ptr<CompressedReadBufferFromFile> rows_sources_read_buf{nullptr};
    };

    using VerticalMergeRuntimeContextPtr = std::shared_ptr<VerticalMergeRuntimeContext>;


    struct VerticalMergeStage : public IStage
    {
        bool execute() override;

        bool prepareVerticalMergeForAllColumns();
        bool executeVerticalMergeForAllColumns();
        bool finalizeVerticalMergeForAllColumns();

        void prepareVerticalMergeForOneColumn();
        bool executeVerticalMergeForOneColumn();
        void finalizeVerticalMergeForOneColumn();

        VerticalMergeRuntimeContextPtr ctx;
        GlobalRuntineContextPtr global_ctx;
    };


    struct MergeProjectionsRuntimeContext : public IStageRuntimeContext
    {
        using MergeTasks = std::deque<MergeTaskPtr>;
        MergeTasks tasks_for_projections;
        MergeTasks::iterator projections_iterator;

        Poco::Logger * log{&Poco::Logger::get("MergeTask::MergeProjectionsStage")};
    };

    using MergeProjectionsRuntimeContextPtr = std::shared_ptr<MergeProjectionsRuntimeContext>;


    struct MergeProjectionsStage : public IStage
    {
        bool execute() override;

        bool mergeMinMaxIndexAndPrepareProjections();
        bool executeProjections();
        bool finalizeProjectionsAndWholeMerge();

        MergeProjectionsRuntimeContextPtr ctx;
        GlobalRuntineContextPtr global_ctx;
    };


    GlobalRuntimeContext global_ctx;

};

/// FIXME
[[ maybe_unused]] static MergeTreeData::MutableDataPartPtr executeHere(MergeTaskPtr task)
{
    while (task->execute()) {}
    return task->getFuture().get();
}

}
