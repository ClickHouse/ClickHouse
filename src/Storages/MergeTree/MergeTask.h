#pragma once

#include <list>
#include <memory>

#include <Common/ProfileEvents.h>
#include <Common/filesystemHelpers.h>
#include "Core/Names.h"
#include <Formats/MarkInCompressedFile.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>

#include <Interpreters/Squashing.h>
#include <Interpreters/TemporaryDataOnDisk.h>

#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/Transforms/ColumnGathererTransform.h>

#include <QueryPipeline/QueryPipeline.h>

#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Storages/MergeTree/ColumnSizeEstimator.h>
#include <Storages/MergeTree/FutureMergedMutatedPart.h>
#include <Storages/MergeTree/IExecutableTask.h>
#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedColumnOnlyOutputStream.h>
#include <Storages/MergeTree/MergeProgress.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeIndices.h>
#include <Storages/MergeTree/PartitionActionBlocker.h>

namespace ProfileEvents
{
    extern const Event MergeHorizontalStageTotalMilliseconds;
    extern const Event MergeVerticalStageTotalMilliseconds;
    extern const Event MergeProjectionStageTotalMilliseconds;
}

namespace DB
{

class MergeTask;
using MergeTaskPtr = std::shared_ptr<MergeTask>;
class RowsSourcesTemporaryFile;

class MergedPartOffsets;
using MergedPartOffsetsPtr = std::shared_ptr<MergedPartOffsets>;

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
        MergeList::Entry * merge_entry_,
        std::unique_ptr<MergeListElement> projection_merge_list_element_,
        time_t time_of_merge_,
        ContextPtr context_,
        TableLockHolder & holder,
        ReservationSharedPtr space_reservation_,
        bool deduplicate_,
        Names deduplicate_by_columns_,
        bool cleanup_,
        MergeTreeData::MergingParams merging_params_,
        bool need_prefix,
        IMergeTreeDataPart * parent_part_,
        MergedPartOffsetsPtr merged_part_offsets_,
        String suffix_,
        MergeTreeTransactionPtr txn,
        MergeTreeData * data_,
        MergeTreeDataMergerMutator * mutator_,
        PartitionActionBlocker * merges_blocker_,
        ActionBlocker * ttl_merges_blocker_)
        {
            global_ctx = std::make_shared<GlobalRuntimeContext>();

            global_ctx->future_part = std::move(future_part_);
            global_ctx->metadata_snapshot = std::move(metadata_snapshot_);
            global_ctx->merge_entry = std::move(merge_entry_);
            global_ctx->projection_merge_list_element = std::move(projection_merge_list_element_);
            global_ctx->merge_list_element_ptr
                = global_ctx->projection_merge_list_element ? global_ctx->projection_merge_list_element.get() : (*global_ctx->merge_entry)->ptr();
            global_ctx->time_of_merge = std::move(time_of_merge_);
            global_ctx->context = std::move(context_);
            global_ctx->holder = &holder;
            global_ctx->space_reservation = std::move(space_reservation_);
            global_ctx->disk = global_ctx->space_reservation->getDisk();
            global_ctx->deduplicate = std::move(deduplicate_);
            global_ctx->deduplicate_by_columns = std::move(deduplicate_by_columns_);
            global_ctx->cleanup = std::move(cleanup_);
            global_ctx->parent_part = std::move(parent_part_);
            global_ctx->merged_part_offsets = std::move(merged_part_offsets_);
            global_ctx->data = std::move(data_);
            global_ctx->mutator = std::move(mutator_);
            global_ctx->merges_blocker = std::move(merges_blocker_);
            global_ctx->ttl_merges_blocker = std::move(ttl_merges_blocker_);
            global_ctx->txn = std::move(txn);
            global_ctx->need_prefix = need_prefix;
            global_ctx->suffix = std::move(suffix_);
            global_ctx->merging_params = std::move(merging_params_);

            auto prepare_stage_ctx = std::make_shared<ExecuteAndFinalizeHorizontalPartRuntimeContext>();
            (*stages.begin())->setRuntimeContext(std::move(prepare_stage_ctx), global_ctx);
        }

    std::future<MergeTreeData::MutableDataPartPtr> getFuture()
    {
        return global_ctx->promise.get_future();
    }

    MergeTreeData::MutableDataPartPtr getUnfinishedPart()
    {
        if (global_ctx)
            return global_ctx->new_data_part;
        return nullptr;
    }

    PlainMarksByName releaseCachedMarks() const
    {
        PlainMarksByName res;
        std::swap(global_ctx->cached_marks, res);
        return res;
    }

    bool execute();

    void cancel() noexcept;

private:
    struct IStage;
    using StagePtr = std::shared_ptr<IStage>;

    struct IStageRuntimeContext {};
    using StageRuntimeContextPtr = std::shared_ptr<IStageRuntimeContext>;

    struct IStage
    {
        virtual void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) = 0;
        virtual StageRuntimeContextPtr getContextForNextStage() = 0;
        virtual ProfileEvents::Event getTotalTimeProfileEvent() const = 0;
        virtual bool execute() = 0;
        virtual void cancel() noexcept = 0;
        virtual ~IStage() = default;
    };

    /// By default this context is uninitialized, but some variables has to be set after construction,
    /// some variables are used in a process of execution
    /// Proper initialization is responsibility of the author
    struct GlobalRuntimeContext : public IStageRuntimeContext
    {
        TableLockHolder * holder;
        MergeList::Entry * merge_entry{nullptr};
        /// If not null, use this instead of the global MergeList::Entry. This is for merging projections.
        std::unique_ptr<MergeListElement> projection_merge_list_element;
        MergeListElement * merge_list_element_ptr{nullptr};
        MergeTreeData * data{nullptr};
        MergeTreeDataMergerMutator * mutator{nullptr};
        PartitionActionBlocker * merges_blocker{nullptr};
        ActionBlocker * ttl_merges_blocker{nullptr};
        StorageSnapshotPtr storage_snapshot{nullptr};
        StorageMetadataPtr metadata_snapshot{nullptr};
        FutureMergedMutatedPartPtr future_part{nullptr};
        std::vector<AlterConversionsPtr> alter_conversions;
        /// This will be either nullptr or new_data_part, so raw pointer is ok.
        IMergeTreeDataPart * parent_part{nullptr};
        MergedPartOffsetsPtr merged_part_offsets;
        ContextPtr context{nullptr};
        time_t time_of_merge{0};
        ReservationSharedPtr space_reservation{nullptr};
        DiskPtr disk{nullptr};
        bool deduplicate{false};
        Names deduplicate_by_columns{};
        bool cleanup{false};

        NamesAndTypesList gathering_columns{};
        NamesAndTypesList merging_columns{};
        NamesAndTypesList storage_columns{};
        MergeTreeData::DataPart::Checksums checksums_gathered_columns{};
        ColumnsWithTypeAndName gathered_columns_samples{};

        IndicesDescription merging_skip_indexes;
        std::unordered_map<String, IndicesDescription> skip_indexes_by_column;

        MergeAlgorithm chosen_merge_algorithm{MergeAlgorithm::Undecided};

        std::vector<ProjectionDescriptionRawPtr> projections_to_rebuild{};
        std::vector<ProjectionDescriptionRawPtr> projections_to_merge{};
        std::map<String, MergeTreeData::DataPartsVector> projections_to_merge_parts{};

        std::unique_ptr<MergeStageProgress> horizontal_stage_progress{nullptr};
        std::unique_ptr<MergeStageProgress> column_progress{nullptr};

        std::shared_ptr<MergedBlockOutputStream> to{nullptr};
        QueryPipeline merged_pipeline;
        std::unique_ptr<PullingPipelineExecutor> merging_executor;

        MergeTreeData::MutableDataPartPtr new_data_part{nullptr};

        /// If lightweight delete mask is present then some input rows are filtered out right after reading.
        std::shared_ptr<std::atomic<size_t>> input_rows_filtered{std::make_shared<std::atomic<size_t>>(0)};
        size_t rows_written{0};
        UInt64 watch_prev_elapsed{0};

        std::promise<MergeTreeData::MutableDataPartPtr> promise{};

        IMergedBlockOutputStream::WrittenOffsetColumns written_offset_columns{};
        PlainMarksByName cached_marks;

        MergeTreeTransactionPtr txn;
        bool need_prefix;
        String suffix;
        MergeTreeData::MergingParams merging_params{};

        scope_guard temporary_directory_lock;

        UInt64 prev_elapsed_ms{0};

        // will throw an exception if merge was cancelled in any way.
        void checkOperationIsNotCanceled() const;
        bool isCancelled() const;
    };

    using GlobalRuntimeContextPtr = std::shared_ptr<GlobalRuntimeContext>;

    /// By default this context is uninitialized, but some variables has to be set after construction,
    /// some variables are used in a process of execution
    /// Proper initialization is responsibility of the author
    struct ExecuteAndFinalizeHorizontalPartRuntimeContext : public IStageRuntimeContext
    {
        bool need_remove_expired_values{false};
        bool force_ttl{false};
        CompressionCodecPtr compression_codec{nullptr};
        std::shared_ptr<RowsSourcesTemporaryFile> rows_sources_temporary_file;
        std::optional<ColumnSizeEstimator> column_sizes{};

        /// For projections to rebuild
        using ProjectionNameToItsBlocks = std::map<String, MergeTreeData::MutableDataPartsVector>;
        ProjectionNameToItsBlocks projection_parts;
        std::move_iterator<ProjectionNameToItsBlocks::iterator> projection_parts_iterator;
        std::vector<Squashing> projection_squashes;
        size_t projection_block_num = 0;
        ExecutableTaskPtr merge_projection_parts_task_ptr;

        size_t initial_reservation{0};
        bool read_with_direct_io{false};

        std::function<bool()> is_cancelled{};

        /// Local variables for this stage
        size_t sum_input_rows_upper_bound{0};
        size_t sum_compressed_bytes_upper_bound{0};
        size_t sum_uncompressed_bytes_upper_bound{0};
        bool blocks_are_granules_size{false};

        LoggerPtr log{getLogger("MergeTask::PrepareStage")};

        /// Dependencies for next stages
        std::list<DB::NameAndTypePair>::const_iterator it_name_and_type;
        bool need_sync{false};
        UInt64 elapsed_execute_ns{0};
    };

    using ExecuteAndFinalizeHorizontalPartRuntimeContextPtr = std::shared_ptr<ExecuteAndFinalizeHorizontalPartRuntimeContext>;

    struct ExecuteAndFinalizeHorizontalPart : public IStage
    {
        bool execute() override;
        void cancel() noexcept override;

        bool prepare() const;
        bool executeImpl() const;
        void finalize() const;

        /// NOTE: Using pointer-to-member instead of std::function and lambda makes stacktraces much more concise and readable
        using ExecuteAndFinalizeHorizontalPartSubtasks = std::array<bool(ExecuteAndFinalizeHorizontalPart::*)()const, 3>;

        const ExecuteAndFinalizeHorizontalPartSubtasks subtasks
        {
            &ExecuteAndFinalizeHorizontalPart::prepare,
            &ExecuteAndFinalizeHorizontalPart::executeImpl,
            &ExecuteAndFinalizeHorizontalPart::executeMergeProjections
        };

        ExecuteAndFinalizeHorizontalPartSubtasks::const_iterator subtasks_iterator = subtasks.begin();

        void prepareProjectionsToMergeAndRebuild() const;
        void calculateProjections(const Block & block) const;
        void finalizeProjections() const;
        void constructTaskForProjectionPartsMerge() const;
        bool executeMergeProjections() const;

        MergeAlgorithm chooseMergeAlgorithm() const;
        void createMergedStream() const;
        void extractMergingAndGatheringColumns() const;

        void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
        {
            ctx = static_pointer_cast<ExecuteAndFinalizeHorizontalPartRuntimeContext>(local);
            global_ctx = static_pointer_cast<GlobalRuntimeContext>(global);
        }

        StageRuntimeContextPtr getContextForNextStage() override;
        ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::MergeHorizontalStageTotalMilliseconds; }

        ExecuteAndFinalizeHorizontalPartRuntimeContextPtr ctx;
        GlobalRuntimeContextPtr global_ctx;
    };

    /// By default this context is uninitialized, but some variables has to be set after construction,
    /// some variables are used in a process of execution
    /// Proper initialization is responsibility of the author
    struct VerticalMergeRuntimeContext : public IStageRuntimeContext
    {
        /// Begin dependencies from previous stage
        std::shared_ptr<RowsSourcesTemporaryFile> rows_sources_temporary_file;
        std::optional<ColumnSizeEstimator> column_sizes;
        CompressionCodecPtr compression_codec;
        std::list<DB::NameAndTypePair>::const_iterator it_name_and_type;
        bool read_with_direct_io{false};
        bool need_sync{false};
        /// End dependencies from previous stages

        enum class State : uint8_t
        {
            NEED_PREPARE,
            NEED_EXECUTE,
            NEED_FINISH
        };
        State vertical_merge_one_column_state{State::NEED_PREPARE};

        Float64 progress_before = 0;
        std::unique_ptr<MergedColumnOnlyOutputStream> column_to{nullptr};

        /// Used for prefetching. Right before starting merge of a column we create a pipeline for the next column
        /// and it initiates prefetching of the first range of that column.
        struct PreparedColumnPipeline
        {
            QueryPipeline pipeline;
            MergeTreeIndices indexes_to_recalc;
        };

        std::optional<PreparedColumnPipeline> prepared_pipeline;
        size_t max_delayed_streams = 0;
        bool use_prefetch = false;
        std::list<std::unique_ptr<MergedColumnOnlyOutputStream>> delayed_streams;
        NameSet removed_files;
        size_t column_elems_written{0};
        QueryPipeline column_parts_pipeline;
        std::unique_ptr<PullingPipelineExecutor> executor;
        UInt64 elapsed_execute_ns{0};
    };

    using VerticalMergeRuntimeContextPtr = std::shared_ptr<VerticalMergeRuntimeContext>;

    struct VerticalMergeStage : public IStage
    {
        bool execute() override;
        void cancel() noexcept override;

        void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
        {
            ctx = static_pointer_cast<VerticalMergeRuntimeContext>(local);
            global_ctx = static_pointer_cast<GlobalRuntimeContext>(global);
        }
        StageRuntimeContextPtr getContextForNextStage() override;
        ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::MergeVerticalStageTotalMilliseconds; }

        bool prepareVerticalMergeForAllColumns() const;
        bool executeVerticalMergeForAllColumns() const;
        bool finalizeVerticalMergeForAllColumns() const;

        /// NOTE: Using pointer-to-member instead of std::function and lambda makes stacktraces much more concise and readable
        using VerticalMergeStageSubtasks = std::array<bool(VerticalMergeStage::*)()const, 3>;

        const VerticalMergeStageSubtasks subtasks
        {
            &VerticalMergeStage::prepareVerticalMergeForAllColumns,
            &VerticalMergeStage::executeVerticalMergeForAllColumns,
            &VerticalMergeStage::finalizeVerticalMergeForAllColumns
        };

        VerticalMergeStageSubtasks::const_iterator subtasks_iterator = subtasks.begin();

        void prepareVerticalMergeForOneColumn() const;
        bool executeVerticalMergeForOneColumn() const;
        void finalizeVerticalMergeForOneColumn() const;

        VerticalMergeRuntimeContext::PreparedColumnPipeline createPipelineForReadingOneColumn(const String & column_name) const;

        VerticalMergeRuntimeContextPtr ctx;
        GlobalRuntimeContextPtr global_ctx;
    };

    /// By default this context is uninitialized, but some variables has to be set after construction,
    /// some variables are used in a process of execution
    /// Proper initialization is responsibility of the author
    struct MergeProjectionsRuntimeContext : public IStageRuntimeContext
    {
        /// Only one dependency
        bool need_sync{false};

        using MergeTasks = std::deque<MergeTaskPtr>;
        MergeTasks tasks_for_projections;
        MergeTasks::iterator projections_iterator;

        LoggerPtr log{getLogger("MergeTask::MergeProjectionsStage")};
        UInt64 elapsed_execute_ns{0};
    };

    using MergeProjectionsRuntimeContextPtr = std::shared_ptr<MergeProjectionsRuntimeContext>;

    struct MergeProjectionsStage : public IStage
    {
        bool execute() override;

        void cancel() noexcept override;

        void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
        {
            ctx = static_pointer_cast<MergeProjectionsRuntimeContext>(local);
            global_ctx = static_pointer_cast<GlobalRuntimeContext>(global);
        }

        StageRuntimeContextPtr getContextForNextStage() override;
        ProfileEvents::Event getTotalTimeProfileEvent() const override { return ProfileEvents::MergeProjectionStageTotalMilliseconds; }

        bool mergeMinMaxIndexAndPrepareProjections() const;
        bool executeProjections() const;
        bool finalizeProjectionsAndWholeMerge() const;

        /// NOTE: Using pointer-to-member instead of std::function and lambda makes stacktraces much more concise and readable
        using MergeProjectionsStageSubtasks = std::array<bool(MergeProjectionsStage::*)()const, 3>;

        const MergeProjectionsStageSubtasks subtasks
        {
            &MergeProjectionsStage::mergeMinMaxIndexAndPrepareProjections,
            &MergeProjectionsStage::executeProjections,
            &MergeProjectionsStage::finalizeProjectionsAndWholeMerge
        };

        MergeProjectionsStageSubtasks::const_iterator subtasks_iterator = subtasks.begin();

        MergeProjectionsRuntimeContextPtr ctx;
        GlobalRuntimeContextPtr global_ctx;
    };

    GlobalRuntimeContextPtr global_ctx;

    using Stages = std::array<StagePtr, 3>;

    const Stages stages
    {
        std::make_shared<ExecuteAndFinalizeHorizontalPart>(),
        std::make_shared<VerticalMergeStage>(),
        std::make_shared<MergeProjectionsStage>()
    };

    Stages::const_iterator stages_iterator = stages.begin();

    static bool enabledBlockNumberColumn(GlobalRuntimeContextPtr global_ctx);
    static bool enabledBlockOffsetColumn(GlobalRuntimeContextPtr global_ctx);

    static void addGatheringColumn(GlobalRuntimeContextPtr global_ctx, const String & name, const DataTypePtr & type);
};

/// FIXME
[[ maybe_unused]] static MergeTreeData::MutableDataPartPtr executeHere(MergeTaskPtr task)
{
    while (task->execute()) {}
    return task->getFuture().get();
}

}
