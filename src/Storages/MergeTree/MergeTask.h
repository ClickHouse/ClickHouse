#pragma once

#include <list>
#include <memory>

#include <Common/filesystemHelpers.h>

#include <Compression/CompressedReadBuffer.h>
#include <Compression/CompressedReadBufferFromFile.h>

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
        MergeList::Entry * merge_entry_,
        std::unique_ptr<MergeListElement> projection_merge_list_element_,
        time_t time_of_merge_,
        ContextPtr context_,
        ReservationSharedPtr space_reservation_,
        bool deduplicate_,
        Names deduplicate_by_columns_,
        bool cleanup_,
        MergeTreeData::MergingParams merging_params_,
        bool need_prefix,
        IMergeTreeDataPart * parent_part_,
        String suffix_,
        MergeTreeTransactionPtr txn,
        MergeTreeData * data_,
        MergeTreeDataMergerMutator * mutator_,
        ActionBlocker * merges_blocker_,
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
            global_ctx->space_reservation = std::move(space_reservation_);
            global_ctx->deduplicate = std::move(deduplicate_);
            global_ctx->deduplicate_by_columns = std::move(deduplicate_by_columns_);
            global_ctx->cleanup = std::move(cleanup_);
            global_ctx->parent_part = std::move(parent_part_);
            global_ctx->data = std::move(data_);
            global_ctx->mutator = std::move(mutator_);
            global_ctx->merges_blocker = std::move(merges_blocker_);
            global_ctx->ttl_merges_blocker = std::move(ttl_merges_blocker_);
            global_ctx->txn = std::move(txn);
            global_ctx->need_prefix = need_prefix;

            auto prepare_stage_ctx = std::make_shared<ExecuteAndFinalizeHorizontalPartRuntimeContext>();

            prepare_stage_ctx->suffix = std::move(suffix_);
            prepare_stage_ctx->merging_params = std::move(merging_params_);

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

    bool execute();

private:
    struct IStage;
    using StagePtr = std::shared_ptr<IStage>;

    struct IStageRuntimeContext {};
    using StageRuntimeContextPtr = std::shared_ptr<IStageRuntimeContext>;

    struct IStage
    {
        virtual void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) = 0;
        virtual StageRuntimeContextPtr getContextForNextStage() = 0;
        virtual bool execute() = 0;
        virtual ~IStage() = default;
    };

    /// By default this context is uninitialized, but some variables has to be set after construction,
    /// some variables are used in a process of execution
    /// Proper initialization is responsibility of the author
    struct GlobalRuntimeContext : public IStageRuntimeContext
    {
        MergeList::Entry * merge_entry{nullptr};
        /// If not null, use this instead of the global MergeList::Entry. This is for merging projections.
        std::unique_ptr<MergeListElement> projection_merge_list_element;
        MergeListElement * merge_list_element_ptr{nullptr};
        MergeTreeData * data{nullptr};
        MergeTreeDataMergerMutator * mutator{nullptr};
        ActionBlocker * merges_blocker{nullptr};
        ActionBlocker * ttl_merges_blocker{nullptr};
        StorageSnapshotPtr storage_snapshot{nullptr};
        StorageMetadataPtr metadata_snapshot{nullptr};
        FutureMergedMutatedPartPtr future_part{nullptr};
        /// This will be either nullptr or new_data_part, so raw pointer is ok.
        IMergeTreeDataPart * parent_part{nullptr};
        ContextPtr context{nullptr};
        time_t time_of_merge{0};
        ReservationSharedPtr space_reservation{nullptr};
        bool deduplicate{false};
        Names deduplicate_by_columns{};
        bool cleanup{false};

        NamesAndTypesList gathering_columns{};
        NamesAndTypesList merging_columns{};
        NamesAndTypesList storage_columns{};
        MergeTreeData::DataPart::Checksums checksums_gathered_columns{};

        IndicesDescription merging_skip_indexes;
        std::unordered_map<String, IndicesDescription> skip_indexes_by_column;

        MergeAlgorithm chosen_merge_algorithm{MergeAlgorithm::Undecided};

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

        MergeTreeTransactionPtr txn;
        bool need_prefix;

        scope_guard temporary_directory_lock;
    };

    using GlobalRuntimeContextPtr = std::shared_ptr<GlobalRuntimeContext>;

    /// By default this context is uninitialized, but some variables has to be set after construction,
    /// some variables are used in a process of execution
    /// Proper initialization is responsibility of the author
    struct ExecuteAndFinalizeHorizontalPartRuntimeContext : public IStageRuntimeContext
    {
        /// Dependencies
        String suffix;
        bool need_prefix;
        MergeTreeData::MergingParams merging_params{};

        TemporaryDataOnDiskPtr tmp_disk{nullptr};
        DiskPtr disk{nullptr};
        bool need_remove_expired_values{false};
        bool force_ttl{false};
        CompressionCodecPtr compression_codec{nullptr};
        size_t sum_input_rows_upper_bound{0};
        std::unique_ptr<WriteBufferFromFileBase> rows_sources_uncompressed_write_buf{nullptr};
        std::unique_ptr<WriteBuffer> rows_sources_write_buf{nullptr};
        std::optional<ColumnSizeEstimator> column_sizes{};

        size_t initial_reservation{0};
        bool read_with_direct_io{false};

        std::function<bool()> is_cancelled{};

        /// Local variables for this stage
        size_t sum_compressed_bytes_upper_bound{0};
        bool blocks_are_granules_size{false};

        LoggerPtr log{getLogger("MergeTask::PrepareStage")};

        /// Dependencies for next stages
        std::list<DB::NameAndTypePair>::const_iterator it_name_and_type;
        bool need_sync{false};
    };

    using ExecuteAndFinalizeHorizontalPartRuntimeContextPtr = std::shared_ptr<ExecuteAndFinalizeHorizontalPartRuntimeContext>;


    struct ExecuteAndFinalizeHorizontalPart : public IStage
    {
        bool execute() override;

        bool prepare();
        bool executeImpl();

        /// NOTE: Using pointer-to-member instead of std::function and lambda makes stacktraces much more concise and readable
        using ExecuteAndFinalizeHorizontalPartSubtasks = std::array<bool(ExecuteAndFinalizeHorizontalPart::*)(), 2>;

        const ExecuteAndFinalizeHorizontalPartSubtasks subtasks
        {
            &ExecuteAndFinalizeHorizontalPart::prepare,
            &ExecuteAndFinalizeHorizontalPart::executeImpl
        };

        ExecuteAndFinalizeHorizontalPartSubtasks::const_iterator subtasks_iterator = subtasks.begin();


        MergeAlgorithm chooseMergeAlgorithm() const;
        void createMergedStream();
        void extractMergingAndGatheringColumns() const;

        void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
        {
            ctx = static_pointer_cast<ExecuteAndFinalizeHorizontalPartRuntimeContext>(local);
            global_ctx = static_pointer_cast<GlobalRuntimeContext>(global);
        }

        StageRuntimeContextPtr getContextForNextStage() override;

        ExecuteAndFinalizeHorizontalPartRuntimeContextPtr ctx;
        GlobalRuntimeContextPtr global_ctx;
    };

    /// By default this context is uninitialized, but some variables has to be set after construction,
    /// some variables are used in a process of execution
    /// Proper initialization is responsibility of the author
    struct VerticalMergeRuntimeContext : public IStageRuntimeContext
    {
        /// Begin dependencies from previous stage
        std::unique_ptr<WriteBufferFromFileBase> rows_sources_uncompressed_write_buf{nullptr};
        std::unique_ptr<WriteBuffer> rows_sources_write_buf{nullptr};
        std::optional<ColumnSizeEstimator> column_sizes;
        CompressionCodecPtr compression_codec;
        TemporaryDataOnDiskPtr tmp_disk{nullptr};
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
        std::optional<Pipe> prepared_pipe;
        size_t max_delayed_streams = 0;
        bool use_prefetch = false;
        std::list<std::unique_ptr<MergedColumnOnlyOutputStream>> delayed_streams;
        size_t column_elems_written{0};
        QueryPipeline column_parts_pipeline;
        std::unique_ptr<PullingPipelineExecutor> executor;
        std::unique_ptr<CompressedReadBufferFromFile> rows_sources_read_buf{nullptr};
    };

    using VerticalMergeRuntimeContextPtr = std::shared_ptr<VerticalMergeRuntimeContext>;


    struct VerticalMergeStage : public IStage
    {
        bool execute() override;
        void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
        {
            ctx = static_pointer_cast<VerticalMergeRuntimeContext>(local);
            global_ctx = static_pointer_cast<GlobalRuntimeContext>(global);
        }
        StageRuntimeContextPtr getContextForNextStage() override;

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

        Pipe createPipeForReadingOneColumn(const String & column_name) const;

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
    };

    using MergeProjectionsRuntimeContextPtr = std::shared_ptr<MergeProjectionsRuntimeContext>;

    struct MergeProjectionsStage : public IStage
    {
        bool execute() override;
        void setRuntimeContext(StageRuntimeContextPtr local, StageRuntimeContextPtr global) override
        {
            ctx = static_pointer_cast<MergeProjectionsRuntimeContext>(local);
            global_ctx = static_pointer_cast<GlobalRuntimeContext>(global);
        }
        StageRuntimeContextPtr getContextForNextStage() override { return nullptr; }

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

    static bool enabledBlockNumberColumn(GlobalRuntimeContextPtr global_ctx)
    {
        return global_ctx->data->getSettings()->enable_block_number_column && global_ctx->metadata_snapshot->getGroupByTTLs().empty();
    }

    static bool enabledBlockOffsetColumn(GlobalRuntimeContextPtr global_ctx)
    {
        return global_ctx->data->getSettings()->enable_block_offset_column && global_ctx->metadata_snapshot->getGroupByTTLs().empty();
    }

    static void addGatheringColumn(GlobalRuntimeContextPtr global_ctx, const String & name, const DataTypePtr & type);
};

/// FIXME
[[ maybe_unused]] static MergeTreeData::MutableDataPartPtr executeHere(MergeTaskPtr task)
{
    while (task->execute()) {}
    return task->getFuture().get();
}

}
