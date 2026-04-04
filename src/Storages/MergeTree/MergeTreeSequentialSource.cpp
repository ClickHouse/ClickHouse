#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>
#include <Processors/Chunk.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Common/logger_useful.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/MergeTreeReadersChain.h>
#include <Storages/MergeTree/MergeTreeReadTask.h>
#include <Storages/MergeTree/MergeTreeSelectAlgorithms.h>
#include <Storages/MergeTree/MergeTreeSelectProcessor.h>
#include <Storages/MergeTree/MergeTreeSource.h>
#include <Storages/MergeTree/PatchParts/BuildPatchJoinCacheSink.h>
#include <Storages/MergeTree/PatchParts/PatchJoinCache.h>
#include <Storages/MergeTree/PatchParts/PatchJoinReadPool.h>
#include <Storages/MergeTree/PatchParts/RangesInPatchParts.h>
#include <Processors/Executors/PipelineExecutor.h>
#include <Processors/Merges/Algorithms/MergeTreeReadInfo.h>
#include <Storages/MergeTree/MergedPartOffsets.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <Common/ThrottlerArray.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace Setting
{
    extern const SettingsNonZeroUInt64 merge_tree_min_read_task_size;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool force_read_through_cache_for_merges;
}

/// Lightweight (in terms of logic) stream for reading single part from
/// MergeTree, used for merges and mutations.
///
/// NOTE:
///  It doesn't filter out rows that are deleted with lightweight deletes.
///  Use createMergeTreeSequentialSource filter out those rows.
class MergeTreeSequentialSource : public ISource
{
public:
    MergeTreeSequentialSource(
        SharedHeader result_header,
        MergeTreeSequentialSourceType type,
        const MergeTreeData & storage_,
        StorageSnapshotPtr storage_snapshot_,
        MergeTreeReadTaskInfoPtr read_task_info_,
        std::optional<MarkRanges> mark_ranges_,
        bool read_with_direct_io_,
        bool prefetch);

    ~MergeTreeSequentialSource() override;

    String getName() const override { return "MergeTreeSequentialSource"; }

    size_t getCurrentMark() const { return current_mark; }

protected:
    Chunk generate() override;

private:
    void updateRowsToRead(size_t mark_number);

    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeReadTaskInfoPtr read_task_info;

    LoggerPtr log = getLogger("MergeTreeSequentialSource");

    MarkRanges mark_ranges;
    std::vector<MarkRanges> patch_ranges;
    std::shared_ptr<MarkCache> mark_cache;

    MergeTreeReadTask::Readers readers;
    MergeTreeReadersChain readers_chain;
    PatchJoinCachePtr patch_join_cache;

    /// Should read using direct IO
    bool read_with_direct_io;

    /// Current mark at which we stop reading
    size_t current_mark = 0;

    /// Number of rows to read from current mark
    size_t current_rows_to_read = 0;

    /// Closes readers and unlock part locks
    void finish();
};

MergeTreeSequentialSource::MergeTreeSequentialSource(
    SharedHeader result_header,
    MergeTreeSequentialSourceType type,
    const MergeTreeData & storage_,
    StorageSnapshotPtr storage_snapshot_,
    MergeTreeReadTaskInfoPtr read_task_info_,
    std::optional<MarkRanges> mark_ranges_,
    bool read_with_direct_io_,
    bool prefetch)
    : ISource(std::move(result_header))
    , storage(storage_)
    , storage_snapshot(std::move(storage_snapshot_))
    , read_task_info(std::move(read_task_info_))
    , mark_ranges(std::move(mark_ranges_).value_or(MarkRanges{MarkRange(0, read_task_info->data_part->getMarksCount())}))
    , mark_cache(storage.getContext()->getMarkCache())
    , read_with_direct_io(read_with_direct_io_)
{
    const auto & data_part = read_task_info->data_part;
    const auto & columns_to_read = read_task_info->task_columns.columns;

    /// Print column name but don't pollute logs in case of many columns.
    if (columns_to_read.size() == 1)
        LOG_DEBUG(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part, column {}",
            data_part->getMarksCount(), data_part->name, data_part->rows_count, columns_to_read.front().name);
    else
        LOG_DEBUG(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part",
            data_part->getMarksCount(), data_part->name, data_part->rows_count);

    /// Note, that we don't check setting collaborate_with_coordinator presence, because this source
    /// is only used in background merges.
    addTotalRowsApprox(data_part->rows_count);

    if (!read_task_info->patch_parts.empty())
    {
        size_t merge_tree_min_read_task_size = storage.getContext()->getSettingsRef()[Setting::merge_tree_min_read_task_size];
        RangesInPatchParts ranges_in_patch_parts(merge_tree_min_read_task_size);

        ranges_in_patch_parts.addPart(data_part, read_task_info->patch_parts, mark_ranges);
        ranges_in_patch_parts.optimize();

        patch_ranges = ranges_in_patch_parts.getRanges(data_part, read_task_info->patch_parts, mark_ranges);
        patch_join_cache = std::make_shared<PatchJoinCache>();

        /// For sequential source (merges/mutations): single bucket, single thread.
        /// Compute min/max block from data part for range-based bucket assignment.
        MergeTreeReaderSettings merge_reader_settings = MergeTreeReaderSettings::createForMergeMutation(storage.getContext()->getReadSettings());
        UInt64 min_block = 0;
        UInt64 max_block = 0;
        auto bn_stats = getPatchMinMaxStats(data_part, mark_ranges, BlockNumberColumn::name, merge_reader_settings);
        if (bn_stats)
        {
            min_block = std::numeric_limits<UInt64>::max();
            for (const auto & stat : *bn_stats)
            {
                min_block = std::min(min_block, stat.min);
                max_block = std::max(max_block, stat.max);
            }
        }

        patch_join_cache->init(ranges_in_patch_parts, /*num_buckets=*/ 1, min_block, max_block);

        /// Build a minimal pipeline: source → sink, to fill the single-bucket cache.
        MergeTreeReadTask::Extras build_extras =
        {
            .mark_cache = mark_cache.get(),
            .reader_settings = merge_reader_settings,
            .storage_snapshot = storage_snapshot,
        };

        /// Collect Join-mode patch info and build pipeline for each patch.
        auto processors = std::make_shared<Processors>();
        for (size_t patch_idx = 0; patch_idx < read_task_info->patch_parts.size(); ++patch_idx)
        {
            const auto & patch_part = read_task_info->patch_parts[patch_idx];
            if (patch_part.mode != PatchMode::Join)
                continue;

            const auto & patch_name = patch_part.part->getPartName();
            const auto & entries = patch_join_cache->getEntries(patch_name);
            if (entries.empty())
                continue;

            const auto * loaded_part = dynamic_cast<const LoadedMergeTreeDataPartInfoForReader *>(patch_part.part.get());
            if (!loaded_part)
                continue;

            auto task_info_for_patch = std::make_shared<MergeTreeReadTaskInfo>();
            task_info_for_patch->data_part = loaded_part->getDataPart();
            task_info_for_patch->alter_conversions = std::make_shared<AlterConversions>();

            /// Use column types from the patch part itself to avoid type conversion failures.
            const auto & part_columns = loaded_part->getColumns();
            NamesAndTypesList resolved_columns;
            for (const auto & col : read_task_info->task_columns.patch_columns[patch_idx])
            {
                auto part_col = part_columns.tryGetByName(col.name);
                resolved_columns.push_back(part_col ? *part_col : col);
            }
            task_info_for_patch->task_columns.columns = resolved_columns;
            task_info_for_patch->const_virtual_fields = read_task_info->const_virtual_fields;

            Block patch_header;
            for (const auto & col : resolved_columns)
                patch_header.insert(ColumnWithTypeAndName(col.type->createColumn(), col.type, col.name));

            auto shared_header = std::make_shared<Block>(patch_header);
            const auto & all_ranges = patch_join_cache->getAllRanges(patch_name);
            std::vector<MarkRange> work_ranges(all_ranges.begin(), all_ranges.end());

            auto pool = std::make_shared<PatchJoinReadPool>(
                patch_header, task_info_for_patch, build_extras,
                std::move(work_ranges), MergeTreeReadTask::BlockSizeParams{
                    .max_block_size_rows = std::numeric_limits<UInt64>::max(),
                    .preferred_block_size_bytes = 0});

            auto algorithm = std::make_unique<MergeTreeThreadSelectAlgorithm>(0);
            auto select_processor = std::make_unique<MergeTreeSelectProcessor>(
                pool, std::move(algorithm),
                /*row_level_filter=*/ nullptr,
                /*prewhere_info=*/ nullptr,
                /*index_read_tasks=*/ IndexReadTasks{},
                ExpressionActionsSettings{},
                merge_reader_settings);
            auto source = std::make_shared<MergeTreeSource>(std::move(select_processor), "PatchJoinCacheBuild");

            auto sink = std::make_shared<BuildPatchJoinCacheSink>(shared_header, entries[0]);
            connect(source->getOutputs().front(), sink->getPort());

            processors->push_back(std::move(source));
            processors->push_back(std::move(sink));
        }

        if (!processors->empty())
        {
            PipelineExecutor executor(processors, /*elem=*/ nullptr);
            executor.execute(/*num_threads=*/ 1, /*concurrency_control=*/ false);
        }
    }

    const auto & context = storage.getContext();
    ReadSettings read_settings = context->getReadSettings();
    read_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache
        = read_settings.distributed_cache_settings.read_if_exists_otherwise_bypass
        = !(*storage.getSettings())[MergeTreeSetting::force_read_through_cache_for_merges];

    /// It does not make sense to use pthread_threadpool for background merges/mutations
    /// And also to preserve backward compatibility
    read_settings.local_fs_method = LocalFSReadMethod::pread;
    if (read_with_direct_io)
        read_settings.direct_io_threshold = 1;

    /// Configure throttling
    switch (type)
    {
        case Mutation:
            addThrottler(read_settings.remote_throttler, context->getMutationsThrottler());
            addThrottler(read_settings.local_throttler, context->getMutationsThrottler());
            break;
        case Merge:
            addThrottler(read_settings.remote_throttler, context->getMergesThrottler());
            addThrottler(read_settings.local_throttler, context->getMergesThrottler());
            break;
    }

    MergeTreeReadTask::Extras extras =
    {
        .mark_cache = mark_cache.get(),
        .patch_join_cache = patch_join_cache.get(),
        .reader_settings = MergeTreeReaderSettings::createForMergeMutation(std::move(read_settings)),
        .storage_snapshot = storage_snapshot,
    };

    readers = MergeTreeReadTask::createReaders(read_task_info, extras, mark_ranges, patch_ranges);

    if (!readers.prewhere.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Sequential source doesn't support PREWHERE");

    if (prefetch && !data_part->isEmpty())
        readers.main->prefetchBeginOfRange(Priority{});

    auto counters = std::make_shared<ReadStepPerformanceCounters>();

    MergeTreeRangeReader range_reader(readers.main.get(), {}, nullptr, counters, true, readers.main->canReadIncompleteGranules());
    readers_chain = MergeTreeReadersChain{{std::move(range_reader)}, readers.patches};

    updateRowsToRead(0);
}

void MergeTreeSequentialSource::updateRowsToRead(size_t mark_number)
{
    const auto & index_granularity = read_task_info->data_part->index_granularity;
    if (mark_number < index_granularity->getMarksCountWithoutFinal())
        current_rows_to_read = index_granularity->getMarkRows(mark_number);
}

Chunk MergeTreeSequentialSource::generate()
try
{
    const auto & index_granularity = read_task_info->data_part->index_granularity;
    if (current_mark >= index_granularity->getMarksCountWithoutFinal())
    {
        finish();
        return {};
    }

    if (isCancelled())
        return {};

    auto read_result = readers_chain.read(current_rows_to_read, mark_ranges, patch_ranges);
    if (!read_result.num_rows)
        return {};

    if (read_result.num_rows > current_rows_to_read)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Read {} rows, more than requested to read: {}", read_result.num_rows, current_rows_to_read);

    current_rows_to_read -= read_result.num_rows;

    if (!current_rows_to_read)
    {
        ++current_mark;
        updateRowsToRead(current_mark);
    }

    const auto & result_header = getPort().getHeader();
    const auto & reader_header = readers_chain.getSampleBlock();

    Columns result_columns;
    result_columns.reserve(result_header.columns());

    for (size_t i = 0; i < result_header.columns(); ++i)
    {
        const auto & name = result_header.safeGetByPosition(i).name;
        auto pos = reader_header.getPositionByName(name);
        auto & result_column = result_columns.emplace_back(std::move(read_result.columns[pos]));

        /// When read_task_info->merged_part_offsets we need to adjust parent part offset in projection because it will
        /// be different when parent has order by column and merge will change order of rows.
        if (read_task_info->merged_part_offsets && read_task_info->data_part->isProjectionPart() && name == "_parent_part_offset")
        {
            chassert(read_task_info->merged_part_offsets->isFinalized());

            result_column = result_column->convertToFullColumnIfSparse();
            auto & column = result_column->assumeMutableRef();
            auto & offset_data = assert_cast<ColumnUInt64 &>(column).getData();
            if (read_task_info->merged_part_offsets->isMappingEnabled())
            {
                for (auto & offset : offset_data)
                    offset = (*read_task_info->merged_part_offsets)[read_task_info->part_index_in_query, offset];
            }
            else
            {
                for (auto & offset : offset_data)
                    offset += read_task_info->part_starting_offset_in_query;
            }
        }
        result_column->assumeMutableRef().shrinkToFit();
    }

    auto result = Chunk(std::move(result_columns), read_result.num_rows);
    /// Part level is useful for next step for merging non-merge tree table
    bool add_part_level = storage.merging_params.mode != MergeTreeData::MergingParams::Ordinary;

    if (add_part_level)
        result.getChunkInfos().add(std::make_shared<MergeTreeReadInfo>(read_task_info->data_part->info.level));

    return result;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (!isRetryableException(std::current_exception()))
        storage.reportBrokenPart(read_task_info->data_part);
    throw;
}

void MergeTreeSequentialSource::finish()
{
    /** Close the files (before destroying the object).
     * When many sources are created, but simultaneously reading only a few of them,
     * buffers don't waste memory.
     */
    readers = {};
    read_task_info.reset();
}

MergeTreeSequentialSource::~MergeTreeSequentialSource() = default;


Pipe createMergeTreeSequentialSource(
    MergeTreeSequentialSourceType type,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    RangesInDataPart data_part,
    AlterConversionsPtr alter_conversions,
    MergedPartOffsetsPtr merged_part_offsets,
    Names columns_to_read,
    std::optional<MarkRanges> mark_ranges,
    std::shared_ptr<std::atomic<size_t>> filtered_rows_count,
    bool apply_deleted_mask,
    bool read_with_direct_io,
    bool prefetch)
{
    auto info = std::make_shared<MergeTreeReadTaskInfo>();
    info->data_part = std::move(data_part.data_part);
    info->alter_conversions = std::move(alter_conversions);
    info->merged_part_offsets = std::move(merged_part_offsets);
    info->part_index_in_query = data_part.part_index_in_query;
    info->part_starting_offset_in_query = data_part.part_starting_offset_in_query;
    info->const_virtual_fields.emplace("_part_index", info->part_index_in_query);
    info->const_virtual_fields.emplace("_part_starting_offset", info->part_starting_offset_in_query);

    /// The part might have some rows masked by lightweight deletes
    bool has_lightweight_delete = info->data_part->hasLightweightDelete() || info->alter_conversions->hasLightweightDelete();
    const bool need_to_filter_deleted_rows = apply_deleted_mask && has_lightweight_delete && !info->data_part->info.isPatch();
    const bool has_filter_column = std::ranges::find(columns_to_read, RowExistsColumn::name) != columns_to_read.end();

    if (need_to_filter_deleted_rows)
    {
        if (!has_filter_column)
            columns_to_read.push_back(RowExistsColumn::name);

        info->mutation_steps.push_back(createLightweightDeleteStep(!has_filter_column));
    }

    auto result_header = std::make_shared<const Block>(storage_snapshot->getSampleBlockForColumns(columns_to_read));
    LoadedMergeTreeDataPartInfoForReader info_for_reader(info->data_part, info->alter_conversions);

    info->task_columns = getReadTaskColumnsForMerge(info_for_reader, storage_snapshot, columns_to_read, info->mutation_steps);
    info->task_columns.moveAllColumnsFromPrewhere();

    if (info->alter_conversions->hasPatches())
    {
        auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withVirtuals().withSubcolumns();
        auto all_read_columns = info->task_columns.getAllColumnNames();
        auto all_read_columns_list = storage_snapshot->getColumnsByNames(options, all_read_columns);
        info->patch_parts = info->alter_conversions->getPatchesForColumns(all_read_columns_list, need_to_filter_deleted_rows);

        addPatchPartsColumns(
            info->task_columns,
            storage_snapshot,
            options,
            info->patch_parts,
            all_read_columns,
            need_to_filter_deleted_rows);
    }

    auto column_part_source = std::make_shared<MergeTreeSequentialSource>(
        std::move(result_header),
        type,
        storage,
        storage_snapshot,
        std::move(info),
        std::move(mark_ranges),
        read_with_direct_io,
        prefetch);

    Pipe pipe(std::move(column_part_source));

    /// Add filtering step that discards deleted rows
    if (need_to_filter_deleted_rows)
    {
        pipe.addSimpleTransform([filtered_rows_count, has_filter_column](const SharedHeader & header)
        {
            return std::make_shared<FilterTransform>(
                header, nullptr, RowExistsColumn::name, !has_filter_column, false, filtered_rows_count);
        });
    }

    return pipe;
}

/// A Query Plan step to read from a single Merge Tree part
/// using Merge Tree Sequential Source (which reads strictly sequentially in a single thread).
/// This step is used for mutations because the usual reading is too tricky.
/// Previously, sequential reading was achieved by changing some settings like max_threads,
/// however, this approach lead to data corruption after some new settings were introduced.
class ReadFromPart final : public ISourceStep
{
public:
    ReadFromPart(
        MergeTreeSequentialSourceType type_,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        RangesInDataPart data_part_,
        AlterConversionsPtr alter_conversions_,
        MergedPartOffsetsPtr merged_part_offsets_,
        Names columns_to_read_,
        std::shared_ptr<std::atomic<size_t>> filtered_rows_count_,
        bool apply_deleted_mask_,
        std::optional<ActionsDAG> filter_,
        bool read_with_direct_io_,
        bool prefetch_,
        ContextPtr context_,
        LoggerPtr log_)
        : ISourceStep(std::make_shared<const Block>(storage_snapshot_->getSampleBlockForColumns(columns_to_read_)))
        , type(type_)
        , storage(storage_)
        , storage_snapshot(storage_snapshot_)
        , data_part(std::move(data_part_))
        , alter_conversions(std::move(alter_conversions_))
        , merged_part_offsets(std::move(merged_part_offsets_))
        , columns_to_read(std::move(columns_to_read_))
        , filtered_rows_count(std::move(filtered_rows_count_))
        , apply_deleted_mask(apply_deleted_mask_)
        , filter(std::move(filter_))
        , read_with_direct_io(read_with_direct_io_)
        , prefetch(prefetch_)
        , context(std::move(context_))
        , log(log_)
    {
    }

    String getName() const override { return fmt::format("ReadFromPart({})", data_part.data_part->name); }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        std::optional<MarkRanges> mark_ranges;

        const auto & metadata_snapshot = storage_snapshot->metadata;
        if (filter && metadata_snapshot->hasPrimaryKey())
        {
            const auto & primary_key = storage_snapshot->metadata->getPrimaryKey();
            const Names & primary_key_column_names = primary_key.column_names;
            ActionsDAGWithInversionPushDown filter_dag(filter->getOutputs().front(), context);
            KeyCondition key_condition(filter_dag, context, primary_key_column_names, primary_key.expression);
            LOG_DEBUG(log, "Key condition: {}", key_condition.toString());

            if (!key_condition.alwaysFalse())
                mark_ranges = MergeTreeDataSelectExecutor::markRangesFromPKRange(
                    data_part,
                    metadata_snapshot,
                    key_condition,
                    /*part_offset_condition=*/{},
                    /*total_offset_condition=*/{},
                    /*exact_ranges=*/nullptr,
                    context->getSettingsRef(),
                    log);

            if (mark_ranges && mark_ranges->empty())
            {
                pipeline.init(Pipe(std::make_unique<NullSource>(output_header)));
                return;
            }
        }

        auto source = createMergeTreeSequentialSource(type,
            storage,
            storage_snapshot,
            data_part,
            alter_conversions,
            merged_part_offsets,
            columns_to_read,
            std::move(mark_ranges),
            filtered_rows_count,
            apply_deleted_mask,
            read_with_direct_io,
            prefetch);

        pipeline.init(Pipe(std::move(source)));
    }

private:
    const MergeTreeSequentialSourceType type;
    const MergeTreeData & storage;
    const StorageSnapshotPtr storage_snapshot;
    const RangesInDataPart data_part;
    const AlterConversionsPtr alter_conversions;
    const MergedPartOffsetsPtr merged_part_offsets;
    const Names columns_to_read;
    const std::shared_ptr<std::atomic<size_t>> filtered_rows_count;
    const bool apply_deleted_mask;
    const std::optional<ActionsDAG> filter;
    const bool read_with_direct_io;
    const bool prefetch;
    const ContextPtr context;
    const LoggerPtr log;
};

void createReadFromPartStep(
    MergeTreeSequentialSourceType type,
    QueryPlan & plan,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    RangesInDataPart data_part,
    AlterConversionsPtr alter_conversions,
    MergedPartOffsetsPtr merged_part_offsets,
    Names columns_to_read,
    std::shared_ptr<std::atomic<size_t>> filtered_rows_count,
    bool apply_deleted_mask,
    std::optional<ActionsDAG> filter,
    bool read_with_direct_io,
    bool prefetch,
    ContextPtr context,
    LoggerPtr log)
{
    auto reading = std::make_unique<ReadFromPart>(
        type,
        storage,
        storage_snapshot,
        std::move(data_part),
        std::move(alter_conversions),
        std::move(merged_part_offsets),
        std::move(columns_to_read),
        filtered_rows_count,
        apply_deleted_mask,
        std::move(filter),
        read_with_direct_io,
        prefetch,
        std::move(context),
        log);

    plan.addStep(std::move(reading));
}

}
