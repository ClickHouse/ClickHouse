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
#include <Processors/Merges/Algorithms/MergeTreePartLevelInfo.h>
#include <Storages/MergeTree/checkDataPart.h>

namespace DB
{

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
        MergeTreeSequentialSourceType type,
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        MergeTreeData::DataPartPtr data_part_,
        AlterConversionsPtr alter_conversions_,
        Names columns_to_read_,
        std::optional<MarkRanges> mark_ranges_,
        bool apply_deleted_mask,
        bool read_with_direct_io_,
        bool prefetch);

    ~MergeTreeSequentialSource() override;

    String getName() const override { return "MergeTreeSequentialSource"; }

    size_t getCurrentMark() const { return current_mark; }

    size_t getCurrentRow() const { return current_row; }

protected:
    Chunk generate() override;

private:
    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;

    /// Data part will not be removed if the pointer owns it
    MergeTreeData::DataPartPtr data_part;

    /// Alter and mutation commands that are required to be applied to the part on-fly.
    AlterConversionsPtr alter_conversions;

    /// Columns we have to read (each Block from read will contain them)
    Names columns_to_read;

    /// Should read using direct IO
    bool read_with_direct_io;

    LoggerPtr log = getLogger("MergeTreeSequentialSource");

    std::optional<MarkRanges> mark_ranges;

    std::shared_ptr<MarkCache> mark_cache;
    using MergeTreeReaderPtr = std::unique_ptr<IMergeTreeReader>;
    MergeTreeReaderPtr reader;

    /// current mark at which we stop reading
    size_t current_mark = 0;

    /// current row at which we stop reading
    size_t current_row = 0;

    /// Closes readers and unlock part locks
    void finish();
};

MergeTreeSequentialSource::MergeTreeSequentialSource(
    MergeTreeSequentialSourceType type,
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    MergeTreeData::DataPartPtr data_part_,
    AlterConversionsPtr alter_conversions_,
    Names columns_to_read_,
    std::optional<MarkRanges> mark_ranges_,
    bool apply_deleted_mask,
    bool read_with_direct_io_,
    bool prefetch)
    : ISource(storage_snapshot_->getSampleBlockForColumns(columns_to_read_))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , data_part(std::move(data_part_))
    , alter_conversions(std::move(alter_conversions_))
    , columns_to_read(std::move(columns_to_read_))
    , read_with_direct_io(read_with_direct_io_)
    , mark_ranges(std::move(mark_ranges_))
    , mark_cache(storage.getContext()->getMarkCache())
{
    /// Print column name but don't pollute logs in case of many columns.
    if (columns_to_read.size() == 1)
        LOG_DEBUG(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part, column {}",
            data_part->getMarksCount(), data_part->name, data_part->rows_count, columns_to_read.front());
    else
        LOG_DEBUG(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part",
            data_part->getMarksCount(), data_part->name, data_part->rows_count);

    /// Note, that we don't check setting collaborate_with_coordinator presence, because this source
    /// is only used in background merges.
    addTotalRowsApprox(data_part->rows_count);

    /// Add columns because we don't want to read empty blocks
    injectRequiredColumns(
        LoadedMergeTreeDataPartInfoForReader(data_part, alter_conversions),
        storage_snapshot,
        storage.supportsSubcolumns(),
        columns_to_read);

    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
        .withExtendedObjects()
        .withVirtuals()
        .withSubcolumns(storage.supportsSubcolumns());

    auto columns_for_reader = storage_snapshot->getColumnsByNames(options, columns_to_read);

    const auto & context = storage.getContext();
    ReadSettings read_settings = context->getReadSettings();
    read_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = !(*storage.getSettings())[MergeTreeSetting::force_read_through_cache_for_merges];

    /// It does not make sense to use pthread_threadpool for background merges/mutations
    /// And also to preserve backward compatibility
    read_settings.local_fs_method = LocalFSReadMethod::pread;
    if (read_with_direct_io)
        read_settings.direct_io_threshold = 1;

    /// Configure throttling
    switch (type)
    {
        case Mutation:
            read_settings.local_throttler = context->getMutationsThrottler();
            break;
        case Merge:
            read_settings.local_throttler = context->getMergesThrottler();
            break;
    }
    read_settings.remote_throttler = read_settings.local_throttler;

    MergeTreeReaderSettings reader_settings =
    {
        .read_settings = read_settings,
        .save_marks_in_cache = false,
        .apply_deleted_mask = apply_deleted_mask,
        .can_read_part_without_marks = true,
    };

    if (!mark_ranges)
        mark_ranges.emplace(MarkRanges{MarkRange(0, data_part->getMarksCount())});

    reader = data_part->getReader(
        columns_for_reader,
        storage_snapshot,
        *mark_ranges,
        /*virtual_fields=*/ {},
        /*uncompressed_cache=*/ {},
        mark_cache.get(),
        alter_conversions,
        reader_settings,
        /*avg_value_size_hints=*/ {},
        /*profile_callback=*/ {});

    if (prefetch && !data_part->isEmpty())
        reader->prefetchBeginOfRange(Priority{});
}

static void fillBlockNumberColumns(
    Columns & res_columns,
    const NamesAndTypesList & columns_list,
    UInt64 block_number,
    UInt64 block_offset,
    UInt64 num_rows)
{
    chassert(res_columns.size() == columns_list.size());

    auto it = columns_list.begin();
    for (size_t i = 0; i < res_columns.size(); ++i, ++it)
    {
        if (res_columns[i])
            continue;

        if (it->name == BlockNumberColumn::name)
        {
            res_columns[i] = BlockNumberColumn::type->createColumnConst(num_rows, block_number)->convertToFullColumnIfConst();
        }
        else if (it->name == BlockOffsetColumn::name)
        {
            auto column = BlockOffsetColumn::type->createColumn();
            auto & block_offset_data = assert_cast<ColumnUInt64 &>(*column).getData();

            block_offset_data.resize(num_rows);
            std::iota(block_offset_data.begin(), block_offset_data.end(), block_offset);

            res_columns[i] = std::move(column);
        }
    }
}

Chunk MergeTreeSequentialSource::generate()
try
{
    const auto & header = getPort().getHeader();
    /// Part level is useful for next step for merging non-merge tree table
    bool add_part_level = storage.merging_params.mode != MergeTreeData::MergingParams::Ordinary;
    size_t num_marks_in_part = data_part->getMarksCount();

    if (!isCancelled() && current_row < data_part->rows_count)
    {
        size_t rows_to_read = data_part->index_granularity.getMarkRows(current_mark);
        bool continue_reading = (current_mark != 0);

        const auto & sample = reader->getColumns();
        Columns columns(sample.size());
        size_t rows_read = reader->readRows(current_mark, num_marks_in_part, continue_reading, rows_to_read, columns);

        if (rows_read)
        {
            fillBlockNumberColumns(columns, sample, data_part->info.min_block, current_row, rows_read);
            reader->fillVirtualColumns(columns, rows_read);

            current_row += rows_read;
            current_mark += (rows_to_read == rows_read);

            bool should_evaluate_missing_defaults = false;
            reader->fillMissingColumns(columns, should_evaluate_missing_defaults, rows_read);

            reader->performRequiredConversions(columns);

            if (should_evaluate_missing_defaults)
                reader->evaluateMissingDefaults({}, columns);

            /// Reorder columns and fill result block.
            size_t num_columns = sample.size();
            Columns res_columns;
            res_columns.reserve(num_columns);

            auto it = sample.begin();
            for (size_t i = 0; i < num_columns; ++i)
            {
                if (header.has(it->name))
                {
                    columns[i]->assumeMutableRef().shrinkToFit();
                    res_columns.emplace_back(std::move(columns[i]));
                }

                ++it;
            }

            auto result = Chunk(std::move(res_columns), rows_read);
            if (add_part_level)
                result.getChunkInfos().add(std::make_shared<MergeTreePartLevelInfo>(data_part->info.level));
            return result;
        }
    }
    else
    {
        finish();
    }

    return {};
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (!isRetryableException(std::current_exception()))
        storage.reportBrokenPart(data_part);
    throw;
}

void MergeTreeSequentialSource::finish()
{
    /** Close the files (before destroying the object).
     * When many sources are created, but simultaneously reading only a few of them,
     * buffers don't waste memory.
     */
    reader.reset();
    data_part.reset();
}

MergeTreeSequentialSource::~MergeTreeSequentialSource() = default;


Pipe createMergeTreeSequentialSource(
    MergeTreeSequentialSourceType type,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::DataPartPtr data_part,
    AlterConversionsPtr alter_conversions,
    Names columns_to_read,
    std::optional<MarkRanges> mark_ranges,
    std::shared_ptr<std::atomic<size_t>> filtered_rows_count,
    bool apply_deleted_mask,
    bool read_with_direct_io,
    bool prefetch)
{

    /// The part might have some rows masked by lightweight deletes
    const bool need_to_filter_deleted_rows = apply_deleted_mask && data_part->hasLightweightDelete();
    const bool has_filter_column = std::ranges::find(columns_to_read, RowExistsColumn::name) != columns_to_read.end();

    if (need_to_filter_deleted_rows && !has_filter_column)
        columns_to_read.emplace_back(RowExistsColumn::name);

    auto column_part_source = std::make_shared<MergeTreeSequentialSource>(type,
        storage, storage_snapshot, data_part, alter_conversions,
        columns_to_read, std::move(mark_ranges),
        /*apply_deleted_mask=*/ false, read_with_direct_io, prefetch);

    Pipe pipe(std::move(column_part_source));

    /// Add filtering step that discards deleted rows
    if (need_to_filter_deleted_rows)
    {
        pipe.addSimpleTransform([filtered_rows_count, has_filter_column](const Block & header)
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
        MergeTreeData::DataPartPtr data_part_,
        AlterConversionsPtr alter_conversions_,
        Names columns_to_read_,
        std::shared_ptr<std::atomic<size_t>> filtered_rows_count_,
        bool apply_deleted_mask_,
        std::optional<ActionsDAG> filter_,
        bool read_with_direct_io_,
        bool prefetch_,
        ContextPtr context_,
        LoggerPtr log_)
        : ISourceStep(storage_snapshot_->getSampleBlockForColumns(columns_to_read_))
        , type(type_)
        , storage(storage_)
        , storage_snapshot(storage_snapshot_)
        , data_part(std::move(data_part_))
        , alter_conversions(std::move(alter_conversions_))
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

    String getName() const override { return fmt::format("ReadFromPart({})", data_part->name); }

    void initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &) override
    {
        std::optional<MarkRanges> mark_ranges;

        const auto & metadata_snapshot = storage_snapshot->metadata;
        if (filter && metadata_snapshot->hasPrimaryKey())
        {
            const auto & primary_key = storage_snapshot->metadata->getPrimaryKey();
            const Names & primary_key_column_names = primary_key.column_names;
            KeyCondition key_condition(&*filter, context, primary_key_column_names, primary_key.expression);
            LOG_DEBUG(log, "Key condition: {}", key_condition.toString());

            if (!key_condition.alwaysFalse())
                mark_ranges = MergeTreeDataSelectExecutor::markRangesFromPKRange(
                    data_part,
                    metadata_snapshot,
                    key_condition,
                    /*part_offset_condition=*/{},
                    /*exact_ranges=*/nullptr,
                    context->getSettingsRef(),
                    log);

            if (mark_ranges && mark_ranges->empty())
            {
                pipeline.init(Pipe(std::make_unique<NullSource>(*output_header)));
                return;
            }
        }

        auto source = createMergeTreeSequentialSource(type,
            storage,
            storage_snapshot,
            data_part,
            alter_conversions,
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
    const MergeTreeData::DataPartPtr data_part;
    const AlterConversionsPtr alter_conversions;
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
    MergeTreeData::DataPartPtr data_part,
    AlterConversionsPtr alter_conversions,
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
