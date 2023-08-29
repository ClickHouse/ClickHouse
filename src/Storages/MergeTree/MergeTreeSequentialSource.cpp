#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <Processors/Transforms/FilterTransform.h>
#include <Processors/QueryPlan/ISourceStep.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>
#include <Processors/Sources/NullSource.h>
#include <Processors/QueryPlan/QueryPlan.h>
#include <Processors/QueryPlan/FilterStep.h>
#include <Common/logger_useful.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}


/// Lightweight (in terms of logic) stream for reading single part from MergeTree
/// NOTE:
///  It doesn't filter out rows that are deleted with lightweight deletes.
///  Use createMergeTreeSequentialSource filter out those rows.
class MergeTreeSequentialSource : public ISource
{
public:
    MergeTreeSequentialSource(
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        MergeTreeData::DataPartPtr data_part_,
        Names columns_to_read_,
        std::optional<MarkRanges> mark_ranges_,
        bool apply_deleted_mask,
        bool read_with_direct_io_,
        bool take_column_types_from_storage,
        bool quiet = false);

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

    /// Columns we have to read (each Block from read will contain them)
    Names columns_to_read;

    /// Should read using direct IO
    bool read_with_direct_io;

    Poco::Logger * log = &Poco::Logger::get("MergeTreeSequentialSource");

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
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    MergeTreeData::DataPartPtr data_part_,
    Names columns_to_read_,
    std::optional<MarkRanges> mark_ranges_,
    bool apply_deleted_mask,
    bool read_with_direct_io_,
    bool take_column_types_from_storage,
    bool quiet)
    : ISource(storage_snapshot_->getSampleBlockForColumns(columns_to_read_))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , data_part(std::move(data_part_))
    , columns_to_read(std::move(columns_to_read_))
    , read_with_direct_io(read_with_direct_io_)
    , mark_ranges(std::move(mark_ranges_))
    , mark_cache(storage.getContext()->getMarkCache())
{
    if (!quiet)
    {
        /// Print column name but don't pollute logs in case of many columns.
        if (columns_to_read.size() == 1)
            LOG_DEBUG(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part, column {}",
                data_part->getMarksCount(), data_part->name, data_part->rows_count, columns_to_read.front());
        else
            LOG_DEBUG(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part",
                data_part->getMarksCount(), data_part->name, data_part->rows_count);
    }

    auto alter_conversions = storage.getAlterConversionsForPart(data_part);

    /// Note, that we don't check setting collaborate_with_coordinator presence, because this source
    /// is only used in background merges.
    addTotalRowsApprox(data_part->rows_count);

    /// Add columns because we don't want to read empty blocks
    injectRequiredColumns(
        LoadedMergeTreeDataPartInfoForReader(data_part, alter_conversions),
        storage_snapshot,
        storage.supportsSubcolumns(),
        columns_to_read);

    NamesAndTypesList columns_for_reader;
    if (take_column_types_from_storage)
    {
        auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
            .withExtendedObjects()
            .withSystemColumns();
        if (storage.supportsSubcolumns())
            options.withSubcolumns();
        columns_for_reader = storage_snapshot->getColumnsByNames(options, columns_to_read);
    }
    else
    {
        /// take columns from data_part
        columns_for_reader = data_part->getColumns().addTypes(columns_to_read);
    }

    ReadSettings read_settings;
    if (read_with_direct_io)
        read_settings.direct_io_threshold = 1;
    read_settings.read_from_filesystem_cache_if_exists_otherwise_bypass_cache = true;

    MergeTreeReaderSettings reader_settings =
    {
        .read_settings = read_settings,
        .save_marks_in_cache = false,
        .apply_deleted_mask = apply_deleted_mask,
    };

    if (!mark_ranges)
        mark_ranges.emplace(MarkRanges{MarkRange(0, data_part->getMarksCount())});

    reader = data_part->getReader(
        columns_for_reader, storage_snapshot,
        *mark_ranges, /* uncompressed_cache = */ nullptr,
        mark_cache.get(), alter_conversions, reader_settings, {}, {});
}

Chunk MergeTreeSequentialSource::generate()
try
{
    const auto & header = getPort().getHeader();

    if (!isCancelled() && current_row < data_part->rows_count)
    {
        size_t rows_to_read = data_part->index_granularity.getMarkRows(current_mark);
        bool continue_reading = (current_mark != 0);

        const auto & sample = reader->getColumns();
        Columns columns(sample.size());
        size_t rows_read = reader->readRows(current_mark, data_part->getMarksCount(), continue_reading, rows_to_read, columns);

        if (rows_read)
        {
            current_row += rows_read;
            current_mark += (rows_to_read == rows_read);

            bool should_evaluate_missing_defaults = false;
            reader->fillMissingColumns(columns, should_evaluate_missing_defaults, rows_read);

            if (should_evaluate_missing_defaults)
            {
                reader->evaluateMissingDefaults({}, columns);
            }

            reader->performRequiredConversions(columns);

            /// Reorder columns and fill result block.
            size_t num_columns = sample.size();
            Columns res_columns;
            res_columns.reserve(num_columns);

            auto it = sample.begin();
            for (size_t i = 0; i < num_columns; ++i)
            {
                if (header.has(it->name))
                    res_columns.emplace_back(std::move(columns[i]));

                ++it;
            }

            return Chunk(std::move(res_columns), rows_read);
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
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
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
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::DataPartPtr data_part,
    Names columns_to_read,
    bool read_with_direct_io,
    bool take_column_types_from_storage,
    bool quiet,
    std::shared_ptr<std::atomic<size_t>> filtered_rows_count)
{
    /// The part might have some rows masked by lightweight deletes
    const bool need_to_filter_deleted_rows = data_part->hasLightweightDelete();
    auto columns = columns_to_read;
    if (need_to_filter_deleted_rows)
        columns.emplace_back(LightweightDeleteDescription::FILTER_COLUMN.name);

    auto column_part_source = std::make_shared<MergeTreeSequentialSource>(
        storage, storage_snapshot, data_part, columns, std::optional<MarkRanges>{},
        /*apply_deleted_mask=*/ false, read_with_direct_io, take_column_types_from_storage, quiet);

    Pipe pipe(std::move(column_part_source));

    /// Add filtering step that discards deleted rows
    if (need_to_filter_deleted_rows)
    {
        pipe.addSimpleTransform([filtered_rows_count](const Block & header)
        {
            return std::make_shared<FilterTransform>(
                header, nullptr, LightweightDeleteDescription::FILTER_COLUMN.name, true, false, filtered_rows_count);
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
        const MergeTreeData & storage_,
        const StorageSnapshotPtr & storage_snapshot_,
        MergeTreeData::DataPartPtr data_part_,
        Names columns_to_read_,
        bool apply_deleted_mask_,
        ActionsDAGPtr filter_,
        ContextPtr context_,
        Poco::Logger * log_)
        : ISourceStep(DataStream{.header = storage_snapshot_->getSampleBlockForColumns(columns_to_read_)})
        , storage(storage_)
        , storage_snapshot(storage_snapshot_)
        , data_part(std::move(data_part_))
        , columns_to_read(std::move(columns_to_read_))
        , apply_deleted_mask(apply_deleted_mask_)
        , filter(std::move(filter_))
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
            KeyCondition key_condition(filter, context, primary_key_column_names, primary_key.expression, NameSet{});
            LOG_DEBUG(log, "Key condition: {}", key_condition.toString());

            if (!key_condition.alwaysFalse())
                mark_ranges = MergeTreeDataSelectExecutor::markRangesFromPKRange(
                    data_part, metadata_snapshot, key_condition, context->getSettingsRef(), log);

            if (mark_ranges && mark_ranges->empty())
            {
                pipeline.init(Pipe(std::make_unique<NullSource>(output_stream->header)));
                return;
            }
        }

        auto source = std::make_unique<MergeTreeSequentialSource>(
            storage, storage_snapshot, data_part, columns_to_read,
            std::move(mark_ranges), apply_deleted_mask, false, true);

        pipeline.init(Pipe(std::move(source)));
    }

private:
    const MergeTreeData & storage;
    StorageSnapshotPtr storage_snapshot;
    MergeTreeData::DataPartPtr data_part;
    Names columns_to_read;
    bool apply_deleted_mask;
    ActionsDAGPtr filter;
    ContextPtr context;
    Poco::Logger * log;
};

void createMergeTreeSequentialSource(
    QueryPlan & plan,
    const MergeTreeData & storage,
    const StorageSnapshotPtr & storage_snapshot,
    MergeTreeData::DataPartPtr data_part,
    Names columns_to_read,
    bool apply_deleted_mask,
    ActionsDAGPtr filter,
    ContextPtr context,
    Poco::Logger * log)
{
    auto reading = std::make_unique<ReadFromPart>(
        storage, storage_snapshot, std::move(data_part),
        std::move(columns_to_read), apply_deleted_mask,
        filter, std::move(context), log);

    plan.addStep(std::move(reading));
}

}
