#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Processors/Transforms/FilterTransform.h>
#include <QueryPipeline/Pipe.h>
#include <Interpreters/Context.h>

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
    bool read_with_direct_io_,
    bool take_column_types_from_storage,
    bool quiet)
    : ISource(storage_snapshot_->getSampleBlockForColumns(columns_to_read_))
    , storage(storage_)
    , storage_snapshot(storage_snapshot_)
    , data_part(std::move(data_part_))
    , columns_to_read(std::move(columns_to_read_))
    , read_with_direct_io(read_with_direct_io_)
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

    /// Note, that we don't check setting collaborate_with_coordinator presence, because this source
    /// is only used in background merges.
    addTotalRowsApprox(data_part->rows_count);

    /// Add columns because we don't want to read empty blocks
    injectRequiredColumns(storage, storage_snapshot, data_part, /*with_subcolumns=*/ false, columns_to_read);

    NamesAndTypesList columns_for_reader;
    if (take_column_types_from_storage)
    {
        auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical)
            .withExtendedObjects()
            .withSystemColumns();
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

    MergeTreeReaderSettings reader_settings =
    {
        .read_settings = read_settings,
        .save_marks_in_cache = false
    };

    reader = data_part->getReader(columns_for_reader, storage_snapshot->metadata,
        MarkRanges{MarkRange(0, data_part->getMarksCount())},
        /* uncompressed_cache = */ nullptr, mark_cache.get(), reader_settings, {}, {});
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
        storage, storage_snapshot, data_part, columns, read_with_direct_io, take_column_types_from_storage, quiet);

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

}
