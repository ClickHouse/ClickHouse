#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Interpreters/Context.h>
#include <Columns/FilterDescription.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

static void filterColumns(Columns & columns, const IColumn::Filter & filter)
{
    for (auto & column : columns)
    {
        if (column)
        {
            column = column->filter(filter, -1);

            if (column->empty())
            {
                columns.clear();
                return;
            }
        }
    }
}

static void filterColumns(Columns & columns, const ColumnPtr & filter)
{
    ConstantFilterDescription const_descr(*filter);
    if (const_descr.always_true)
        return;

    if (const_descr.always_false)
    {
        for (auto & col : columns)
            if (col)
                col = col->cloneEmpty();

        return;
    }

    FilterDescription descr(*filter);
    filterColumns(columns, *descr.data);
}

MergeTreeSequentialSource::MergeTreeSequentialSource(
    const MergeTreeData & storage_,
    const StorageSnapshotPtr & storage_snapshot_,
    MergeTreeData::DataPartPtr data_part_,
    Names columns_to_read_,
    bool read_with_direct_io_,
    bool take_column_types_from_storage,
    bool quiet)
    : SourceWithProgress(storage_snapshot_->getSampleBlockForColumns(columns_to_read_))
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
        auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withExtendedObjects();
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
        /* uncompressed_cache = */ nullptr, mark_cache.get(), reader_settings);
}

Chunk MergeTreeSequentialSource::generate()
try
{
    const auto & header = getPort().getHeader();

    bool need_collect_deleted_mask = reader->needCollectDeletedMask();
    size_t rows_read = 0;

    do
    {
        if (!isCancelled() && current_row < data_part->rows_count)
        {
            size_t rows_to_read = data_part->index_granularity.getMarkRows(current_mark);
            bool continue_reading = (current_mark != 0);

            const auto & sample = reader->getColumns();
            Columns columns(sample.size());
            rows_read = reader->readRows(current_mark, data_part->getMarksCount(), continue_reading, rows_to_read, columns);

            if (rows_read)
            {
                current_row += rows_read;
                current_mark += (rows_to_read == rows_read);

                if (need_collect_deleted_mask)
                {
                    size_t from_pos = current_row - rows_read;

                    /// Get deleted mask for rows_read
                    ColumnPtr deleted_mask_holder = reader->getDeletedMask(from_pos, rows_read);
                    const ColumnUInt8 * mask_filter = typeid_cast<const ColumnUInt8 *>(deleted_mask_holder.get());

                    const IColumn::Filter * deleted_rows_filter = &mask_filter->getData();

                    // Filter only if some items were deleted
                    if (auto num_deleted_rows = std::count(deleted_rows_filter->begin(), deleted_rows_filter->end(), 0))
                    {
                        const auto remaining_rows = deleted_rows_filter->size() - num_deleted_rows;

                        /// Update rows_read with actual rows in columns
                        rows_read = remaining_rows;

                        /// If we return {} here, it means finished, no reading of the following rows.
                        /// Continue to read until rows_read (remaining rows) are not zero or reach the end (REAL finish).
                        if (!rows_read)
                            continue;

                        for (auto & col : columns)
                            col = col->filter(*deleted_rows_filter, remaining_rows);
                    }
                }

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
    } while (need_collect_deleted_mask && !rows_read);

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

}
