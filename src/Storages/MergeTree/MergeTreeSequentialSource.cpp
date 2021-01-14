#include <Storages/MergeTree/MergeTreeSequentialSource.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>
#include <Interpreters/Context.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeSequentialSource::MergeTreeSequentialSource(
    const MergeTreeData & storage_,
    const StorageMetadataPtr & metadata_snapshot_,
    MergeTreeData::DataPartPtr data_part_,
    Names columns_to_read_,
    bool read_with_direct_io_,
    bool take_column_types_from_storage,
    bool quiet)
    : SourceWithProgress(metadata_snapshot_->getSampleBlockForColumns(columns_to_read_, storage_.getVirtuals(), storage_.getStorageID()))
    , storage(storage_)
    , metadata_snapshot(metadata_snapshot_)
    , data_part(std::move(data_part_))
    , columns_to_read(std::move(columns_to_read_))
    , read_with_direct_io(read_with_direct_io_)
    , mark_cache(storage.global_context.getMarkCache())
{
    if (!quiet)
    {
        /// Print column name but don't pollute logs in case of many columns.
        if (columns_to_read.size() == 1)
            LOG_TRACE(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part, column {}",
                data_part->getMarksCount(), data_part->name, data_part->rows_count, columns_to_read.front());
        else
            LOG_TRACE(log, "Reading {} marks from part {}, total {} rows starting from the beginning of the part",
                data_part->getMarksCount(), data_part->name, data_part->rows_count);
    }

    addTotalRowsApprox(data_part->rows_count);

    /// Add columns because we don't want to read empty blocks
    injectRequiredColumns(storage, metadata_snapshot, data_part, columns_to_read);
    NamesAndTypesList columns_for_reader;
    if (take_column_types_from_storage)
    {
        const NamesAndTypesList & physical_columns = metadata_snapshot->getColumns().getAllPhysical();
        columns_for_reader = physical_columns.addTypes(columns_to_read);
    }
    else
    {
        /// take columns from data_part
        columns_for_reader = data_part->getColumns().addTypes(columns_to_read);
    }

    MergeTreeReaderSettings reader_settings =
    {
        /// bytes to use AIO (this is hack)
        .min_bytes_to_use_direct_io = read_with_direct_io ? 1UL : std::numeric_limits<size_t>::max(),
        .max_read_buffer_size = DBMS_DEFAULT_BUFFER_SIZE,
        .save_marks_in_cache = false
    };

    reader = data_part->getReader(columns_for_reader, metadata_snapshot,
        MarkRanges{MarkRange(0, data_part->getMarksCount())},
        /* uncompressed_cache = */ nullptr, mark_cache.get(), reader_settings);
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
        size_t rows_read = reader->readRows(current_mark, continue_reading, rows_to_read, columns);

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
        storage.reportBrokenPart(data_part->name);
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
