#include <Storages/MergeTree/MergeTreeSequentialBlockInputStream.h>
#include <Storages/MergeTree/MergeTreeBlockReadUtils.h>

namespace DB
{
namespace ErrorCodes
{
    extern const int MEMORY_LIMIT_EXCEEDED;
}

MergeTreeSequentialBlockInputStream::MergeTreeSequentialBlockInputStream(
    const MergeTreeData & storage_,
    const MergeTreeData::DataPartPtr & data_part_,
    Names columns_to_read_,
    bool read_with_direct_io_,
    bool take_column_types_from_storage,
    bool quiet)
    : storage(storage_)
    , data_part(data_part_)
    , part_columns_lock(data_part->columns_lock)
    , columns_to_read(columns_to_read_)
    , read_with_direct_io(read_with_direct_io_)
    , mark_cache(storage.global_context.getMarkCache())
{
    if (!quiet)
    {
        std::stringstream message;
        message << "Reading " << data_part->getMarksCount() << " marks from part " << data_part->name
            << ", total " << data_part->rows_count
            << " rows starting from the beginning of the part";

        LOG_TRACE(log, message.rdbuf());
    }

    addTotalRowsApprox(data_part->rows_count);

    header = storage.getSampleBlockForColumns(columns_to_read);

    /// Add columns because we don't want to read empty blocks
    injectRequiredColumns(storage, data_part, columns_to_read);
    NamesAndTypesList columns_for_reader;
    if (take_column_types_from_storage)
    {
        const NamesAndTypesList & physical_columns = storage.getColumns().getAllPhysical();
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

    reader = data_part->getReader(columns_for_reader,
        MarkRanges{MarkRange(0, data_part->getMarksCount())},
        /* uncompressed_cache = */ nullptr, mark_cache.get(), reader_settings);
}


void MergeTreeSequentialBlockInputStream::fixHeader(Block & header_block) const
{
    /// Types may be different during ALTER (when this stream is used to perform an ALTER).
    for (const auto & name_type : data_part->getColumns())
    {
        if (header_block.has(name_type.name))
        {
            auto & elem = header_block.getByName(name_type.name);
            if (!elem.type->equals(*name_type.type))
            {
                elem.type = name_type.type;
                elem.column = elem.type->createColumn();
            }
        }
    }
}

Block MergeTreeSequentialBlockInputStream::getHeader() const
{
    return header;
}

Block MergeTreeSequentialBlockInputStream::readImpl()
try
{
    Block res;
    if (!isCancelled() && current_row < data_part->rows_count)
    {
        size_t rows_to_read = data_part->index_granularity.getMarkRows(current_mark);
        bool continue_reading = (current_mark != 0);

        auto & sample = reader->getColumns();
        Columns columns(sample.size());
        size_t rows_readed = reader->readRows(current_mark, continue_reading, rows_to_read, columns);

        if (rows_readed)
        {
            current_row += rows_readed;
            current_mark += (rows_to_read == rows_readed);

            bool should_evaluate_missing_defaults = false;
            reader->fillMissingColumns(columns, should_evaluate_missing_defaults, rows_readed);

            if (should_evaluate_missing_defaults)
            {
                reader->evaluateMissingDefaults({}, columns);
            }

            reader->performRequiredConversions(columns);

            res = header.cloneEmpty();

            /// Reorder columns and fill result block.
            size_t num_columns = sample.size();
            auto it = sample.begin();
            for (size_t i = 0; i < num_columns; ++i)
            {
                if (res.has(it->name))
                    res.getByName(it->name).column = std::move(columns[i]);

                ++it;
            }

            res.checkNumberOfRows();
        }
    }
    else
    {
        finish();
    }

    return res;
}
catch (...)
{
    /// Suspicion of the broken part. A part is added to the queue for verification.
    if (getCurrentExceptionCode() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
        storage.reportBrokenPart(data_part->name);
    throw;
}


void MergeTreeSequentialBlockInputStream::finish()
{
    /** Close the files (before destroying the object).
     * When many sources are created, but simultaneously reading only a few of them,
     * buffers don't waste memory.
     */
    reader.reset();
    part_columns_lock.unlock();
    data_part.reset();
}


MergeTreeSequentialBlockInputStream::~MergeTreeSequentialBlockInputStream() = default;

}
