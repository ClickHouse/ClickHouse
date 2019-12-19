#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int ARGUMENT_OUT_OF_BOUND;
}

MergeTreeReaderCompact::MergeTreeReaderCompact(const MergeTreeData::DataPartPtr & data_part_,
    const NamesAndTypesList & columns_, UncompressedCache * uncompressed_cache_, MarkCache * mark_cache_,
    const MarkRanges & mark_ranges_, const MergeTreeReaderSettings & settings_, const ValueSizeMap & avg_value_size_hints_)
    : IMergeTreeReader(data_part_, columns_
    , uncompressed_cache_, mark_cache_, mark_ranges_
    , settings_, avg_value_size_hints_)
{
    initMarksLoader();
    size_t buffer_size = settings.max_read_buffer_size;
    const String full_data_path = path + MergeTreeDataPartCompact::DATA_FILE_NAME + MergeTreeDataPartCompact::DATA_FILE_EXTENSION;

    if (uncompressed_cache)
    {
        auto buffer = std::make_unique<CachedCompressedReadBuffer>(
            full_data_path, uncompressed_cache, 0, settings.min_bytes_to_use_direct_io, buffer_size);

        // if (profile_callback)
        //     buffer->setProfileCallback(profile_callback, clock_type);

        cached_buffer = std::move(buffer);
        data_buffer = cached_buffer.get();
    }
    else
    {
        auto buffer = std::make_unique<CompressedReadBufferFromFile>(
            full_data_path, 0, settings.min_bytes_to_use_direct_io, buffer_size);

        // if (profile_callback)
        //     buffer->setProfileCallback(profile_callback, clock_type);

        non_cached_buffer = std::move(buffer);
        data_buffer = non_cached_buffer.get();
    }

    column_positions.reserve(columns.size());
    for (const auto & column : columns)
        column_positions.push_back(data_part->getColumnPosition(column.name));
}

size_t MergeTreeReaderCompact::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    /// FIXME compute correct granularity

    if (continue_reading)
        from_mark = next_mark;

    size_t read_rows = 0;
    size_t num_columns = columns.size();

    while (read_rows < max_rows_to_read)
    {
        size_t rows_to_read = data_part->index_granularity.getMarkRows(from_mark);

        auto name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (!column_positions[pos])
                continue;

            auto & [name, type] = *name_and_type;
            bool append = res_columns[pos] != nullptr;
            if (!append)
                res_columns[pos] = name_and_type->type->createColumn();

            /// To keep offsets shared. TODO Very dangerous. Get rid of this.
            MutableColumnPtr column = res_columns[pos]->assumeMutable();

            try
            {
                size_t column_size_before_reading = column->size();
                readData(name, *type, *column, from_mark, *column_positions[pos], rows_to_read);
                size_t read_rows_in_column = column->size() - column_size_before_reading;

                if (read_rows_in_column < rows_to_read)
                    throw Exception("Cannot read all data in MergeTreeReaderCompact. Rows read: " + toString(read_rows_in_column) +
                        ". Rows expected: " + toString(rows_to_read) + ".", ErrorCodes::CANNOT_READ_ALL_DATA);

                /// For elements of Nested, column_size_before_reading may be greater than column size
                ///  if offsets are not empty and were already read, but elements are empty.
                /// FIXME
                // if (column->size())
                //     read_rows_in_mark = std::max(read_rows, column->size() - column_size_before_reading);
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage("(while reading column " + name + ")");
                throw;
            }

            if (column->size())
                res_columns[pos] = std::move(column);
            else
                res_columns[pos] = nullptr;
        }

        ++from_mark;
        read_rows += rows_to_read;
    }

    next_mark = from_mark;

    return read_rows;
}


void MergeTreeReaderCompact::readData(
    const String & /* name */, const IDataType & type, IColumn & column,
    size_t from_mark, size_t column_position, size_t rows_to_read)
{
    /// FIXME seek only if needed
    seekToMark(from_mark, column_position);

    IDataType::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.getter = [&](IDataType::SubstreamPath) -> ReadBuffer * { return data_buffer; };
    // deserialize_settings.avg_value_size_hint = avg_value_size_hints[name];
    deserialize_settings.position_independent_encoding = true;

    IDataType::DeserializeBinaryBulkStatePtr state;
    type.deserializeBinaryBulkStatePrefix(deserialize_settings, state);
    type.deserializeBinaryBulkWithMultipleStreams(column, rows_to_read, deserialize_settings, state);
}


void MergeTreeReaderCompact::initMarksLoader()
{
    if (marks_loader.initialized())
        return;

    size_t columns_num = data_part->columns.size();

    auto load = [this, columns_num](const String & mrk_path) -> MarkCache::MappedPtr
    {
        size_t file_size = Poco::File(mrk_path).getSize();
        size_t marks_count = data_part->getMarksCount();
        size_t mark_size_in_bytes = data_part->index_granularity_info.mark_size_in_bytes;

        size_t expected_file_size = mark_size_in_bytes * marks_count;
        if (expected_file_size != file_size)
            throw Exception(
                "Bad size of marks file '" + mrk_path + "': " + std::to_string(file_size) + ", must be: " + std::to_string(expected_file_size),
                ErrorCodes::CORRUPTED_DATA);

        /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
        auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

        auto res = std::make_shared<MarksInCompressedFile>(marks_count * columns_num);

        ReadBufferFromFile buffer(mrk_path, file_size);
        size_t i = 0;

        while (!buffer.eof())
        {
            buffer.seek(sizeof(size_t), SEEK_CUR);
            buffer.readStrict(reinterpret_cast<char *>(res->data() + i * columns_num), sizeof(MarkInCompressedFile) * columns_num);
            ++i;
        }

        if (i * mark_size_in_bytes != file_size)
            throw Exception("Cannot read all marks from file " + mrk_path, ErrorCodes::CANNOT_READ_ALL_DATA);

        res->protect();
        return res;
    };

    auto mrk_path = data_part->index_granularity_info.getMarksFilePath(path + MergeTreeDataPartCompact::DATA_FILE_NAME);
    marks_loader = MergeTreeMarksLoader{mark_cache, std::move(mrk_path), load, settings.save_marks_in_cache, columns_num};
}

void MergeTreeReaderCompact::seekToMark(size_t row_index, size_t column_index)
{
    MarkInCompressedFile mark = marks_loader.getMark(row_index, column_index);
    try
    {
        if (cached_buffer)
            cached_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
        if (non_cached_buffer)
            non_cached_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to mark (" + toString(row_index) + ", " + toString(column_index) + ")");

        throw;
    }
}


void MergeTreeReaderCompact::seekToStart()
{
    if (cached_buffer)
        cached_buffer->seek(0, 0);
    if (non_cached_buffer)
        non_cached_buffer->seek(0, 0);
}

}
