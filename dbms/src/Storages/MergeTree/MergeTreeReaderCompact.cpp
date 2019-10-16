#include <Storages/MergeTree/MergeTreeReaderCompact.h>
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
    const MarkRanges & mark_ranges_, const ReaderSettings & settings_, const ValueSizeMap & avg_value_size_hints_)
    : IMergeTreeReader(data_part_, columns_
    , uncompressed_cache_, mark_cache_, mark_ranges_
    , settings_, avg_value_size_hints_)
{
    size_t buffer_size = settings.max_read_buffer_size;

    if (uncompressed_cache)
    {
        auto buffer = std::make_unique<CachedCompressedReadBuffer>(
            "data.bin", uncompressed_cache, 0, settings.min_bytes_to_use_direct_io, buffer_size);

        // if (profile_callback)
        //     buffer->setProfileCallback(profile_callback, clock_type);

        cached_buffer = std::move(buffer);
        data_buffer = cached_buffer.get();
    }
    else
    {
        auto buffer = std::make_unique<CompressedReadBufferFromFile>(
            "data.bin", 0, settings.min_bytes_to_use_direct_io, buffer_size);

        // if (profile_callback)
        //     buffer->setProfileCallback(profile_callback, clock_type);

        non_cached_buffer = std::move(buffer);
        data_buffer = non_cached_buffer.get();
    }
}

size_t MergeTreeReaderCompact::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Block & res)
{
    UNUSED(from_mark);
    UNUSED(continue_reading);
    UNUSED(max_rows_to_read);
    UNUSED(res);

    size_t rows_to_read = data_part->index_granularity.getMarkRows(from_mark);
    size_t read_rows = 0;

    size_t ind = 0;
    for (const auto & it : columns)
    {
        bool append = res.has(it.name);
        if (!append)
            res.insert(ColumnWithTypeAndName(it.type->createColumn(), it.type, it.name));

        /// To keep offsets shared. TODO Very dangerous. Get rid of this.
        MutableColumnPtr column = res.getByName(it.name).column->assumeMutable();

        try
        {
            size_t column_size_before_reading = column->size();

            readData(it.name, *it.type, *column, from_mark, ind++, rows_to_read);

            /// For elements of Nested, column_size_before_reading may be greater than column size
            ///  if offsets are not empty and were already read, but elements are empty.
            if (column->size())
                read_rows = std::max(read_rows, column->size() - column_size_before_reading);
        }
        catch (Exception & e)
        {
            /// Better diagnostics.
            e.addMessage("(while reading column " + it.name + ")");
            throw;
        }

        if (column->size())
            res.getByName(it.name).column = std::move(column);
        else
            res.erase(it.name);
    }

    return read_rows;
}


void MergeTreeReaderCompact::readData(
    const String & name, const IDataType & type, IColumn & column,
    size_t from_mark, size_t column_position, size_t rows_to_read)
{
    seekToMark(from_mark, column_position);
    
    IDataType::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.getter = [&](IDataType::SubstreamPath) -> ReadBuffer * { return data_buffer; };
    deserialize_settings.avg_value_size_hint = avg_value_size_hints[name];
    deserialize_settings.position_independent_encoding = false;

    IDataType::DeserializeBinaryBulkStatePtr state;
    type.deserializeBinaryBulkStatePrefix(deserialize_settings, state);
    type.deserializeBinaryBulkWithMultipleStreams(column, rows_to_read, deserialize_settings, state);

    if (column.size() != rows_to_read)
        throw Exception("Cannot read all data in NativeBlockInputStream. Rows read: " + toString(column.size()) + ". Rows expected: "+ toString(rows_to_read) + ".",
            ErrorCodes::CANNOT_READ_ALL_DATA);
}


void MergeTreeReaderCompact::loadMarks()
{
    const auto & index_granularity_info = data_part->index_granularity_info;
    size_t marks_count = data_part->getMarksCount();
    std::string mrk_path = index_granularity_info.getMarksFilePath(NAME_OF_FILE_WITH_DATA);

    auto load_func = [&]() -> MarkCache::MappedPtr
    {
        size_t file_size = Poco::File(mrk_path).getSize();
        size_t expected_file_size = index_granularity_info.mark_size_in_bytes * marks_count;
        if (expected_file_size != file_size)
            throw Exception(
                "Bad size of marks file '" + mrk_path + "': " + std::to_string(file_size) + ", must be: " + std::to_string(expected_file_size),
                ErrorCodes::CORRUPTED_DATA);

        /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
        auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

        auto res = std::make_shared<MarksInCompressedFile>(marks_count * columns.size());

        ReadBufferFromFile buffer(mrk_path, file_size);
        size_t i = 0;

        while (!buffer.eof())
        {
            buffer.seek(sizeof(size_t));
            buffer.read(marks.getRowAddress(i), marks.getRowSize());
            ++i;
        }

        if (i * index_granularity_info.mark_size_in_bytes != file_size)
            throw Exception("Cannot read all marks from file " + mrk_path, ErrorCodes::CANNOT_READ_ALL_DATA);

        res->protect();
        return res;
    };

    auto marks_array = IMergeTreeReader::loadMarks(mrk_path, load_func);
    marks = MarksInCompressedFileCompact(marks_array, columns.size());
}

const MarkInCompressedFile & MergeTreeReaderCompact::getMark(size_t row, size_t col)
{
    if (!marks.initialized())
        loadMarks();
    return marks.getMark(row, col);
}

void MergeTreeReaderCompact::seekToMark(size_t row, size_t col)
{
    MarkInCompressedFile mark = getMark(row, col);

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
            e.addMessage("(while seeking to mark (" + toString(row) + ", " + toString(col) + ")");

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

