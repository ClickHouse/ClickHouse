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
    initMarksLoader();
    size_t buffer_size = settings.max_read_buffer_size;

    if (uncompressed_cache)
    {
        auto buffer = std::make_unique<CachedCompressedReadBuffer>(
            path + "data.bin", uncompressed_cache, 0, settings.min_bytes_to_use_direct_io, buffer_size);

        // if (profile_callback)
        //     buffer->setProfileCallback(profile_callback, clock_type);

        cached_buffer = std::move(buffer);
        data_buffer = cached_buffer.get();
    }
    else
    {
        auto buffer = std::make_unique<CompressedReadBufferFromFile>(
            path + "data.bin", 0, settings.min_bytes_to_use_direct_io, buffer_size);

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

    /// FIXME compute correct granularity
    size_t rows_to_read = data_part->index_granularity.getMarkRows(from_mark);
    size_t read_rows = 0;

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
            size_t column_position = data_part->getColumnPosition(it.name);

            readData(it.name, *it.type, *column, from_mark, column_position, rows_to_read);

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
    std::cerr << "(MergeTreeReaderCompact::readData) from_mark: " << from_mark << "\n";
    std::cerr << "(MergeTreeReaderCompact::readData) column_position: " << column_position << "\n";
    std::cerr << "(MergeTreeReaderCompact::readData) rows_to_read: " << rows_to_read << "\n";
    std::cerr << "(MergeTreeReaderCompact::readData) start reading column: " << name << "\n";

    seekToMark(from_mark, column_position);
    
    IDataType::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.getter = [&](IDataType::SubstreamPath) -> ReadBuffer * { return data_buffer; };
    deserialize_settings.avg_value_size_hint = avg_value_size_hints[name];
    deserialize_settings.position_independent_encoding = false;

    IDataType::DeserializeBinaryBulkStatePtr state;
    type.deserializeBinaryBulkStatePrefix(deserialize_settings, state);
    type.deserializeBinaryBulkWithMultipleStreams(column, rows_to_read, deserialize_settings, state);

    std::cerr << "(MergeTreeReaderCompact::readData) end reading column rows: " << column.size() << "\n";
    std::cerr << "(MergeTreeReaderCompact::readData) end reading column: " << name << "\n";

    // if (column.size() != rows_to_read)
    //     throw Exception("Cannot read all data in NativeBlockInputStream. Rows read: " + toString(column.size()) + ". Rows expected: "+ toString(rows_to_read) + ".",
    //         ErrorCodes::CANNOT_READ_ALL_DATA);
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

        std::cerr << "(initMarksLoader) marks_count: " << marks_count << "\n";
        std::cerr << "() mark_size_in_bytes: " << mark_size_in_bytes << "\n";

        size_t expected_file_size = mark_size_in_bytes * marks_count;
        if (expected_file_size != file_size)
            throw Exception(
                "Bad size of marks file '" + mrk_path + "': " + std::to_string(file_size) + ", must be: " + std::to_string(expected_file_size),
                ErrorCodes::CORRUPTED_DATA);

        /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
        auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();


        auto res = std::make_shared<MarksInCompressedFile>(marks_count * columns_num);

        std::cerr << "(MergeTreeReaderCompact::loadMarks) marks_count: " << marks_count << "\n";

        ReadBufferFromFile buffer(mrk_path, file_size);
        size_t i = 0;

        while (!buffer.eof())
        {
            buffer.seek(sizeof(size_t), SEEK_CUR);
            buffer.readStrict(reinterpret_cast<char *>(res->data() + i * columns_num), sizeof(MarkInCompressedFile) * columns_num);
            std::cerr << "(MergeTreeReaderCompact::loadMarks) i: " << i << "\n";
            std::cerr << "(MergeTreeReaderCompact::loadMarks) buffer pos in file: " << buffer.getPositionInFile() << "\n";
            ++i;
        }

        std::cerr << "(MergeTreeReaderCompact::loadMarks) file_size: " << file_size << "\n";
        std::cerr << "(MergeTreeReaderCompact::loadMarks) correct file size: " << i * mark_size_in_bytes << "\n";

        if (i * mark_size_in_bytes != file_size)
            throw Exception("Cannot read all marks from file " + mrk_path, ErrorCodes::CANNOT_READ_ALL_DATA);

        res->protect();
        return res;
    };

    auto mrk_path = data_part->index_granularity_info.getMarksFilePath(path + NAME_OF_FILE_WITH_DATA);
    marks_loader = MergeTreeMarksLoader{mark_cache, std::move(mrk_path), load, settings.save_marks_in_cache, columns_num};
}

void MergeTreeReaderCompact::seekToMark(size_t row_index, size_t column_index)
{
    MarkInCompressedFile mark = marks_loader.getMark(row_index, column_index);

    std::cerr << "(MergeTreeReaderCompact::seekToMark) mark: (" << mark.offset_in_compressed_file << ", " << mark.offset_in_decompressed_block << "\n";

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

