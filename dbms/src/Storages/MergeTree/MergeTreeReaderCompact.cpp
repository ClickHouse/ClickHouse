#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
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
    const MarkRanges & mark_ranges_, const MergeTreeReaderSettings & settings_, const ValueSizeMap & avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_, clockid_t clock_type_)
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

        if (profile_callback_)
            buffer->setProfileCallback(profile_callback_, clock_type_);

        cached_buffer = std::move(buffer);
        data_buffer = cached_buffer.get();
    }
    else
    {
        auto buffer = std::make_unique<CompressedReadBufferFromFile>(
            full_data_path, 0, settings.min_bytes_to_use_direct_io, buffer_size);

        if (profile_callback_)
            buffer->setProfileCallback(profile_callback_, clock_type_);

        non_cached_buffer = std::move(buffer);
        data_buffer = non_cached_buffer.get();
    }

    size_t columns_num = columns.size();

    column_positions.resize(columns_num);
    read_only_offsets.resize(columns_num);
    auto name_and_type = columns.begin();
    for (size_t i = 0; i < columns_num; ++i, ++name_and_type)
    {
        const auto & [name, type] = *name_and_type;
        auto position = data_part->getColumnPosition(name);

        /// If array of Nested column is missing in part,
        ///  we have to read it's offsets if they exists.
        if (!position && typeid_cast<const DataTypeArray *>(type.get()))
        {
            position = findColumnForOffsets(name);
            read_only_offsets[i] = (position != std::nullopt);
        }

        column_positions[i] = std::move(position);
    }

}

size_t MergeTreeReaderCompact::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    if (continue_reading)
        from_mark = next_mark;

    size_t read_rows = 0;
    size_t num_columns = columns.size();

    MutableColumns mutable_columns(num_columns);
    auto column_it = columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++column_it)
    {
        if (!column_positions[i])
            continue;

        bool append = res_columns[i] != nullptr;
        if (!append)
            res_columns[i] = column_it->type->createColumn();
        mutable_columns[i] = res_columns[i]->assumeMutable();
    }

    while (read_rows < max_rows_to_read)
    {
        size_t rows_to_read = data_part->index_granularity.getMarkRows(from_mark);

        auto name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (!res_columns[pos])
                continue;

            const auto & [name, type] = *name_and_type;
            auto & column = mutable_columns[pos];

            try
            {
                size_t column_size_before_reading = column->size();

                readData(name, *column, *type, from_mark, *column_positions[pos], rows_to_read, read_only_offsets[pos]);

                size_t read_rows_in_column = column->size() - column_size_before_reading;

                if (read_rows_in_column < rows_to_read)
                    throw Exception("Cannot read all data in MergeTreeReaderCompact. Rows read: " + toString(read_rows_in_column) +
                        ". Rows expected: " + toString(rows_to_read) + ".", ErrorCodes::CANNOT_READ_ALL_DATA);
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage("(while reading column " + name + ")");
                throw;
            }
        }

        ++from_mark;
        read_rows += rows_to_read;
    }

    for (size_t i = 0; i < num_columns; ++i)
    {
        auto & column = mutable_columns[i];
        if (column && column->size())
            res_columns[i] = std::move(column);
        else
            res_columns[i] = nullptr;
    }

    next_mark = from_mark;

    return read_rows;
}

MergeTreeReaderCompact::ColumnPosition MergeTreeReaderCompact::findColumnForOffsets(const String & column_name)
{
    String table_name = Nested::extractTableName(column_name);
    for (const auto & part_column : data_part->getColumns())
    {
        if (typeid_cast<const DataTypeArray *>(part_column.type.get()))
        {
            auto position = data_part->getColumnPosition(part_column.name);
            if (position && Nested::extractTableName(part_column.name) == table_name)
                return position;
        }
    }

    return {};
}


void MergeTreeReaderCompact::readData(
    const String & name, IColumn & column, const IDataType & type,
    size_t from_mark, size_t column_position, size_t rows_to_read, bool only_offsets)
{
    if (!isContinuousReading(from_mark, column_position))
        seekToMark(from_mark, column_position);

    auto buffer_getter = [&](const IDataType::SubstreamPath & substream_path) -> ReadBuffer *
    {
        if (only_offsets && (substream_path.size() != 1 || substream_path[0].type != IDataType::Substream::ArraySizes))
            return nullptr;

        return data_buffer;
    };

    IDataType::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.getter = buffer_getter;
    deserialize_settings.avg_value_size_hint = avg_value_size_hints[name];
    deserialize_settings.position_independent_encoding = true;

    IDataType::DeserializeBinaryBulkStatePtr state;
    type.deserializeBinaryBulkStatePrefix(deserialize_settings, state);
    type.deserializeBinaryBulkWithMultipleStreams(column, rows_to_read, deserialize_settings, state);

    /// The buffer is left in inconsistent state after reading single offsets
    if (only_offsets)
        last_read_granule.reset();
    else
        last_read_granule.emplace(from_mark, column_position);
}


void MergeTreeReaderCompact::initMarksLoader()
{
    if (marks_loader.initialized())
        return;

    size_t columns_num = data_part->getColumns().size();

    auto load = [this, columns_num](const String & mrk_path) -> MarkCache::MappedPtr
    {
        size_t file_size = Poco::File(mrk_path).getSize();
        size_t marks_count = data_part->getMarksCount();
        size_t mark_size_in_bytes = data_part->index_granularity_info.getMarkSizeInBytes(columns_num);

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
            buffer.readStrict(reinterpret_cast<char *>(res->data() + i * columns_num), sizeof(MarkInCompressedFile) * columns_num);
            buffer.seek(sizeof(size_t), SEEK_CUR);
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


bool MergeTreeReaderCompact::isContinuousReading(size_t mark, size_t column_position)
{
    if (!last_read_granule)
        return false;
    const auto & [last_mark, last_column] = *last_read_granule;
    return (mark == last_mark && column_position == last_column + 1)
        || (mark == last_mark + 1 && column_position == 0 && last_column == data_part->getColumns().size() - 1);
}

}
