#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>
#include <Poco/File.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


MergeTreeReaderCompact::MergeTreeReaderCompact(
    DataPartCompactPtr data_part_,
    NamesAndTypesList columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        std::move(data_part_),
        std::move(columns_),
        metadata_snapshot_,
        uncompressed_cache_,
        mark_cache_,
        std::move(mark_ranges_),
        std::move(settings_),
        std::move(avg_value_size_hints_))
    , marks_loader(
          data_part->volume->getDisk(),
          mark_cache,
          data_part->index_granularity_info.getMarksFilePath(data_part->getFullRelativePath() + MergeTreeDataPartCompact::DATA_FILE_NAME),
          data_part->getMarksCount(),
          data_part->index_granularity_info,
          settings.save_marks_in_cache,
          data_part->getColumns().size())
{
    size_t columns_num = columns.size();

    column_positions.resize(columns_num);
    read_only_offsets.resize(columns_num);
    auto name_and_type = columns.begin();
    for (size_t i = 0; i < columns_num; ++i, ++name_and_type)
    {
        const auto & [name, type] = getColumnFromPart(*name_and_type);
        auto position = data_part->getColumnPosition(name);

        if (!position && typeid_cast<const DataTypeArray *>(type.get()))
        {
            /// If array of Nested column is missing in part,
            ///  we have to read its offsets if they exist.
            position = findColumnForOffsets(name);
            read_only_offsets[i] = (position != std::nullopt);
        }

        column_positions[i] = std::move(position);
    }

    /// Do not use max_read_buffer_size, but try to lower buffer size with maximal size of granule to avoid reading much data.
    auto buffer_size = getReadBufferSize(data_part, marks_loader, column_positions, all_mark_ranges);
    if (!buffer_size || settings.max_read_buffer_size < buffer_size)
        buffer_size = settings.max_read_buffer_size;

    const String full_data_path = data_part->getFullRelativePath() + MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION;
    if (uncompressed_cache)
    {
        auto buffer = std::make_unique<CachedCompressedReadBuffer>(
            fullPath(data_part->volume->getDisk(), full_data_path),
            [this, full_data_path, buffer_size]()
            {
                return data_part->volume->getDisk()->readFile(
                    full_data_path,
                    buffer_size,
                    0,
                    settings.min_bytes_to_use_direct_io,
                    settings.min_bytes_to_use_mmap_io);
            },
            uncompressed_cache);

        if (profile_callback_)
            buffer->setProfileCallback(profile_callback_, clock_type_);

        cached_buffer = std::move(buffer);
        data_buffer = cached_buffer.get();
    }
    else
    {
        auto buffer =
            std::make_unique<CompressedReadBufferFromFile>(
                data_part->volume->getDisk()->readFile(
                    full_data_path, buffer_size, 0, settings.min_bytes_to_use_direct_io, settings.min_bytes_to_use_mmap_io));

        if (profile_callback_)
            buffer->setProfileCallback(profile_callback_, clock_type_);

        non_cached_buffer = std::move(buffer);
        data_buffer = non_cached_buffer.get();
    }
}

size_t MergeTreeReaderCompact::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    if (continue_reading)
        from_mark = next_mark;

    size_t read_rows = 0;
    size_t num_columns = columns.size();
    checkNumberOfColumns(num_columns);

    MutableColumns mutable_columns(num_columns);
    auto column_it = columns.begin();
    for (size_t i = 0; i < num_columns; ++i, ++column_it)
    {
        if (!column_positions[i])
            continue;

        bool append = res_columns[i] != nullptr;
        if (!append)
            res_columns[i] = getColumnFromPart(*column_it).type->createColumn();
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

            auto [name, type] = getColumnFromPart(*name_and_type);
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
        if (column && !column->empty())
            res_columns[i] = std::move(column);
        else
            res_columns[i] = nullptr;
    }

    next_mark = from_mark;

    return read_rows;
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

namespace
{

/// A simple class that helps to iterate over 2-dim marks of compact parts.
class MarksCounter
{
public:
    MarksCounter(size_t rows_num_, size_t columns_num_)
        : rows_num(rows_num_), columns_num(columns_num_) {}

    struct Iterator
    {
        size_t row;
        size_t column;
        MarksCounter * counter;

        Iterator(size_t row_, size_t column_, MarksCounter * counter_)
            : row(row_), column(column_), counter(counter_) {}

        Iterator operator++()
        {
            if (column + 1 == counter->columns_num)
            {
                ++row;
                column = 0;
            }
            else
            {
                ++column;
            }

            return *this;
        }

        bool operator==(const Iterator & other) const { return row == other.row && column == other.column; }
        bool operator!=(const Iterator & other) const { return !(*this == other); }
    };

    Iterator get(size_t row, size_t column) { return Iterator(row, column, this); }
    Iterator end() { return get(rows_num, 0); }

private:
    size_t rows_num;
    size_t columns_num;
};

}

size_t MergeTreeReaderCompact::getReadBufferSize(
    const DataPartPtr & part,
    MergeTreeMarksLoader & marks_loader,
    const ColumnPositions & column_positions,
    const MarkRanges & mark_ranges)
{
    size_t buffer_size = 0;
    size_t columns_num = column_positions.size();
    size_t file_size = part->getFileSizeOrZero(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION);

    MarksCounter counter(part->getMarksCount(), part->getColumns().size());

    for (const auto & mark_range : mark_ranges)
    {
        for (size_t mark = mark_range.begin; mark < mark_range.end; ++mark)
        {
            for (size_t i = 0; i < columns_num; ++i)
            {
                if (!column_positions[i])
                    continue;

                auto it = counter.get(mark, *column_positions[i]);
                size_t cur_offset = marks_loader.getMark(it.row, it.column).offset_in_compressed_file;

                while (it != counter.end() && cur_offset == marks_loader.getMark(it.row, it.column).offset_in_compressed_file)
                    ++it;

                size_t next_offset = (it == counter.end() ? file_size : marks_loader.getMark(it.row, it.column).offset_in_compressed_file);
                buffer_size = std::max(buffer_size, next_offset - cur_offset);
            }
        }
    }

    return buffer_size;
}

}
