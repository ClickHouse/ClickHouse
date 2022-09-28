#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int MEMORY_LIMIT_EXCEEDED;
}


MergeTreeReaderCompact::MergeTreeReaderCompact(
    MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
    NamesAndTypesList columns_,
    const StorageMetadataPtr & metadata_snapshot_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    ThreadPool * load_marks_threadpool_,
    ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        data_part_info_for_read_,
        columns_,
        metadata_snapshot_,
        uncompressed_cache_,
        mark_cache_,
        mark_ranges_,
        settings_,
        avg_value_size_hints_)
    , marks_loader(
          data_part_info_for_read_->getDataPartStorage(),
          mark_cache,
          data_part_info_for_read_->getIndexGranularityInfo().getMarksFilePath(MergeTreeDataPartCompact::DATA_FILE_NAME),
          data_part_info_for_read_->getMarksCount(),
          data_part_info_for_read_->getIndexGranularityInfo(),
          settings.save_marks_in_cache,
          settings.read_settings,
          load_marks_threadpool_,
          data_part_info_for_read_->getColumns().size())
{
    try
    {
        fillColumnPositions();

        /// Do not use max_read_buffer_size, but try to lower buffer size with maximal size of granule to avoid reading much data.
        auto buffer_size = getReadBufferSize(*data_part_info_for_read, marks_loader, column_positions, all_mark_ranges);
        if (buffer_size)
            settings.read_settings = settings.read_settings.adjustBufferSize(buffer_size);

        if (!settings.read_settings.local_fs_buffer_size || !settings.read_settings.remote_fs_buffer_size)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read to empty buffer.");

        const String path = MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION;
        if (uncompressed_cache)
        {
            auto buffer = std::make_unique<CachedCompressedReadBuffer>(
                std::string(fs::path(data_part_info_for_read->getDataPartStorage()->getFullPath()) / path),
                [this, path]()
                {
                    return data_part_info_for_read->getDataPartStorage()->readFile(
                        path,
                        settings.read_settings,
                        std::nullopt, std::nullopt);
                },
                uncompressed_cache,
                /* allow_different_codecs = */ true);

            if (profile_callback_)
                buffer->setProfileCallback(profile_callback_, clock_type_);

            if (!settings.checksum_on_read)
                buffer->disableChecksumming();

            cached_buffer = std::move(buffer);
            data_buffer = cached_buffer.get();
            compressed_data_buffer = cached_buffer.get();
        }
        else
        {
            auto buffer =
                std::make_unique<CompressedReadBufferFromFile>(
                    data_part_info_for_read->getDataPartStorage()->readFile(
                        path,
                        settings.read_settings,
                        std::nullopt, std::nullopt),
                    /* allow_different_codecs = */ true);

            if (profile_callback_)
                buffer->setProfileCallback(profile_callback_, clock_type_);

            if (!settings.checksum_on_read)
                buffer->disableChecksumming();

            non_cached_buffer = std::move(buffer);
            data_buffer = non_cached_buffer.get();
            compressed_data_buffer = non_cached_buffer.get();
        }
    }
    catch (...)
    {
        data_part_info_for_read->reportBroken();
        throw;
    }
}

void MergeTreeReaderCompact::fillColumnPositions()
{
    size_t columns_num = columns_to_read.size();

    column_positions.resize(columns_num);
    read_only_offsets.resize(columns_num);

    for (size_t i = 0; i < columns_num; ++i)
    {
        const auto & column_to_read = columns_to_read[i];

        auto position = data_part_info_for_read->getColumnPosition(column_to_read.getNameInStorage());
        bool is_array = isArray(column_to_read.type);

        if (column_to_read.isSubcolumn())
        {
            auto storage_column_from_part = getColumnInPart(
                {column_to_read.getNameInStorage(), column_to_read.getTypeInStorage()});

            auto subcolumn_name = column_to_read.getSubcolumnName();
            if (!storage_column_from_part.type->hasSubcolumn(subcolumn_name))
                position.reset();
        }

        if (!position && is_array)
        {
            /// If array of Nested column is missing in part,
            /// we have to read its offsets if they exist.
            position = findColumnForOffsets(column_to_read);
            read_only_offsets[i] = (position != std::nullopt);
        }

        column_positions[i] = std::move(position);
        if (read_only_offsets[i])
            partially_read_columns.insert(column_to_read.name);
    }
}

size_t MergeTreeReaderCompact::readRows(
    size_t from_mark, size_t current_task_last_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    if (continue_reading)
        from_mark = next_mark;

    size_t read_rows = 0;
    size_t num_columns = columns_to_read.size();
    checkNumberOfColumns(num_columns);

    MutableColumns mutable_columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i)
    {
        if (column_positions[i] && res_columns[i] == nullptr)
            res_columns[i] = columns_to_read[i].type->createColumn(*serializations[i]);
    }

    while (read_rows < max_rows_to_read)
    {
        size_t rows_to_read = data_part_info_for_read->getIndexGranularity().getMarkRows(from_mark);

        for (size_t pos = 0; pos < num_columns; ++pos)
        {
            if (!res_columns[pos])
                continue;

            try
            {
                auto & column = res_columns[pos];
                size_t column_size_before_reading = column->size();

                readData(columns_to_read[pos], column, from_mark, current_task_last_mark, *column_positions[pos], rows_to_read, read_only_offsets[pos]);

                size_t read_rows_in_column = column->size() - column_size_before_reading;
                if (read_rows_in_column != rows_to_read)
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "Cannot read all data in MergeTreeReaderCompact. Rows read: {}. Rows expected: {}.",
                        read_rows_in_column, rows_to_read);
            }
            catch (Exception & e)
            {
                if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                    data_part_info_for_read->reportBroken();

                /// Better diagnostics.
                e.addMessage("(while reading column " + columns_to_read[pos].name + ")");
                throw;
            }
            catch (...)
            {
                data_part_info_for_read->reportBroken();
                throw;
            }
        }

        ++from_mark;
        read_rows += rows_to_read;
    }

    next_mark = from_mark;

    return read_rows;
}

void MergeTreeReaderCompact::readData(
    const NameAndTypePair & name_and_type, ColumnPtr & column,
    size_t from_mark, size_t current_task_last_mark, size_t column_position, size_t rows_to_read, bool only_offsets)
{
    const auto & [name, type] = name_and_type;

    adjustUpperBound(current_task_last_mark); /// Must go before seek.

    if (!isContinuousReading(from_mark, column_position))
        seekToMark(from_mark, column_position);

    auto buffer_getter = [&](const ISerialization::SubstreamPath & substream_path) -> ReadBuffer *
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
        if (only_offsets && !is_offsets)
            return nullptr;

        return data_buffer;
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.getter = buffer_getter;
    deserialize_settings.avg_value_size_hint = avg_value_size_hints[name];

    if (name_and_type.isSubcolumn())
    {
        const auto & type_in_storage = name_and_type.getTypeInStorage();
        const auto & name_in_storage = name_and_type.getNameInStorage();

        auto serialization = getSerializationInPart({name_in_storage, type_in_storage});
        ColumnPtr temp_column = type_in_storage->createColumn(*serialization);

        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, state);
        serialization->deserializeBinaryBulkWithMultipleStreams(temp_column, rows_to_read, deserialize_settings, state, nullptr);

        auto subcolumn = type_in_storage->getSubcolumn(name_and_type.getSubcolumnName(), temp_column);

        /// TODO: Avoid extra copying.
        if (column->empty())
            column = subcolumn;
        else
            column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
    }
    else
    {
        auto serialization = getSerializationInPart(name_and_type);
        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, state);
        serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_to_read, deserialize_settings, state, nullptr);
    }

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
        compressed_data_buffer->seek(mark.offset_in_compressed_file, mark.offset_in_decompressed_block);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        if (e.code() == ErrorCodes::ARGUMENT_OUT_OF_BOUND)
            e.addMessage("(while seeking to mark (" + toString(row_index) + ", " + toString(column_index) + ")");

        throw;
    }
}

void MergeTreeReaderCompact::adjustUpperBound(size_t last_mark)
{
    size_t right_offset = 0;
    if (last_mark < data_part_info_for_read->getMarksCount()) /// Otherwise read until the end of file
        right_offset = marks_loader.getMark(last_mark).offset_in_compressed_file;

    if (right_offset == 0)
    {
        /// If already reading till the end of file.
        if (last_right_offset && *last_right_offset == 0)
            return;

        last_right_offset = 0; // Zero value means the end of file.
        data_buffer->setReadUntilEnd();
    }
    else
    {
        if (last_right_offset && right_offset <= last_right_offset.value())
            return;

        last_right_offset = right_offset;
        data_buffer->setReadUntilPosition(right_offset);
    }
}

bool MergeTreeReaderCompact::isContinuousReading(size_t mark, size_t column_position)
{
    if (!last_read_granule)
        return false;
    const auto & [last_mark, last_column] = *last_read_granule;
    return (mark == last_mark && column_position == last_column + 1)
        || (mark == last_mark + 1 && column_position == 0 && last_column == data_part_info_for_read->getColumns().size() - 1);
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
    const IMergeTreeDataPartInfoForReader & data_part_info_for_reader,
    MergeTreeMarksLoader & marks_loader,
    const ColumnPositions & column_positions,
    const MarkRanges & mark_ranges)
{
    size_t buffer_size = 0;
    size_t columns_num = column_positions.size();
    size_t file_size = data_part_info_for_reader.getFileSizeOrZero(MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION);

    MarksCounter counter(data_part_info_for_reader.getMarksCount(), data_part_info_for_reader.getColumns().size());

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
