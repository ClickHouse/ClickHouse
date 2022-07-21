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
        data_part_,
        columns_,
        metadata_snapshot_,
        uncompressed_cache_,
        mark_cache_,
        mark_ranges_,
        settings_,
        avg_value_size_hints_)
    , marks_loader(
          data_part->data_part_storage,
          mark_cache,
          data_part->index_granularity_info.getMarksFilePath(MergeTreeDataPartCompact::DATA_FILE_NAME),
          data_part->getMarksCount(),
          data_part->index_granularity_info,
          settings.save_marks_in_cache,
          data_part->getColumns().size())
{
    try
    {
        size_t columns_num = columns.size();

        column_positions.resize(columns_num);
        read_only_offsets.resize(columns_num);
        auto name_and_type = columns.begin();
        for (size_t i = 0; i < columns_num; ++i, ++name_and_type)
        {
            if (name_and_type->isSubcolumn())
            {
                auto storage_column_from_part = getColumnInPart(
                    {name_and_type->getNameInStorage(), name_and_type->getTypeInStorage()});

                if (!storage_column_from_part.type->tryGetSubcolumnType(name_and_type->getSubcolumnName()))
                    continue;
            }

            auto column_from_part = getColumnInPart(*name_and_type);

            auto position = data_part->getColumnPosition(column_from_part.getNameInStorage());
            if (!position && typeid_cast<const DataTypeArray *>(column_from_part.type.get()))
            {
                /// If array of Nested column is missing in part,
                /// we have to read its offsets if they exist.
                position = findColumnForOffsets(column_from_part.name);
                read_only_offsets[i] = (position != std::nullopt);
            }

            column_positions[i] = std::move(position);
        }

        /// Do not use max_read_buffer_size, but try to lower buffer size with maximal size of granule to avoid reading much data.
        auto buffer_size = getReadBufferSize(data_part, marks_loader, column_positions, all_mark_ranges);
        if (buffer_size)
            settings.read_settings = settings.read_settings.adjustBufferSize(buffer_size);

        if (!settings.read_settings.local_fs_buffer_size || !settings.read_settings.remote_fs_buffer_size)
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot read to empty buffer.");

        const String path = MergeTreeDataPartCompact::DATA_FILE_NAME_WITH_EXTENSION;
        if (uncompressed_cache)
        {
            auto buffer = std::make_unique<CachedCompressedReadBuffer>(
                std::string(fs::path(data_part->data_part_storage->getFullPath()) / path),
                [this, path]()
                {
                    return data_part->data_part_storage->readFile(
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
                    data_part->data_part_storage->readFile(
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
        storage.reportBrokenPart(data_part);
        throw;
    }
}

size_t MergeTreeReaderCompact::readRows(
    size_t from_mark, size_t current_task_last_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
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

        auto column_from_part = getColumnInPart(*column_it);
        if (res_columns[i] == nullptr)
        {
            auto serialization = data_part->getSerialization(column_from_part);
            res_columns[i] = column_from_part.type->createColumn(*serialization);
        }
    }

    while (read_rows < max_rows_to_read)
    {
        size_t rows_to_read = data_part->index_granularity.getMarkRows(from_mark);

        auto name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (!res_columns[pos])
                continue;

            auto column_from_part = getColumnInPart(*name_and_type);

            try
            {
                auto & column = res_columns[pos];
                size_t column_size_before_reading = column->size();

                readData(column_from_part, column, from_mark, current_task_last_mark, *column_positions[pos], rows_to_read, read_only_offsets[pos]);

                size_t read_rows_in_column = column->size() - column_size_before_reading;
                if (read_rows_in_column != rows_to_read)
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "Cannot read all data in MergeTreeReaderCompact. Rows read: {}. Rows expected: {}.",
                        read_rows_in_column, rows_to_read);
            }
            catch (Exception & e)
            {
                if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
                    storage.reportBrokenPart(data_part);

                /// Better diagnostics.
                e.addMessage("(while reading column " + column_from_part.name + ")");
                throw;
            }
            catch (...)
            {
                storage.reportBrokenPart(data_part);
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
        if (only_offsets && (substream_path.size() != 1 || substream_path[0].type != ISerialization::Substream::ArraySizes))
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

        auto serialization = data_part->getSerialization(NameAndTypePair{name_in_storage, type_in_storage});
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
        auto serialization = data_part->getSerialization(name_and_type);
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
    if (last_mark < data_part->getMarksCount()) /// Otherwise read until the end of file
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
