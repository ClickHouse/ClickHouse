#include <Storages/MergeTree/MergeTreeReaderCompact.h>
#include <Storages/MergeTree/MergeTreeDataPartCompact.h>
#include <Storages/MergeTree/checkDataPart.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/NestedUtils.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_READ_ALL_DATA;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


MergeTreeReaderCompact::MergeTreeReaderCompact(
    MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
    NamesAndTypesList columns_,
    const StorageSnapshotPtr & storage_snapshot_,
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
        storage_snapshot_,
        uncompressed_cache_,
        mark_cache_,
        mark_ranges_,
        settings_,
        avg_value_size_hints_)
    , marks_loader(
          data_part_info_for_read_,
          mark_cache,
          data_part_info_for_read_->getIndexGranularityInfo().getMarksFilePath(MergeTreeDataPartCompact::DATA_FILE_NAME),
          data_part_info_for_read_->getMarksCount(),
          data_part_info_for_read_->getIndexGranularityInfo(),
          settings.save_marks_in_cache,
          settings.read_settings,
          load_marks_threadpool_,
          data_part_info_for_read_->getColumns().size())
    , profile_callback(profile_callback_)
    , clock_type(clock_type_)
{
}

void MergeTreeReaderCompact::initialize()
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
        auto data_part_storage = data_part_info_for_read->getDataPartStorage();

        if (uncompressed_cache)
        {
            auto buffer = std::make_unique<CachedCompressedReadBuffer>(
                std::string(fs::path(data_part_storage->getFullPath()) / path),
                [this, path, data_part_storage]()
                {
                    return data_part_storage->readFile(
                        path,
                        settings.read_settings,
                        std::nullopt, std::nullopt);
                },
                uncompressed_cache,
                /* allow_different_codecs = */ true);

            if (profile_callback)
                buffer->setProfileCallback(profile_callback, clock_type);

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
                    data_part_storage->readFile(
                        path,
                        settings.read_settings,
                        std::nullopt, std::nullopt),
                    /* allow_different_codecs = */ true);

            if (profile_callback)
                buffer->setProfileCallback(profile_callback, clock_type);

            if (!settings.checksum_on_read)
                buffer->disableChecksumming();

            non_cached_buffer = std::move(buffer);
            data_buffer = non_cached_buffer.get();
            compressed_data_buffer = non_cached_buffer.get();
        }
    }
    catch (const Exception & e)
    {
        if (!isRetryableException(e))
            data_part_info_for_read->reportBroken();
        throw;
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
    columns_for_offsets.resize(columns_num);

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

        /// If array of Nested column is missing in part,
        /// we have to read its offsets if they exist.
        if (!position && is_array)
        {
            NameAndTypePair column_to_read_with_subcolumns = column_to_read;
            auto [name_in_storage, subcolumn_name] = Nested::splitName(column_to_read.name);

            /// If it is a part of Nested, we need to get the column from
            /// storage metatadata which is converted to Nested type with subcolumns.
            /// It is required for proper counting of shared streams.
            if (!subcolumn_name.empty())
            {
                /// If column is renamed get the new name from storage metadata.
                if (alter_conversions->columnHasNewName(name_in_storage))
                    name_in_storage = alter_conversions->getColumnNewName(name_in_storage);

                if (!storage_columns_with_collected_nested)
                {
                    auto options = GetColumnsOptions(GetColumnsOptions::AllPhysical).withExtendedObjects();
                    auto storage_columns_list = Nested::collect(storage_snapshot->getColumns(options));
                    storage_columns_with_collected_nested = ColumnsDescription(std::move(storage_columns_list));
                }

                column_to_read_with_subcolumns = storage_columns_with_collected_nested
                    ->getColumnOrSubcolumn(
                        GetColumnsOptions::All,
                        Nested::concatenateName(name_in_storage, subcolumn_name));
            }

            auto name_level_for_offsets = findColumnForOffsets(column_to_read_with_subcolumns);

            if (name_level_for_offsets.has_value())
            {
                column_positions[i] = data_part_info_for_read->getColumnPosition(name_level_for_offsets->first);
                columns_for_offsets[i] = name_level_for_offsets;
                partially_read_columns.insert(column_to_read.name);
            }
        }
        else
        {
            column_positions[i] = std::move(position);
        }
    }
}

size_t MergeTreeReaderCompact::readRows(
    size_t from_mark, size_t current_task_last_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    if (!initialized)
    {
        initialize();
        initialized = true;
    }

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

                readData(columns_to_read[pos], column, from_mark, current_task_last_mark, *column_positions[pos], rows_to_read, columns_for_offsets[pos]);

                size_t read_rows_in_column = column->size() - column_size_before_reading;
                if (read_rows_in_column != rows_to_read)
                    throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA,
                        "Cannot read all data in MergeTreeReaderCompact. Rows read: {}. Rows expected: {}.",
                        read_rows_in_column, rows_to_read);
            }
            catch (Exception & e)
            {
                if (!isRetryableException(e))
                    data_part_info_for_read->reportBroken();

                /// Better diagnostics.
                e.addMessage(getMessageForDiagnosticOfBrokenPart(from_mark, max_rows_to_read));
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
    size_t from_mark, size_t current_task_last_mark, size_t column_position, size_t rows_to_read,
    ColumnNameLevel name_level_for_offsets)
{
    const auto & [name, type] = name_and_type;
    std::optional<NameAndTypePair> column_for_offsets;

    if (name_level_for_offsets.has_value())
    {
        const auto & part_columns = data_part_info_for_read->getColumnsDescription();
        column_for_offsets = part_columns.getPhysical(name_level_for_offsets->first);
    }

    adjustUpperBound(current_task_last_mark); /// Must go before seek.

    if (!isContinuousReading(from_mark, column_position))
        seekToMark(from_mark, column_position);

    /// If we read only offsets we have to read prefix anyway
    /// to preserve correctness of serialization.
    auto buffer_getter_for_prefix = [&](const auto &) -> ReadBuffer *
    {
        return data_buffer;
    };

    auto buffer_getter = [&](const ISerialization::SubstreamPath & substream_path) -> ReadBuffer *
    {
        /// Offset stream from another column could be read, in case of current
        /// column does not exists (see findColumnForOffsets() in
        /// MergeTreeReaderCompact::fillColumnPositions())
        if (name_level_for_offsets.has_value())
        {
            bool is_offsets = !substream_path.empty() && substream_path.back().type == ISerialization::Substream::ArraySizes;
            if (!is_offsets)
                return nullptr;

            /// Offset stream can be read only from columns of current level or
            /// below (since it is OK to read all parent streams from the
            /// alternative).
            ///
            /// Consider the following columns in nested "root":
            /// - root.array Array(UInt8) - exists
            /// - root.nested_array Array(Array(UInt8)) - does not exists (only_offsets_level=1)
            ///
            /// For root.nested_array it will try to read multiple streams:
            /// - offsets (substream_path = {ArraySizes})
            ///   OK
            /// - root.nested_array elements (substream_path = {ArrayElements, ArraySizes})
            ///   NOT OK - cannot use root.array offsets stream for this
            ///
            /// Here only_offsets_level is the level of the alternative stream,
            /// and substream_path.size() is the level of the current stream.
            if (name_level_for_offsets->second < ISerialization::getArrayLevel(substream_path))
                return nullptr;
        }

        return data_buffer;
    };

    ISerialization::DeserializeBinaryBulkStatePtr state;
    ISerialization::DeserializeBinaryBulkStatePtr state_for_prefix;

    ISerialization::DeserializeBinaryBulkSettings deserialize_settings;
    deserialize_settings.avg_value_size_hint = avg_value_size_hints[name];

    if (name_and_type.isSubcolumn())
    {
        NameAndTypePair name_type_in_storage{name_and_type.getNameInStorage(), name_and_type.getTypeInStorage()};

        /// In case of reading onlys offset use the correct serialization for reading of the prefix
        auto serialization = getSerializationInPart(name_type_in_storage);
        ColumnPtr temp_column = name_type_in_storage.type->createColumn(*serialization);

        if (column_for_offsets)
        {
            auto serialization_for_prefix = getSerializationInPart(*column_for_offsets);

            deserialize_settings.getter = buffer_getter_for_prefix;
            serialization_for_prefix->deserializeBinaryBulkStatePrefix(deserialize_settings, state_for_prefix);
        }

        deserialize_settings.getter = buffer_getter;
        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, state);
        serialization->deserializeBinaryBulkWithMultipleStreams(temp_column, rows_to_read, deserialize_settings, state, nullptr);

        auto subcolumn = name_type_in_storage.type->getSubcolumn(name_and_type.getSubcolumnName(), temp_column);

        /// TODO: Avoid extra copying.
        if (column->empty())
            column = subcolumn;
        else
            column->assumeMutable()->insertRangeFrom(*subcolumn, 0, subcolumn->size());
    }
    else
    {
        /// In case of reading only offsets use the correct serialization for reading the prefix
        auto serialization = getSerializationInPart(name_and_type);

        if (column_for_offsets)
        {
            auto serialization_for_prefix = getSerializationInPart(*column_for_offsets);

            deserialize_settings.getter = buffer_getter_for_prefix;
            serialization_for_prefix->deserializeBinaryBulkStatePrefix(deserialize_settings, state_for_prefix);
        }

        deserialize_settings.getter = buffer_getter;
        serialization->deserializeBinaryBulkStatePrefix(deserialize_settings, state);
        serialization->deserializeBinaryBulkWithMultipleStreams(column, rows_to_read, deserialize_settings, state, nullptr);
    }

    /// The buffer is left in inconsistent state after reading single offsets
    if (name_level_for_offsets.has_value())
        last_read_granule.reset();
    else
        last_read_granule.emplace(from_mark, column_position);
}

void MergeTreeReaderCompact::prefetchBeginOfRange(Priority priority)
try
{
    if (!initialized)
    {
        initialize();
        initialized = true;
    }

    adjustUpperBound(all_mark_ranges.back().end);
    seekToMark(all_mark_ranges.front().begin, 0);
    data_buffer->prefetch(priority);
}
catch (const Exception & e)
{
    if (!isRetryableException(e))
        data_part_info_for_read->reportBroken();
    throw;
}
catch (...)
{
    data_part_info_for_read->reportBroken();
    throw;
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
