#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/escapeForFileName.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Common/typeid_cast.h>
#include <Poco/File.h>


namespace DB
{

namespace
{
    using OffsetColumns = std::map<std::string, ColumnPtr>;

    constexpr auto DATA_FILE_EXTENSION = ".bin";
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int MEMORY_LIMIT_EXCEEDED;
    extern const int ARGUMENT_OUT_OF_BOUND;
}


MergeTreeReader::~MergeTreeReader() = default;


MergeTreeReader::MergeTreeReader(const String & path,
    const MergeTreeData::DataPartPtr & data_part, const NamesAndTypesList & columns,
    UncompressedCache * uncompressed_cache, MarkCache * mark_cache, bool save_marks_in_cache,
    const MergeTreeData & storage, const MarkRanges & all_mark_ranges,
    size_t aio_threshold, size_t max_read_buffer_size, const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback,
    clockid_t clock_type)
    : data_part(data_part), avg_value_size_hints(avg_value_size_hints), path(path), columns(columns)
    , uncompressed_cache(uncompressed_cache), mark_cache(mark_cache), save_marks_in_cache(save_marks_in_cache), storage(storage)
    , all_mark_ranges(all_mark_ranges), aio_threshold(aio_threshold), max_read_buffer_size(max_read_buffer_size)
{
    try
    {
        if (!Poco::File(path).exists())
            throw Exception("Part " + path + " is missing", ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);

        for (const NameAndTypePair & column : columns)
            addStreams(column.name, *column.type, profile_callback, clock_type);
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);
        throw;
    }
}


const MergeTreeReader::ValueSizeMap & MergeTreeReader::getAvgValueSizeHints() const
{
    return avg_value_size_hints;
}


size_t MergeTreeReader::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Block & res)
{
    size_t read_rows = 0;
    try
    {
        /// Pointers to offset columns that are common to the nested data structure columns.
        /// If append is true, then the value will be equal to nullptr and will be used only to
        /// check that the offsets column has been already read.
        OffsetColumns offset_columns;

        for (const NameAndTypePair & it : columns)
        {
            /// The column is already present in the block so we will append the values to the end.
            bool append = res.has(it.name);
            if (!append)
                res.insert(ColumnWithTypeAndName(it.type->createColumn(), it.type, it.name));

            /// To keep offsets shared. TODO Very dangerous. Get rid of this.
            MutableColumnPtr column = res.getByName(it.name).column->assumeMutable();

            bool read_offsets = true;

            /// For nested data structures collect pointers to offset columns.
            if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(it.type.get()))
            {
                String name = Nested::extractTableName(it.name);

                auto it_inserted = offset_columns.emplace(name, nullptr);

                /// offsets have already been read on the previous iteration and we don't need to read it again
                if (!it_inserted.second)
                    read_offsets = false;

                /// need to create new offsets
                if (it_inserted.second && !append)
                    it_inserted.first->second = ColumnArray::ColumnOffsets::create();

                /// share offsets in all elements of nested structure
                if (!append)
                    column = ColumnArray::create(type_arr->getNestedType()->createColumn(),
                                                 it_inserted.first->second)->assumeMutable();
            }

            try
            {
                size_t column_size_before_reading = column->size();

                readData(it.name, *it.type, *column, from_mark, continue_reading, max_rows_to_read, read_offsets);

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

        /// NOTE: positions for all streams must be kept in sync. In particular, even if for some streams there are no rows to be read,
        /// you must ensure that no seeks are skipped and at this point they all point to to_mark.
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            storage.reportBrokenPart(data_part->name);

        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + " from mark " + toString(from_mark) + " with max_rows_to_read = " + toString(max_rows_to_read) + ")");
        throw;
    }
    catch (...)
    {
        storage.reportBrokenPart(data_part->name);

        throw;
    }

    return read_rows;
}

void MergeTreeReader::addStreams(const String & name, const IDataType & type,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type)
{
    IDataType::StreamCallback callback = [&] (const IDataType::SubstreamPath & substream_path)
    {
        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        if (streams.count(stream_name))
            return;

        bool data_file_exists = Poco::File(path + stream_name + DATA_FILE_EXTENSION).exists();

        /** If data file is missing then we will not try to open it.
          * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
          */
        if (!data_file_exists)
            return;

        streams.emplace(stream_name, std::make_unique<MergeTreeReaderStream>(
            path + stream_name, DATA_FILE_EXTENSION, data_part->getMarksCount(),
            all_mark_ranges, mark_cache, save_marks_in_cache,
            uncompressed_cache, data_part->getFileSizeOrZero(stream_name + DATA_FILE_EXTENSION),
            aio_threshold, max_read_buffer_size,
            &storage.index_granularity_info,
            profile_callback, clock_type));
    };

    IDataType::SubstreamPath substream_path;
    type.enumerateStreams(callback, substream_path);
}


void MergeTreeReader::readData(
    const String & name, const IDataType & type, IColumn & column,
    size_t from_mark, bool continue_reading, size_t max_rows_to_read,
    bool with_offsets)
{
    auto get_stream_getter = [&](bool stream_for_prefix) -> IDataType::InputStreamGetter
    {
        return [&, stream_for_prefix](const IDataType::SubstreamPath & substream_path) -> ReadBuffer *
        {
            /// If offsets for arrays have already been read.
            if (!with_offsets && substream_path.size() == 1 && substream_path[0].type == IDataType::Substream::ArraySizes)
                return nullptr;

            String stream_name = IDataType::getFileNameForStream(name, substream_path);

            auto it = streams.find(stream_name);
            if (it == streams.end())
                return nullptr;

            MergeTreeReaderStream & stream = *it->second;

            if (stream_for_prefix)
            {
                stream.seekToStart();
                continue_reading = false;
            }
            else if (!continue_reading)
                stream.seekToMark(from_mark);

            return stream.data_buffer;
        };
    };

    double & avg_value_size_hint = avg_value_size_hints[name];
    IDataType::DeserializeBinaryBulkSettings settings;
    settings.avg_value_size_hint = avg_value_size_hint;

    if (deserialize_binary_bulk_state_map.count(name) == 0)
    {
        settings.getter = get_stream_getter(true);
        type.deserializeBinaryBulkStatePrefix(settings, deserialize_binary_bulk_state_map[name]);
    }

    settings.getter = get_stream_getter(false);
    settings.continuous_reading = continue_reading;
    auto & deserialize_state = deserialize_binary_bulk_state_map[name];
    type.deserializeBinaryBulkWithMultipleStreams(column, max_rows_to_read, settings, deserialize_state);
    IDataType::updateAvgValueSizeHint(column, avg_value_size_hint);
}


static bool arrayHasNoElementsRead(const IColumn & column)
{
    const ColumnArray * column_array = typeid_cast<const ColumnArray *>(&column);

    if (!column_array)
        return false;

    size_t size = column_array->size();
    if (!size)
        return false;

    size_t data_size = column_array->getData().size();
    if (data_size)
        return false;

    size_t last_offset = column_array->getOffsets()[size - 1];
    return last_offset != 0;
}


void MergeTreeReader::fillMissingColumns(Block & res, bool & should_reorder, bool & should_evaluate_missing_defaults, size_t num_rows)
{
    try
    {
        /// For a missing column of a nested data structure we must create not a column of empty
        /// arrays, but a column of arrays of correct length.

        /// First, collect offset columns for all arrays in the block.
        OffsetColumns offset_columns;
        for (size_t i = 0; i < res.columns(); ++i)
        {
            const ColumnWithTypeAndName & column = res.safeGetByPosition(i);

            if (const ColumnArray * array = typeid_cast<const ColumnArray *>(column.column.get()))
            {
                String offsets_name = Nested::extractTableName(column.name);
                auto & offsets_column = offset_columns[offsets_name];

                /// If for some reason multiple offsets columns are present for the same nested data structure,
                /// choose the one that is not empty.
                if (!offsets_column || offsets_column->empty())
                    offsets_column = array->getOffsetsPtr();
            }
        }

        should_evaluate_missing_defaults = false;
        should_reorder = false;

        /// insert default values only for columns without default expressions
        for (const auto & requested_column : columns)
        {
            bool has_column = res.has(requested_column.name);
            if (has_column)
            {
                const auto & col = *res.getByName(requested_column.name).column;
                if (arrayHasNoElementsRead(col))
                {
                    res.erase(requested_column.name);
                    has_column = false;
                }
            }

            if (!has_column)
            {
                should_reorder = true;
                if (storage.getColumns().hasDefault(requested_column.name))
                {
                    should_evaluate_missing_defaults = true;
                    continue;
                }

                ColumnWithTypeAndName column_to_add;
                column_to_add.name = requested_column.name;
                column_to_add.type = requested_column.type;

                String offsets_name = Nested::extractTableName(column_to_add.name);
                if (offset_columns.count(offsets_name))
                {
                    ColumnPtr offsets_column = offset_columns[offsets_name];
                    DataTypePtr nested_type = typeid_cast<const DataTypeArray &>(*column_to_add.type).getNestedType();
                    size_t nested_rows = typeid_cast<const ColumnUInt64 &>(*offsets_column).getData().back();

                    ColumnPtr nested_column = nested_type->createColumnConstWithDefaultValue(nested_rows)->convertToFullColumnIfConst();

                    column_to_add.column = ColumnArray::create(nested_column, offsets_column);
                }
                else
                {
                    /// We must turn a constant column into a full column because the interpreter could infer that it is constant everywhere
                    /// but in some blocks (from other parts) it can be a full column.
                    column_to_add.column = column_to_add.type->createColumnConstWithDefaultValue(num_rows)->convertToFullColumnIfConst();
                }

                res.insert(std::move(column_to_add));
            }
        }
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + ")");
        throw;
    }
}

void MergeTreeReader::reorderColumns(Block & res, const Names & ordered_names, const String * filter_name)
{
    try
    {
        Block ordered_block;

        for (const auto & name : ordered_names)
            if (res.has(name))
                ordered_block.insert(res.getByName(name));

        if (filter_name && !ordered_block.has(*filter_name) && res.has(*filter_name))
            ordered_block.insert(res.getByName(*filter_name));

        std::swap(res, ordered_block);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + ")");
        throw;
    }
}

void MergeTreeReader::evaluateMissingDefaults(Block & res)
{
    try
    {
        DB::evaluateMissingDefaults(res, columns, storage.getColumns().getDefaults(), storage.global_context);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + ")");
        throw;
    }
}

}
