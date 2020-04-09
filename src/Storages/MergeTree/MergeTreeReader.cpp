#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/escapeForFileName.h>
#include <Compression/CachedCompressedReadBuffer.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Common/typeid_cast.h>


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


MergeTreeReader::MergeTreeReader(
    String path_,
    MergeTreeData::DataPartPtr data_part_,
    NamesAndTypesList columns_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    bool save_marks_in_cache_,
    const MergeTreeData & storage_,
    MarkRanges all_mark_ranges_,
    size_t aio_threshold_,
    size_t max_read_buffer_size_,
    ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : data_part(std::move(data_part_))
    , avg_value_size_hints(std::move(avg_value_size_hints_))
    , path(std::move(path_)), columns(std::move(columns_))
    , uncompressed_cache(uncompressed_cache_)
    , mark_cache(mark_cache_)
    , save_marks_in_cache(save_marks_in_cache_)
    , storage(storage_)
    , all_mark_ranges(std::move(all_mark_ranges_))
    , aio_threshold(aio_threshold_)
    , max_read_buffer_size(max_read_buffer_size_)
{
    try
    {
        for (const NameAndTypePair & column : columns)
            addStreams(column.name, *column.type, profile_callback_, clock_type_);
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


size_t MergeTreeReader::readRows(size_t from_mark, bool continue_reading, size_t max_rows_to_read, Columns & res_columns)
{
    size_t read_rows = 0;
    try
    {
        size_t num_columns = columns.size();

        if (res_columns.size() != num_columns)
            throw Exception("invalid number of columns passed to MergeTreeReader::readRows. "
                            "Expected " + toString(num_columns) + ", "
                            "got " + toString(res_columns.size()), ErrorCodes::LOGICAL_ERROR);

        /// Pointers to offset columns that are common to the nested data structure columns.
        /// If append is true, then the value will be equal to nullptr and will be used only to
        /// check that the offsets column has been already read.
        OffsetColumns offset_columns;

        auto name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            auto & [name, type] = *name_and_type;

            /// The column is already present in the block so we will append the values to the end.
            bool append = res_columns[pos] != nullptr;
            if (!append)
                res_columns[pos] = name_and_type->type->createColumn();

            /// To keep offsets shared. TODO Very dangerous. Get rid of this.
            MutableColumnPtr column = res_columns[pos]->assumeMutable();

            bool read_offsets = true;

            /// For nested data structures collect pointers to offset columns.
            if (const auto * type_arr = typeid_cast<const DataTypeArray *>(type.get()))
            {
                String table_name = Nested::extractTableName(name);

                auto it_inserted = offset_columns.emplace(table_name, nullptr);

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

                readData(name, *type, *column, from_mark, continue_reading, max_rows_to_read, read_offsets);

                /// For elements of Nested, column_size_before_reading may be greater than column size
                ///  if offsets are not empty and were already read, but elements are empty.
                if (!column->empty())
                    read_rows = std::max(read_rows, column->size() - column_size_before_reading);
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage("(while reading column " + name + ")");
                throw;
            }

            if (column->empty())
                res_columns[pos] = nullptr;
            else
                res_columns[pos] = std::move(column);
        }

        /// NOTE: positions for all streams must be kept in sync.
        /// In particular, even if for some streams there are no rows to be read,
        /// you must ensure that no seeks are skipped and at this point they all point to to_mark.
    }
    catch (Exception & e)
    {
        if (e.code() != ErrorCodes::MEMORY_LIMIT_EXCEEDED)
            storage.reportBrokenPart(data_part->name);

        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + " "
                     "from mark " + toString(from_mark) + " "
                     "with max_rows_to_read = " + toString(max_rows_to_read) + ")");
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

        bool data_file_exists = data_part->checksums.files.count(stream_name + DATA_FILE_EXTENSION);

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
            &data_part->index_granularity_info,
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
    const auto * column_array = typeid_cast<const ColumnArray *>(&column);

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


void MergeTreeReader::fillMissingColumns(Columns & res_columns, bool & should_evaluate_missing_defaults, size_t num_rows)
{
    try
    {
        size_t num_columns = columns.size();

        if (res_columns.size() != num_columns)
            throw Exception("invalid number of columns passed to MergeTreeReader::fillMissingColumns. "
                            "Expected " + toString(num_columns) + ", "
                            "got " + toString(res_columns.size()), ErrorCodes::LOGICAL_ERROR);

        /// For a missing column of a nested data structure we must create not a column of empty
        /// arrays, but a column of arrays of correct length.

        /// First, collect offset columns for all arrays in the block.
        OffsetColumns offset_columns;
        auto requested_column = columns.begin();
        for (size_t i = 0; i < num_columns; ++i, ++requested_column)
        {
            if (res_columns[i] == nullptr)
                continue;

            if (const auto * array = typeid_cast<const ColumnArray *>(res_columns[i].get()))
            {
                String offsets_name = Nested::extractTableName(requested_column->name);
                auto & offsets_column = offset_columns[offsets_name];

                /// If for some reason multiple offsets columns are present for the same nested data structure,
                /// choose the one that is not empty.
                if (!offsets_column || offsets_column->empty())
                    offsets_column = array->getOffsetsPtr();
            }
        }

        should_evaluate_missing_defaults = false;

        /// insert default values only for columns without default expressions
        requested_column = columns.begin();
        for (size_t i = 0; i < num_columns; ++i, ++requested_column)
        {
            auto & [name, type] = *requested_column;

            if (res_columns[i] && arrayHasNoElementsRead(*res_columns[i]))
                res_columns[i] = nullptr;

            if (res_columns[i] == nullptr)
            {
                if (storage.getColumns().hasDefault(name))
                {
                    should_evaluate_missing_defaults = true;
                    continue;
                }

                String offsets_name = Nested::extractTableName(name);
                auto offset_it = offset_columns.find(offsets_name);
                if (offset_it != offset_columns.end())
                {
                    ColumnPtr offsets_column = offset_it->second;
                    DataTypePtr nested_type = typeid_cast<const DataTypeArray &>(*type).getNestedType();
                    size_t nested_rows = typeid_cast<const ColumnUInt64 &>(*offsets_column).getData().back();

                    ColumnPtr nested_column =
                        nested_type->createColumnConstWithDefaultValue(nested_rows)->convertToFullColumnIfConst();

                    res_columns[i] = ColumnArray::create(nested_column, offsets_column);
                }
                else
                {
                    /// We must turn a constant column into a full column because the interpreter could infer
                    /// that it is constant everywhere but in some blocks (from other parts) it can be a full column.
                    res_columns[i] = type->createColumnConstWithDefaultValue(num_rows)->convertToFullColumnIfConst();
                }
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

void MergeTreeReader::evaluateMissingDefaults(Block additional_columns, Columns & res_columns)
{
    try
    {
        size_t num_columns = columns.size();

        if (res_columns.size() != num_columns)
            throw Exception("invalid number of columns passed to MergeTreeReader::fillMissingColumns. "
                            "Expected " + toString(num_columns) + ", "
                            "got " + toString(res_columns.size()), ErrorCodes::LOGICAL_ERROR);

        /// Convert columns list to block.
        /// TODO: rewrite with columns interface. It wll be possible after changes in ExpressionActions.
        auto name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
        {
            if (res_columns[pos] == nullptr)
                continue;

            additional_columns.insert({res_columns[pos], name_and_type->type, name_and_type->name});
        }

        DB::evaluateMissingDefaults(additional_columns, columns, storage.getColumns().getDefaults(), storage.global_context);

        /// Move columns from block.
        name_and_type = columns.begin();
        for (size_t pos = 0; pos < num_columns; ++pos, ++name_and_type)
            res_columns[pos] = std::move(additional_columns.getByName(name_and_type->name).column);
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + ")");
        throw;
    }
}

}
