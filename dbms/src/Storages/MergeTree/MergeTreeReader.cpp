#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/escapeForFileName.h>
#include <Common/MemoryTracker.h>
#include <IO/CachedCompressedReadBuffer.h>
#include <IO/CompressedReadBufferFromFile.h>
#include <Columns/ColumnArray.h>
#include <Interpreters/evaluateMissingDefaults.h>
#include <Storages/MergeTree/MergeTreeReader.h>
#include <Columns/ColumnNullable.h>
#include <Common/typeid_cast.h>
#include <Poco/File.h>


namespace DB
{

namespace
{
    using OffsetColumns = std::map<std::string, ColumnPtr>;

    constexpr auto DATA_FILE_EXTENSION = ".bin";
    constexpr auto NULL_MAP_EXTENSION = ".null.bin";

    bool isNullStream(const std::string & extension)
    {
        return extension == NULL_MAP_EXTENSION;
    }
}

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_FOUND_EXPECTED_DATA_PART;
    extern const int MEMORY_LIMIT_EXCEEDED;
}


MergeTreeReader::~MergeTreeReader() = default;


MergeTreeReader::MergeTreeReader(const String & path,
    const MergeTreeData::DataPartPtr & data_part, const NamesAndTypesList & columns,
    UncompressedCache * uncompressed_cache, MarkCache * mark_cache, bool save_marks_in_cache,
    MergeTreeData & storage, const MarkRanges & all_mark_ranges,
    size_t aio_threshold, size_t max_read_buffer_size, const ValueSizeMap & avg_value_size_hints,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback,
    clockid_t clock_type)
    : avg_value_size_hints(avg_value_size_hints), path(path), data_part(data_part), columns(columns)
    , uncompressed_cache(uncompressed_cache), mark_cache(mark_cache), save_marks_in_cache(save_marks_in_cache), storage(storage)
    , all_mark_ranges(all_mark_ranges), aio_threshold(aio_threshold), max_read_buffer_size(max_read_buffer_size)
{
    try
    {
        if (!Poco::File(path).exists())
            throw Exception("Part " + path + " is missing", ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);

        for (const NameAndTypePair & column : columns)
            addStream(column.name, *column.type, all_mark_ranges, profile_callback, clock_type);
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


MergeTreeRangeReader MergeTreeReader::readRange(size_t from_mark, size_t to_mark)
{
    return MergeTreeRangeReader(*this, from_mark, to_mark, storage.index_granularity);
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
            if (streams.end() == streams.find(it.name))
                continue;

            /// The column is already present in the block so we will append the values to the end.
            bool append = res.has(it.name);

            ColumnWithTypeAndName column;
            column.name = it.name;
            column.type = it.type;
            if (append)
                column.column = res.getByName(column.name).column;

            bool read_offsets = true;

            const IDataType * observed_type;
            bool is_nullable;

            if (column.type.get()->isNullable())
            {
                const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(*column.type);
                observed_type = nullable_type.getNestedType().get();
                is_nullable = true;
            }
            else
            {
                observed_type = column.type.get();
                is_nullable = false;
            }

            /// For nested data structures collect pointers to offset columns.
            if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(observed_type))
            {
                String name = DataTypeNested::extractNestedTableName(column.name);

                if (offset_columns.count(name) == 0)
                    offset_columns[name] = append ? nullptr : std::make_shared<ColumnArray::ColumnOffsets_t>();
                else
                    read_offsets = false; /// offsets have already been read on the previous iteration

                if (!append)
                {
                    column.column = std::make_shared<ColumnArray>(type_arr->getNestedType()->createColumn(), offset_columns[name]);
                    if (is_nullable)
                        column.column = std::make_shared<ColumnNullable>(column.column, std::make_shared<ColumnUInt8>());
                }
            }
            else if (!append)
                column.column = column.type->createColumn();

            try
            {
                size_t column_size_before_reading = column.column->size();
                readData(column.name, *column.type, *column.column, from_mark, continue_reading, max_rows_to_read, 0, read_offsets);
                read_rows = std::max(read_rows, column.column->size() - column_size_before_reading);
            }
            catch (Exception & e)
            {
                /// Better diagnostics.
                e.addMessage("(while reading column " + column.name + ")");
                throw;
            }

            if (!append && column.column->size())
                res.insert(std::move(column));
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


void MergeTreeReader::fillMissingColumns(Block & res, const Names & ordered_names, const bool always_reorder)
{
    fillMissingColumnsImpl(res, ordered_names, always_reorder);
}


void MergeTreeReader::fillMissingColumnsAndReorder(Block & res, const Names & ordered_names)
{
    fillMissingColumnsImpl(res, ordered_names, true);
}


MergeTreeReader::Stream::Stream(
    const String & path_prefix_, const String & extension_, size_t marks_count_,
    const MarkRanges & all_mark_ranges,
    MarkCache * mark_cache_, bool save_marks_in_cache_,
    UncompressedCache * uncompressed_cache,
    size_t aio_threshold, size_t max_read_buffer_size,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type)
    : path_prefix(path_prefix_), extension(extension_), marks_count(marks_count_)
    , mark_cache(mark_cache_), save_marks_in_cache(save_marks_in_cache_)
{
    /// Compute the size of the buffer.
    size_t max_mark_range = 0;

    for (size_t i = 0; i < all_mark_ranges.size(); ++i)
    {
        size_t right = all_mark_ranges[i].end;
        /// NOTE: if we are reading the whole file, then right == marks_count
        /// and we will use max_read_buffer_size for buffer size, thus avoiding the need to load marks.

        /// If the end of range is inside the block, we will need to read it too.
        if (right < marks_count && getMark(right).offset_in_decompressed_block > 0)
        {
            while (right < marks_count
                   && getMark(right).offset_in_compressed_file
                       == getMark(all_mark_ranges[i].end).offset_in_compressed_file)
            {
                ++right;
            }
        }

        /// If there are no marks after the end of range, just use max_read_buffer_size
        if (right >= marks_count
            || (right + 1 == marks_count
                && getMark(right).offset_in_compressed_file
                    == getMark(all_mark_ranges[i].end).offset_in_compressed_file))
        {
            max_mark_range = max_read_buffer_size;
            break;
        }

        max_mark_range = std::max(max_mark_range,
            getMark(right).offset_in_compressed_file - getMark(all_mark_ranges[i].begin).offset_in_compressed_file);
    }

    size_t buffer_size = std::min(max_read_buffer_size, max_mark_range);

    /// Estimate size of the data to be read.
    size_t estimated_size = 0;
    if (aio_threshold > 0)
    {
        for (const auto & mark_range : all_mark_ranges)
        {
            size_t offset_begin = (mark_range.begin > 0)
                ? getMark(mark_range.begin).offset_in_compressed_file
                : 0;

            size_t offset_end = (mark_range.end < marks_count)
                ? getMark(mark_range.end).offset_in_compressed_file
                : Poco::File(path_prefix + extension).getSize();

            if (offset_end > offset_begin)
                estimated_size += offset_end - offset_begin;
        }
    }

    /// Initialize the objects that shall be used to perform read operations.
    if (uncompressed_cache)
    {
        auto buffer = std::make_unique<CachedCompressedReadBuffer>(
            path_prefix + extension, uncompressed_cache, estimated_size, aio_threshold, buffer_size);

        if (profile_callback)
            buffer->setProfileCallback(profile_callback, clock_type);

        cached_buffer = std::move(buffer);
        data_buffer = cached_buffer.get();
    }
    else
    {
        auto buffer = std::make_unique<CompressedReadBufferFromFile>(
            path_prefix + extension, estimated_size, aio_threshold, buffer_size);

        if (profile_callback)
            buffer->setProfileCallback(profile_callback, clock_type);

        non_cached_buffer = std::move(buffer);
        data_buffer = non_cached_buffer.get();
    }
}

std::unique_ptr<MergeTreeReader::Stream> MergeTreeReader::Stream::createEmptyPtr()
{
    std::unique_ptr<Stream> res(new Stream);
    res->is_empty = true;
    return res;
}

const MarkInCompressedFile & MergeTreeReader::Stream::getMark(size_t index)
{
    if (!marks)
        loadMarks();
    return (*marks)[index];
}

void MergeTreeReader::Stream::loadMarks()
{
    std::string path;

    if (isNullStream(extension))
        path = path_prefix + ".null.mrk";
    else
        path = path_prefix + ".mrk";

    auto load = [&]() -> MarkCache::MappedPtr
    {
        /// Memory for marks must not be accounted as memory usage for query, because they are stored in shared cache.
        TemporarilyDisableMemoryTracker temporarily_disable_memory_tracker;

        size_t file_size = Poco::File(path).getSize();
        size_t expected_file_size = sizeof(MarkInCompressedFile) * marks_count;
        if (expected_file_size != file_size)
            throw Exception(
                    "bad size of marks file `" + path + "':" + std::to_string(file_size) + ", must be: "  + std::to_string(expected_file_size),
                    ErrorCodes::CORRUPTED_DATA);

        auto res = std::make_shared<MarksInCompressedFile>(marks_count);

        /// Read directly to marks.
        ReadBufferFromFile buffer(path, file_size, -1, reinterpret_cast<char *>(res->data()));

        if (buffer.eof() || buffer.buffer().size() != file_size)
            throw Exception("Cannot read all marks from file " + path, ErrorCodes::CANNOT_READ_ALL_DATA);

        return res;
    };

    if (mark_cache)
    {
        auto key = mark_cache->hash(path);
        if (save_marks_in_cache)
        {
            marks = mark_cache->getOrSet(key, load);
        }
        else
        {
            marks = mark_cache->get(key);
            if (!marks)
                marks = load();
        }
    }
    else
        marks = load();

    if (!marks)
        throw Exception("Failed to load marks: " + path, ErrorCodes::LOGICAL_ERROR);
}


void MergeTreeReader::Stream::seekToMark(size_t index)
{
    MarkInCompressedFile mark = getMark(index);

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
            e.addMessage("(while seeking to mark " + toString(index)
                + " of column " + path_prefix + "; offsets are: "
                + toString(mark.offset_in_compressed_file) + " "
                + toString(mark.offset_in_decompressed_block) + ")");

        throw;
    }
}


void MergeTreeReader::addStream(const String & name, const IDataType & type, const MarkRanges & all_mark_ranges,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback, clockid_t clock_type,
    size_t level)
{
    String escaped_column_name = escapeForFileName(name);

    const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type);
    bool data_file_exists = Poco::File(path + escaped_column_name + DATA_FILE_EXTENSION).exists();
    bool is_column_of_nested_type = type_arr && level == 0 && DataTypeNested::extractNestedTableName(name) != name;

    /** If data file is missing then we will not try to open it.
      * It is necessary since it allows to add new column to structure of the table without creating new files for old parts.
      * But we should try to load offset data for array columns of Nested subtable (their data will be filled by default value).
      */
    if (!data_file_exists && !is_column_of_nested_type)
        return;

    if (type.isNullable())
    {
        /// First create the stream that handles the null map of the given column.
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
        const IDataType & nested_type = *nullable_type.getNestedType();

        std::string filename = name + NULL_MAP_EXTENSION;

        streams.emplace(filename, std::make_unique<Stream>(
            path + escaped_column_name, NULL_MAP_EXTENSION, data_part->size,
            all_mark_ranges, mark_cache, save_marks_in_cache,
            uncompressed_cache, aio_threshold, max_read_buffer_size, profile_callback, clock_type));

        /// Then create the stream that handles the data of the given column.
        addStream(name, nested_type, all_mark_ranges, profile_callback, clock_type, level);
    }
    /// For arrays separate streams for sizes are used.
    else if (type_arr)
    {
        String size_name = DataTypeNested::extractNestedTableName(name)
            + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
        String escaped_size_name = escapeForFileName(DataTypeNested::extractNestedTableName(name))
            + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
        String size_path = path + escaped_size_name + DATA_FILE_EXTENSION;

        /// We have neither offsets nor data -> skipping, default values will be filled after
        if (!data_file_exists && !Poco::File(size_path).exists())
            return;

        if (!streams.count(size_name))
            streams.emplace(size_name, std::make_unique<Stream>(
                path + escaped_size_name, DATA_FILE_EXTENSION, data_part->size,
                all_mark_ranges, mark_cache, save_marks_in_cache,
                uncompressed_cache, aio_threshold, max_read_buffer_size, profile_callback, clock_type));

        if (data_file_exists)
            addStream(name, *type_arr->getNestedType(), all_mark_ranges, profile_callback, clock_type, level + 1);
        else
            streams.emplace(name, Stream::createEmptyPtr());
    }
    else
        streams.emplace(name, std::make_unique<Stream>(
            path + escaped_column_name, DATA_FILE_EXTENSION, data_part->size,
            all_mark_ranges, mark_cache, save_marks_in_cache,
            uncompressed_cache, aio_threshold, max_read_buffer_size, profile_callback, clock_type));
}


void MergeTreeReader::readData(
    const String & name, const IDataType & type, IColumn & column,
    size_t from_mark, bool continue_reading, size_t max_rows_to_read,
    size_t level, bool read_offsets)
{
    if (type.isNullable())
    {
        /// First read from the null map.
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
        const IDataType & nested_type = *nullable_type.getNestedType();

        ColumnNullable & nullable_col = static_cast<ColumnNullable &>(column);
        IColumn & nested_col = *nullable_col.getNestedColumn();

        std::string filename = name + NULL_MAP_EXTENSION;

        Stream & stream = *(streams.at(filename));
        if (!continue_reading)
            stream.seekToMark(from_mark);
        IColumn & col8 = nullable_col.getNullMapConcreteColumn();
        DataTypeUInt8{}.deserializeBinaryBulk(col8, *stream.data_buffer, max_rows_to_read, 0);

        /// Then read data.
        readData(name, nested_type, nested_col, from_mark, continue_reading, max_rows_to_read, level, read_offsets);
    }
    else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
    {
        /// For arrays the sizes must be deserialized first, then the values.
        if (read_offsets)
        {
            Stream & stream = *streams[DataTypeNested::extractNestedTableName(name) + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level)];
            if (!continue_reading)
                stream.seekToMark(from_mark);
            type_arr->deserializeOffsets(
                column,
                *stream.data_buffer,
                max_rows_to_read);
        }

        ColumnArray & array = typeid_cast<ColumnArray &>(column);
        const size_t required_internal_size = array.getOffsets().size() ? array.getOffsets()[array.getOffsets().size() - 1] : 0;

        readData(
            name,
            *type_arr->getNestedType(),
            array.getData(),
                 from_mark, continue_reading, required_internal_size - array.getData().size(),
            level + 1);

        size_t read_internal_size = array.getData().size();

        /// Fix for erroneously written empty files with array data.
        /// This can happen after ALTER that adds new columns to nested data structures.
        if (required_internal_size != read_internal_size)
        {
            if (read_internal_size != 0)
                LOG_ERROR(&Logger::get("MergeTreeReader"),
                    "Internal size of array " + name + " doesn't match offsets: corrupted data, filling with default values.");

            array.getDataPtr() = type_arr->getNestedType()->createConstColumn(
                    required_internal_size,
                    type_arr->getNestedType()->getDefault())->convertToFullColumnIfConst();

            /// NOTE: we could zero this column so that it won't get added to the block
            /// and later be recreated with more correct default values (from the table definition).
        }
    }
    else
    {
        Stream & stream = *streams[name];

        /// It means that data column of array column will be empty, and it will be replaced by const data column
        if (stream.isEmpty())
            return;

        double & avg_value_size_hint = avg_value_size_hints[name];
        if (!continue_reading)
            stream.seekToMark(from_mark);
        type.deserializeBinaryBulk(column, *stream.data_buffer, max_rows_to_read, avg_value_size_hint);

        IDataType::updateAvgValueSizeHint(column, avg_value_size_hint);
    }
}


void MergeTreeReader::fillMissingColumnsImpl(Block & res, const Names & ordered_names, bool always_reorder)
{
    if (!res)
        throw Exception("Empty block passed to fillMissingColumnsImpl", ErrorCodes::LOGICAL_ERROR);

    try
    {
        /// For a missing column of a nested data structure we must create not a column of empty
        /// arrays, but a column of arrays of correct length.
        /// TODO: If for some nested data structure only missing columns were selected, the arrays in these columns will be empty,
        /// even if the offsets for this nested structure are present in the current part. This can be fixed.
        /// NOTE: Similar, but slightly different code is present in Block::addDefaults.

        /// First, collect offset columns for all arrays in the block.
        OffsetColumns offset_columns;
        for (size_t i = 0; i < res.columns(); ++i)
        {
            const ColumnWithTypeAndName & column = res.safeGetByPosition(i);

            IColumn * observed_column;
            std::string column_name;
            if (column.column->isNullable())
            {
                ColumnNullable & nullable_col = static_cast<ColumnNullable &>(*(column.column));
                observed_column = nullable_col.getNestedColumn().get();
                column_name = observed_column->getName();
            }
            else
            {
                observed_column = column.column.get();
                column_name = column.name;
            }

            if (const ColumnArray * array = typeid_cast<const ColumnArray *>(observed_column))
            {
                String offsets_name = DataTypeNested::extractNestedTableName(column_name);
                auto & offsets_column = offset_columns[offsets_name];

                /// If for some reason multiple offsets columns are present for the same nested data structure,
                /// choose the one that is not empty.
                if (!offsets_column || offsets_column->empty())
                    offsets_column = array->getOffsetsColumn();
            }
        }

        auto should_evaluate_defaults = false;
        auto should_sort = always_reorder;

        for (const auto & requested_column : columns)
        {
            /// insert default values only for columns without default expressions
            if (!res.has(requested_column.name))
            {
                should_sort = true;
                if (storage.column_defaults.count(requested_column.name) != 0)
                {
                    should_evaluate_defaults = true;
                    continue;
                }

                ColumnWithTypeAndName column_to_add;
                column_to_add.name = requested_column.name;
                column_to_add.type = requested_column.type;

                String offsets_name = DataTypeNested::extractNestedTableName(column_to_add.name);
                if (offset_columns.count(offsets_name))
                {
                    ColumnPtr offsets_column = offset_columns[offsets_name];
                    DataTypePtr nested_type = typeid_cast<DataTypeArray &>(*column_to_add.type).getNestedType();
                    size_t nested_rows = offsets_column->empty() ? 0
                        : typeid_cast<ColumnUInt64 &>(*offsets_column).getData().back();

                    ColumnPtr nested_column = nested_type->createConstColumn(
                        nested_rows, nested_type->getDefault())->convertToFullColumnIfConst();

                    column_to_add.column = std::make_shared<ColumnArray>(nested_column, offsets_column);
                }
                else
                {
                    /// We must turn a constant column into a full column because the interpreter could infer that it is constant everywhere
                    /// but in some blocks (from other parts) it can be a full column.
                    column_to_add.column = column_to_add.type->createConstColumn(
                        res.rows(), column_to_add.type->getDefault())->convertToFullColumnIfConst();
                }

                res.insert(std::move(column_to_add));
            }
        }

        /// evaluate defaulted columns if necessary
        if (should_evaluate_defaults)
            evaluateMissingDefaults(res, columns, storage.column_defaults, storage.context);

        /// sort columns to ensure consistent order among all blocks
        if (should_sort)
        {
            Block ordered_block;

            for (const auto & name : ordered_names)
                if (res.has(name))
                    ordered_block.insert(res.getByName(name));

            std::swap(res, ordered_block);
        }
    }
    catch (Exception & e)
    {
        /// Better diagnostics.
        e.addMessage("(while reading from part " + path + ")");
        throw;
    }
}

}
