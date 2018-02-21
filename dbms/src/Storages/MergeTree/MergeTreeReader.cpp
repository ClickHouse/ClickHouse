#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <Common/escapeForFileName.h>
#include <Common/MemoryTracker.h>
#include <IO/CachedCompressedReadBuffer.h>
#include <IO/CompressedReadBufferFromFile.h>
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
            addStreams(column.name, *column.type, all_mark_ranges, profile_callback, clock_type);
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
                    column = ColumnArray::create(type_arr->getNestedType()->createColumn(), it_inserted.first->second);
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


const MarkInCompressedFile & MergeTreeReader::Stream::getMark(size_t index)
{
    if (!marks)
        loadMarks();
    return (*marks)[index];
}


void MergeTreeReader::Stream::loadMarks()
{
    std::string path = path_prefix + ".mrk";

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


void MergeTreeReader::addStreams(const String & name, const IDataType & type, const MarkRanges & all_mark_ranges,
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

        streams.emplace(stream_name, std::make_unique<Stream>(
            path + stream_name, DATA_FILE_EXTENSION, data_part->marks_count,
            all_mark_ranges, mark_cache, save_marks_in_cache,
            uncompressed_cache, aio_threshold, max_read_buffer_size, profile_callback, clock_type));
    };

    type.enumerateStreams(callback, {});
}


void MergeTreeReader::readData(
    const String & name, const IDataType & type, IColumn & column,
    size_t from_mark, bool continue_reading, size_t max_rows_to_read,
    bool with_offsets)
{
    IDataType::InputStreamGetter stream_getter = [&] (const IDataType::SubstreamPath & path) -> ReadBuffer *
    {
        /// If offsets for arrays have already been read.
        if (!with_offsets && !path.empty() && path.back().type == IDataType::Substream::ArraySizes)
            return nullptr;

        String stream_name = IDataType::getFileNameForStream(name, path);

        auto it = streams.find(stream_name);
        if (it == streams.end())
            return nullptr;

        Stream & stream = *it->second;

        if (!continue_reading)
            stream.seekToMark(from_mark);

        return stream.data_buffer;
    };

    double & avg_value_size_hint = avg_value_size_hints[name];
    type.deserializeBinaryBulkWithMultipleStreams(column, stream_getter, max_rows_to_read, avg_value_size_hint, true, {});
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


void MergeTreeReader::fillMissingColumns(Block & res, const Names & ordered_names, bool always_reorder)
{
    if (!res)
        throw Exception("Empty block passed to fillMissingColumns", ErrorCodes::LOGICAL_ERROR);

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

        bool should_evaluate_defaults = false;
        bool should_sort = always_reorder;

        size_t rows = res.rows();

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
                should_sort = true;
                if (storage.column_defaults.count(requested_column.name) != 0)
                {
                    should_evaluate_defaults = true;
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
                    size_t nested_rows = offsets_column->empty() ? 0
                        : typeid_cast<const ColumnUInt64 &>(*offsets_column).getData().back();

                    ColumnPtr nested_column = nested_type->createColumnConstWithDefaultValue(nested_rows)->convertToFullColumnIfConst();

                    column_to_add.column = ColumnArray::create(nested_column, offsets_column);
                }
                else
                {
                    /// We must turn a constant column into a full column because the interpreter could infer that it is constant everywhere
                    /// but in some blocks (from other parts) it can be a full column.
                    column_to_add.column = column_to_add.type->createColumnConstWithDefaultValue(rows)->convertToFullColumnIfConst();
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
