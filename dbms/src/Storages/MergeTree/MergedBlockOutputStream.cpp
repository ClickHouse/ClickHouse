#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <IO/createWriteBufferFromFileBase.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/DataTypeNested.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnNullable.h>
#include <Common/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/MemoryTracker.h>
#include <Poco/File.h>


namespace DB
{

namespace
{

constexpr auto DATA_FILE_EXTENSION = ".bin";
constexpr auto MARKS_FILE_EXTENSION = ".mrk";
constexpr auto NULL_MAP_EXTENSION = ".null.bin";
constexpr auto NULL_MARKS_FILE_EXTENSION = ".null.mrk";

}

/// Implementation of IMergedBlockOutputStream.

IMergedBlockOutputStream::IMergedBlockOutputStream(
    MergeTreeData & storage_,
    size_t min_compress_block_size_,
    size_t max_compress_block_size_,
    CompressionSettings compression_settings_,
    size_t aio_threshold_)
    : storage(storage_),
    min_compress_block_size(min_compress_block_size_),
    max_compress_block_size(max_compress_block_size_),
    aio_threshold(aio_threshold_),
    compression_settings(compression_settings_)
{
}


void IMergedBlockOutputStream::addStream(
    const String & path,
    const String & name,
    const IDataType & type,
    size_t estimated_size,
    size_t level,
    const String & filename,
    bool skip_offsets)
{
    String escaped_column_name;
    if (filename.size())
        escaped_column_name = escapeForFileName(filename);
    else
        escaped_column_name = escapeForFileName(name);

    if (type.isNullable())
    {
        /// First create the stream that handles the null map of the given column.
        const DataTypeNullable & nullable_type = static_cast<const DataTypeNullable &>(type);
        const IDataType & nested_type = *nullable_type.getNestedType();

        std::string null_map_name = name + NULL_MAP_EXTENSION;
        column_streams[null_map_name] = std::make_unique<ColumnStream>(
            escaped_column_name,
            path + escaped_column_name, NULL_MAP_EXTENSION,
            path + escaped_column_name, NULL_MARKS_FILE_EXTENSION,
            max_compress_block_size,
            compression_settings,
            estimated_size,
            aio_threshold);

        /// Then create the stream that handles the data of the given column.
        addStream(path, name, nested_type, estimated_size, level, filename, false);
    }
    else if (const DataTypeArray * type_arr = typeid_cast<const DataTypeArray *>(&type))
    {
        if (!skip_offsets)
        {
            /// For arrays, separate files are used for sizes.
            String size_name = DataTypeNested::extractNestedTableName(name)
                + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);
            String escaped_size_name = escapeForFileName(DataTypeNested::extractNestedTableName(name))
                + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);

            column_streams[size_name] = std::make_unique<ColumnStream>(
                escaped_size_name,
                path + escaped_size_name, DATA_FILE_EXTENSION,
                path + escaped_size_name, MARKS_FILE_EXTENSION,
                max_compress_block_size,
                compression_settings,
                estimated_size,
                aio_threshold);
        }

        addStream(path, name, *type_arr->getNestedType(), estimated_size, level + 1, "", false);
    }
    else
    {
        column_streams[name] = std::make_unique<ColumnStream>(
            escaped_column_name,
            path + escaped_column_name, DATA_FILE_EXTENSION,
            path + escaped_column_name, MARKS_FILE_EXTENSION,
            max_compress_block_size,
            compression_settings,
            estimated_size,
            aio_threshold);
    }
}


void IMergedBlockOutputStream::writeData(
    const String & name,
    const DataTypePtr & type,
    const ColumnPtr & column,
    OffsetColumns & offset_columns,
    size_t level,
    bool skip_offsets)
{
    writeDataImpl(name, type, column, nullptr, offset_columns, level, skip_offsets);
}


void IMergedBlockOutputStream::writeDataImpl(
    const String & name,
    const DataTypePtr & type,
    const ColumnPtr & column,
    const ColumnPtr & offsets,
    OffsetColumns & offset_columns,
    size_t level,
    bool skip_offsets)
{
    /// NOTE: the parameter write_array_data indicates whether we call this method
    /// to write the contents of an array. This is to cope with the fact that
    /// serialization of arrays for the MergeTree engine slightly differs from
    /// what the other engines do.

    if (type->isNullable())
    {
        /// First write to the null map.
        const auto & nullable_type = static_cast<const DataTypeNullable &>(*type);
        const auto & nested_type = nullable_type.getNestedType();

        const auto & nullable_col = static_cast<const ColumnNullable &>(*column);
        const auto & nested_col = nullable_col.getNestedColumn();

        std::string filename = name + NULL_MAP_EXTENSION;
        ColumnStream & stream = *column_streams[filename];
        auto null_map_type = std::make_shared<DataTypeUInt8>();

        writeColumn(nullable_col.getNullMapColumn(), null_map_type, stream, offsets);

        /// Then write data.
        writeDataImpl(name, nested_type, nested_col, offsets, offset_columns, level, skip_offsets);
    }
    else if (auto type_arr = typeid_cast<const DataTypeArray *>(type.get()))
    {
        /// For arrays, you first need to serialize dimensions, and then values.
        String size_name = DataTypeNested::extractNestedTableName(name)
            + ARRAY_SIZES_COLUMN_NAME_SUFFIX + toString(level);

        const auto & column_array = typeid_cast<const ColumnArray &>(*column);

        ColumnPtr next_level_offsets;
        ColumnPtr lengths_column;

        auto offsets_data_type = std::make_shared<DataTypeNumber<ColumnArray::Offset_t>>();

        if (offsets)
        {
            /// Have offsets from prev level. Calculate offsets for next level.
            next_level_offsets = offsets->clone();
            const auto & array_offsets = column_array.getOffsets();
            auto & next_level_offsets_column = typeid_cast<ColumnArray::ColumnOffsets_t &>(*next_level_offsets);
            auto & next_level_offsets_data = next_level_offsets_column.getData();
            for (auto & offset : next_level_offsets_data)
                offset = offset ? array_offsets[offset - 1] : 0;

            /// Calculate lengths of arrays and write them as a new array.
            lengths_column = column_array.getLengthsColumn();
        }

        if (!skip_offsets && offset_columns.count(size_name) == 0)
        {
            offset_columns.insert(size_name);

            ColumnStream & stream = *column_streams[size_name];
            if (offsets)
                writeColumn(lengths_column, offsets_data_type, stream, offsets);
            else
                writeColumn(column, type, stream, nullptr);
        }

        writeDataImpl(name, type_arr->getNestedType(), column_array.getDataPtr(),
                      offsets ? next_level_offsets : column_array.getOffsetsColumn(),
                      offset_columns, level + 1, skip_offsets);
    }
    else
    {
        ColumnStream & stream = *column_streams[name];
        writeColumn(column, type, stream, offsets);
    }
}

void IMergedBlockOutputStream::writeColumn(
        const ColumnPtr & column,
        const DataTypePtr & type,
        IMergedBlockOutputStream::ColumnStream & stream,
        ColumnPtr offsets)
{
    std::shared_ptr<DataTypeArray> array_type_holder;
    DataTypeArray * array_type;
    ColumnPtr array_column;

    if (offsets)
    {
        array_type_holder = std::make_shared<DataTypeArray>(type);
        array_type = array_type_holder.get();
        array_column =  std::make_shared<ColumnArray>(column, offsets);
    }
    else
        array_type = typeid_cast<DataTypeArray *>(type.get());

    size_t size = offsets ? offsets->size() : column->size();
    size_t prev_mark = 0;
    while (prev_mark < size)
    {
        size_t limit = 0;

        /// If there is `index_offset`, then the first mark goes not immediately, but after this number of rows.
        if (prev_mark == 0 && index_offset != 0)
            limit = index_offset;
        else
        {
            limit = storage.index_granularity;

            /// There could already be enough data to compress into the new block.
            if (stream.compressed.offset() >= min_compress_block_size)
                stream.compressed.next();

            writeIntBinary(stream.plain_hashing.count(), stream.marks);
            writeIntBinary(stream.compressed.offset(), stream.marks);
        }

        if (offsets)
            array_type->serializeBinaryBulk(*array_column, stream.compressed, prev_mark, limit);
        else if (array_type)
            array_type->serializeOffsets(*column, stream.compressed, prev_mark, limit);
        else
            type->serializeBinaryBulk(*column, stream.compressed, prev_mark, limit);

        /// So that instead of the marks pointing to the end of the compressed block, there were marks pointing to the beginning of the next one.
        stream.compressed.nextIfAtEnd();

        prev_mark += limit;
    }
}


/// Implementation of IMergedBlockOutputStream::ColumnStream.

IMergedBlockOutputStream::ColumnStream::ColumnStream(
    const String & escaped_column_name_,
    const String & data_path,
    const std::string & data_file_extension_,
    const std::string & marks_path,
    const std::string & marks_file_extension_,
    size_t max_compress_block_size,
    CompressionSettings compression_settings,
    size_t estimated_size,
    size_t aio_threshold) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    plain_file(createWriteBufferFromFileBase(data_path + data_file_extension, estimated_size, aio_threshold, max_compress_block_size)),
    plain_hashing(*plain_file), compressed_buf(plain_hashing, compression_settings), compressed(compressed_buf),
    marks_file(marks_path + marks_file_extension, 4096, O_TRUNC | O_CREAT | O_WRONLY), marks(marks_file)
{
}

void IMergedBlockOutputStream::ColumnStream::finalize()
{
    compressed.next();
    plain_file->next();
    marks.next();
}

void IMergedBlockOutputStream::ColumnStream::sync()
{
    plain_file->sync();
    marks_file.sync();
}

void IMergedBlockOutputStream::ColumnStream::addToChecksums(MergeTreeData::DataPart::Checksums & checksums)
{
    String name = escaped_column_name;

    checksums.files[name + data_file_extension].is_compressed = true;
    checksums.files[name + data_file_extension].uncompressed_size = compressed.count();
    checksums.files[name + data_file_extension].uncompressed_hash = compressed.getHash();
    checksums.files[name + data_file_extension].file_size = plain_hashing.count();
    checksums.files[name + data_file_extension].file_hash = plain_hashing.getHash();

    checksums.files[name + marks_file_extension].file_size = marks.count();
    checksums.files[name + marks_file_extension].file_hash = marks.getHash();
}


/// Implementation of MergedBlockOutputStream.

MergedBlockOutputStream::MergedBlockOutputStream(
    MergeTreeData & storage_,
    String part_path_,
    const NamesAndTypesList & columns_list_,
    CompressionSettings compression_settings)
    : IMergedBlockOutputStream(
        storage_, storage_.context.getSettings().min_compress_block_size,
        storage_.context.getSettings().max_compress_block_size, compression_settings,
        storage_.context.getSettings().min_bytes_to_use_direct_io),
    columns_list(columns_list_), part_path(part_path_)
{
    init();
    for (const auto & it : columns_list)
        addStream(part_path, it.name, *it.type, 0, 0, "", false);
}

MergedBlockOutputStream::MergedBlockOutputStream(
    MergeTreeData & storage_,
    String part_path_,
    const NamesAndTypesList & columns_list_,
    CompressionSettings compression_settings,
    const MergeTreeData::DataPart::ColumnToSize & merged_column_to_size_,
    size_t aio_threshold_)
    : IMergedBlockOutputStream(
        storage_, storage_.context.getSettings().min_compress_block_size,
        storage_.context.getSettings().max_compress_block_size, compression_settings,
        aio_threshold_),
    columns_list(columns_list_), part_path(part_path_)
{
    init();
    for (const auto & it : columns_list)
    {
        size_t estimated_size = 0;
        if (aio_threshold > 0)
        {
            auto it2 = merged_column_to_size_.find(it.name);
            if (it2 != merged_column_to_size_.end())
                estimated_size = it2->second;
        }
        addStream(part_path, it.name, *it.type, estimated_size, 0, "", false);
    }
}

std::string MergedBlockOutputStream::getPartPath() const
{
    return part_path;
}

 /// If data is pre-sorted.
void MergedBlockOutputStream::write(const Block & block)
{
    writeImpl(block, nullptr);
}

/** If the data is not sorted, but we pre-calculated the permutation, after which they will be sorted.
    * This method is used to save RAM, since you do not need to keep two blocks at once - the source and the sorted.
    */
void MergedBlockOutputStream::writeWithPermutation(const Block & block, const IColumn::Permutation * permutation)
{
    writeImpl(block, permutation);
}

void MergedBlockOutputStream::writeSuffix()
{
    throw Exception("Method writeSuffix is not supported by MergedBlockOutputStream", ErrorCodes::NOT_IMPLEMENTED);
}

void MergedBlockOutputStream::writeSuffixAndFinalizePart(
        MergeTreeData::MutableDataPartPtr & new_part,
        const NamesAndTypesList * total_column_list,
        MergeTreeData::DataPart::Checksums * additional_column_checksums)
{
    if (!total_column_list)
        total_column_list = &columns_list;

    /// Finish write and get checksums.
    MergeTreeData::DataPart::Checksums checksums;

    if (additional_column_checksums)
        checksums = std::move(*additional_column_checksums);

    if (storage.merging_params.mode != MergeTreeData::MergingParams::Unsorted)
    {
        index_stream->next();
        checksums.files["primary.idx"].file_size = index_stream->count();
        checksums.files["primary.idx"].file_hash = index_stream->getHash();
        index_stream = nullptr;
    }

    for (ColumnStreams::iterator it = column_streams.begin(); it != column_streams.end(); ++it)
    {
        it->second->finalize();
        it->second->addToChecksums(checksums);
    }

    column_streams.clear();

    if (rows_count == 0)
    {
        /// A part is empty - all records are deleted.
        Poco::File(part_path).remove(true);
        return;
    }

    if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        new_part->partition.store(storage, part_path, checksums);
        new_part->minmax_idx.store(storage, part_path, checksums);

        WriteBufferFromFile count_out(part_path + "count.txt", 4096);
        HashingWriteBuffer count_out_hashing(count_out);
        writeIntText(rows_count, count_out_hashing);
        count_out_hashing.next();
        checksums.files["count.txt"].file_size = count_out_hashing.count();
        checksums.files["count.txt"].file_hash = count_out_hashing.getHash();
    }

    {
        /// Write a file with a description of columns.
        WriteBufferFromFile out(part_path + "columns.txt", 4096);
        total_column_list->writeText(out);
    }

    {
        /// Write file with checksums.
        WriteBufferFromFile out(part_path + "checksums.txt", 4096);
        checksums.write(out);
    }

    new_part->rows_count = rows_count;
    new_part->marks_count = marks_count;
    new_part->modification_time = time(nullptr);
    new_part->columns = *total_column_list;
    new_part->index.swap(index_columns);
    new_part->checksums = checksums;
    new_part->size_in_bytes = MergeTreeData::DataPart::calcTotalSize(new_part->getFullPath());
}

void MergedBlockOutputStream::init()
{
    Poco::File(part_path).createDirectories();

    if (storage.merging_params.mode != MergeTreeData::MergingParams::Unsorted)
    {
        index_file_stream = std::make_unique<WriteBufferFromFile>(
            part_path + "primary.idx", DBMS_DEFAULT_BUFFER_SIZE, O_TRUNC | O_CREAT | O_WRONLY);
        index_stream = std::make_unique<HashingWriteBuffer>(*index_file_stream);
    }
}


void MergedBlockOutputStream::writeImpl(const Block & block, const IColumn::Permutation * permutation)
{
    block.checkNumberOfRows();
    size_t rows = block.rows();

    /// The set of written offset columns so that you do not write mutual to nested structures columns several times
    OffsetColumns offset_columns;

    auto sort_description = storage.getSortDescription();

    /// Here we will add the columns related to the Primary Key, then write the index.
    std::vector<ColumnWithTypeAndName> primary_columns(sort_description.size());
    std::map<String, size_t> primary_columns_name_to_position;

    for (size_t i = 0, size = sort_description.size(); i < size; ++i)
    {
        const auto & descr = sort_description[i];

        String name = !descr.column_name.empty()
            ? descr.column_name
            : block.safeGetByPosition(descr.column_number).name;

        if (!primary_columns_name_to_position.emplace(name, i).second)
            throw Exception("Primary key contains duplicate columns", ErrorCodes::BAD_ARGUMENTS);

        primary_columns[i] = !descr.column_name.empty()
            ? block.getByName(descr.column_name)
            : block.safeGetByPosition(descr.column_number);

        /// Reorder primary key columns in advance and add them to `primary_columns`.
        if (permutation)
            primary_columns[i].column = primary_columns[i].column->permute(*permutation, 0);
    }

    if (index_columns.empty())
    {
        index_columns.resize(sort_description.size());
        for (size_t i = 0, size = sort_description.size(); i < size; ++i)
            index_columns[i] = primary_columns[i].column->cloneEmpty();
    }

    /// Now write the data.
    for (const auto & it : columns_list)
    {
        const ColumnWithTypeAndName & column = block.getByName(it.name);

        if (permutation)
        {
            auto primary_column_it = primary_columns_name_to_position.find(it.name);
            if (primary_columns_name_to_position.end() != primary_column_it)
            {
                writeData(column.name, column.type, primary_columns[primary_column_it->second].column, offset_columns, 0, false);
            }
            else
            {
                /// We rearrange the columns that are not included in the primary key here; Then the result is released - to save RAM.
                ColumnPtr permutted_column = column.column->permute(*permutation, 0);
                writeData(column.name, column.type, permutted_column, offset_columns, 0, false);
            }
        }
        else
        {
            writeData(column.name, column.type, column.column, offset_columns, 0, false);
        }
    }

    rows_count += rows;

    {
        /** While filling index (index_columns), disable memory tracker.
          * Because memory is allocated here (maybe in context of INSERT query),
          *  but then freed in completely different place (while merging parts), where query memory_tracker is not available.
          * And otherwise it will look like excessively growing memory consumption in context of query.
          *  (observed in long INSERT SELECTs)
          */
        TemporarilyDisableMemoryTracker temporarily_disable_memory_tracker;

        /// Write index. The index contains Primary Key value for each `index_granularity` row.
        for (size_t i = index_offset; i < rows; i += storage.index_granularity)
        {
            if (storage.merging_params.mode != MergeTreeData::MergingParams::Unsorted)
            {
                for (size_t j = 0, size = primary_columns.size(); j < size; ++j)
                {
                    const IColumn & primary_column = *primary_columns[j].column.get();
                    index_columns[j]->insertFrom(primary_column, i);
                    primary_columns[j].type->serializeBinary(primary_column, i, *index_stream);
                }
            }

            ++marks_count;
        }
    }

    size_t written_for_last_mark = (storage.index_granularity - index_offset + rows) % storage.index_granularity;
    index_offset = (storage.index_granularity - written_for_last_mark) % storage.index_granularity;
}


/// Implementation of MergedColumnOnlyOutputStream.

MergedColumnOnlyOutputStream::MergedColumnOnlyOutputStream(
    MergeTreeData & storage_, String part_path_, bool sync_, CompressionSettings compression_settings, bool skip_offsets_)
    : IMergedBlockOutputStream(
        storage_, storage_.context.getSettings().min_compress_block_size,
        storage_.context.getSettings().max_compress_block_size, compression_settings,
        storage_.context.getSettings().min_bytes_to_use_direct_io),
    part_path(part_path_), sync(sync_), skip_offsets(skip_offsets_)
{
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    if (!initialized)
    {
        column_streams.clear();
        for (size_t i = 0; i < block.columns(); ++i)
        {
            addStream(part_path, block.safeGetByPosition(i).name,
                *block.safeGetByPosition(i).type, 0, 0, block.safeGetByPosition(i).name, skip_offsets);
        }
        initialized = true;
    }

    size_t rows = block.rows();

    OffsetColumns offset_columns;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        writeData(column.name, column.type, column.column, offset_columns, 0, skip_offsets);
    }

    size_t written_for_last_mark = (storage.index_granularity - index_offset + rows) % storage.index_granularity;
    index_offset = (storage.index_granularity - written_for_last_mark) % storage.index_granularity;
}

void MergedColumnOnlyOutputStream::writeSuffix()
{
    throw Exception("Method writeSuffix is not supported by MergedColumnOnlyOutputStream", ErrorCodes::NOT_IMPLEMENTED);
}

MergeTreeData::DataPart::Checksums MergedColumnOnlyOutputStream::writeSuffixAndGetChecksums()
{
    MergeTreeData::DataPart::Checksums checksums;

    for (auto & column_stream : column_streams)
    {
        column_stream.second->finalize();
        if (sync)
            column_stream.second->sync();

        column_stream.second->addToChecksums(checksums);
    }

    column_streams.clear();
    initialized = false;

    return checksums;
}

}
