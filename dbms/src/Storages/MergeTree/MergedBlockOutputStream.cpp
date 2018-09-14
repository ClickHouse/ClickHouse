#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <IO/createWriteBufferFromFileBase.h>
#include <Common/escapeForFileName.h>
#include <DataTypes/NestedUtils.h>
#include <Common/StringUtils/StringUtils.h>
#include <Common/typeid_cast.h>
#include <Common/MemoryTracker.h>
#include <Poco/File.h>


namespace DB
{

namespace
{

constexpr auto DATA_FILE_EXTENSION = ".bin";
constexpr auto MARKS_FILE_EXTENSION = ".mrk";

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


void IMergedBlockOutputStream::addStreams(
    const String & path,
    const String & name,
    const IDataType & type,
    size_t estimated_size,
    bool skip_offsets)
{
    IDataType::StreamCallback callback = [&] (const IDataType::SubstreamPath & substream_path)
    {
        if (skip_offsets && !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes)
            return;

        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        /// Shared offsets for Nested type.
        if (column_streams.count(stream_name))
            return;

        column_streams[stream_name] = std::make_unique<ColumnStream>(
            stream_name,
            path + stream_name, DATA_FILE_EXTENSION,
            path + stream_name, MARKS_FILE_EXTENSION,
            max_compress_block_size,
            compression_settings,
            estimated_size,
            aio_threshold);
    };

    IDataType::SubstreamPath stream_path;
    type.enumerateStreams(callback, stream_path);
}


IDataType::OutputStreamGetter IMergedBlockOutputStream::createStreamGetter(
        const String & name, OffsetColumns & offset_columns, bool skip_offsets)
{
    return [&, skip_offsets] (const IDataType::SubstreamPath & substream_path) -> WriteBuffer *
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
        if (is_offsets && skip_offsets)
            return nullptr;

        String stream_name = IDataType::getFileNameForStream(name, substream_path);

        /// Don't write offsets more than one time for Nested type.
        if (is_offsets && offset_columns.count(stream_name))
            return nullptr;

        return &column_streams[stream_name]->compressed;
    };
}


void IMergedBlockOutputStream::writeData(
    const String & name,
    const IDataType & type,
    const IColumn & column,
    OffsetColumns & offset_columns,
    bool skip_offsets,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state)
{
    auto & settings = storage.context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = createStreamGetter(name, offset_columns, skip_offsets);
    serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part != 0;

    size_t size = column.size();
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

            /// Write marks.
            type.enumerateStreams([&] (const IDataType::SubstreamPath & substream_path)
            {
                bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
                if (is_offsets && skip_offsets)
                    return;

                String stream_name = IDataType::getFileNameForStream(name, substream_path);

                /// Don't write offsets more than one time for Nested type.
                if (is_offsets && offset_columns.count(stream_name))
                    return;

                ColumnStream & stream = *column_streams[stream_name];

                /// There could already be enough data to compress into the new block.
                if (stream.compressed.offset() >= min_compress_block_size)
                    stream.compressed.next();

                writeIntBinary(stream.plain_hashing.count(), stream.marks);
                writeIntBinary(stream.compressed.offset(), stream.marks);
            }, serialize_settings.path);
        }

        type.serializeBinaryBulkWithMultipleStreams(column, prev_mark, limit, serialize_settings, serialization_state);

        /// So that instead of the marks pointing to the end of the compressed block, there were marks pointing to the beginning of the next one.
        type.enumerateStreams([&] (const IDataType::SubstreamPath & substream_path)
        {
            bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
            if (is_offsets && skip_offsets)
                return;

            String stream_name = IDataType::getFileNameForStream(name, substream_path);

            /// Don't write offsets more than one time for Nested type.
            if (is_offsets && offset_columns.count(stream_name))
                return;

            column_streams[stream_name]->compressed.nextIfAtEnd();
        }, serialize_settings.path);

        prev_mark += limit;
    }

    /// Memoize offsets for Nested types, that are already written. They will not be written again for next columns of Nested structure.
    type.enumerateStreams([&] (const IDataType::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
        if (is_offsets)
        {
            String stream_name = IDataType::getFileNameForStream(name, substream_path);
            offset_columns.insert(stream_name);
        }
    }, serialize_settings.path);
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
        addStreams(part_path, it.name, *it.type, 0, false);
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
        addStreams(part_path, it.name, *it.type, estimated_size, false);
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
    /// Finish columns serialization.
    if (!serialization_states.empty())
    {
        auto & settings = storage.context.getSettingsRef();
        IDataType::SerializeBinaryBulkSettings serialize_settings;
        serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
        serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part != 0;
        OffsetColumns offset_columns;
        auto it = columns_list.begin();
        for (size_t i = 0; i < columns_list.size(); ++i, ++it)
        {
            serialize_settings.getter = createStreamGetter(it->name, offset_columns, false);
            it->type->serializeBinaryBulkStateSuffix(serialize_settings, serialization_states[i]);
        }
    }

    if (!total_column_list)
        total_column_list = &columns_list;

    /// Finish write and get checksums.
    MergeTreeData::DataPart::Checksums checksums;

    if (additional_column_checksums)
        checksums = std::move(*additional_column_checksums);

    if (index_stream)
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

    if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
    {
        new_part->partition.store(storage, part_path, checksums);
        if (new_part->minmax_idx.initialized)
            new_part->minmax_idx.store(storage, part_path, checksums);
        else if (rows_count)
            throw Exception("MinMax index was not initialized for new non-empty part " + new_part->name
                + ". It is a bug.", ErrorCodes::LOGICAL_ERROR);

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
    new_part->index.assign(std::make_move_iterator(index_columns.begin()), std::make_move_iterator(index_columns.end()));
    new_part->checksums = checksums;
    new_part->bytes_on_disk = MergeTreeData::DataPart::calculateTotalSizeOnDisk(new_part->getFullPath());
}

void MergedBlockOutputStream::init()
{
    Poco::File(part_path).createDirectories();

    if (storage.hasPrimaryKey())
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

    /// The set of written offset columns so that you do not write shared offsets of nested structures columns several times
    OffsetColumns offset_columns;

    auto sort_columns = storage.getPrimarySortColumns();

    /// Here we will add the columns related to the Primary Key, then write the index.
    std::vector<ColumnWithTypeAndName> primary_columns(sort_columns.size());
    std::map<String, size_t> primary_columns_name_to_position;

    for (size_t i = 0, size = sort_columns.size(); i < size; ++i)
    {
        const auto & name = sort_columns[i];

        if (!primary_columns_name_to_position.emplace(name, i).second)
            throw Exception("Primary key contains duplicate columns", ErrorCodes::BAD_ARGUMENTS);

        primary_columns[i] = block.getByName(name);

        /// Reorder primary key columns in advance and add them to `primary_columns`.
        if (permutation)
            primary_columns[i].column = primary_columns[i].column->permute(*permutation, 0);
    }

    if (index_columns.empty())
    {
        index_columns.resize(sort_columns.size());
        for (size_t i = 0, size = sort_columns.size(); i < size; ++i)
            index_columns[i] = primary_columns[i].column->cloneEmpty();
    }

    if (serialization_states.empty())
    {
        serialization_states.reserve(columns_list.size());
        OffsetColumns tmp_offset_columns;
        IDataType::SerializeBinaryBulkSettings settings;

        for (const auto & col : columns_list)
        {
            settings.getter = createStreamGetter(col.name, tmp_offset_columns, false);
            serialization_states.emplace_back(nullptr);
            col.type->serializeBinaryBulkStatePrefix(settings, serialization_states.back());
        }
    }

    /// Now write the data.
    auto it = columns_list.begin();
    for (size_t i = 0; i < columns_list.size(); ++i, ++it)
    {
        const ColumnWithTypeAndName & column = block.getByName(it->name);

        if (permutation)
        {
            auto primary_column_it = primary_columns_name_to_position.find(it->name);
            if (primary_columns_name_to_position.end() != primary_column_it)
            {
                auto & primary_column = *primary_columns[primary_column_it->second].column;
                writeData(column.name, *column.type, primary_column, offset_columns, false, serialization_states[i]);
            }
            else
            {
                /// We rearrange the columns that are not included in the primary key here; Then the result is released - to save RAM.
                ColumnPtr permuted_column = column.column->permute(*permutation, 0);
                writeData(column.name, *column.type, *permuted_column, offset_columns, false, serialization_states[i]);
            }
        }
        else
        {
            writeData(column.name, *column.type, *column.column, offset_columns, false, serialization_states[i]);
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
        auto temporarily_disable_memory_tracker = getCurrentMemoryTrackerActionLock();

        /// Write index. The index contains Primary Key value for each `index_granularity` row.
        for (size_t i = index_offset; i < rows; i += storage.index_granularity)
        {
            if (storage.hasPrimaryKey())
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
    MergeTreeData & storage_, const Block & header_, String part_path_, bool sync_, CompressionSettings compression_settings, bool skip_offsets_)
    : IMergedBlockOutputStream(
        storage_, storage_.context.getSettings().min_compress_block_size,
        storage_.context.getSettings().max_compress_block_size, compression_settings,
        storage_.context.getSettings().min_bytes_to_use_direct_io),
    header(header_), part_path(part_path_), sync(sync_), skip_offsets(skip_offsets_)
{
}

void MergedColumnOnlyOutputStream::write(const Block & block)
{
    if (!initialized)
    {
        column_streams.clear();
        serialization_states.clear();
        serialization_states.reserve(block.columns());
        OffsetColumns tmp_offset_columns;
        IDataType::SerializeBinaryBulkSettings settings;

        for (size_t i = 0; i < block.columns(); ++i)
        {
            const auto & col = block.safeGetByPosition(i);

            addStreams(part_path, col.name, *col.type, 0, skip_offsets);
            serialization_states.emplace_back(nullptr);
            settings.getter = createStreamGetter(col.name, tmp_offset_columns, false);
            col.type->serializeBinaryBulkStatePrefix(settings, serialization_states.back());
        }

        initialized = true;
    }

    size_t rows = block.rows();

    OffsetColumns offset_columns;
    for (size_t i = 0; i < block.columns(); ++i)
    {
        const ColumnWithTypeAndName & column = block.safeGetByPosition(i);
        writeData(column.name, *column.type, *column.column, offset_columns, skip_offsets, serialization_states[i]);
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
    /// Finish columns serialization.
    auto & settings = storage.context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part != 0;
    OffsetColumns offset_columns;
    for (size_t i = 0; i < header.columns(); ++i)
    {
        auto & column = header.safeGetByPosition(i);
        serialize_settings.getter = createStreamGetter(column.name, offset_columns, skip_offsets);
        column.type->serializeBinaryBulkStateSuffix(serialize_settings, serialization_states[i]);
    }

    MergeTreeData::DataPart::Checksums checksums;

    for (auto & column_stream : column_streams)
    {
        column_stream.second->finalize();
        if (sync)
            column_stream.second->sync();

        column_stream.second->addToChecksums(checksums);
    }

    column_streams.clear();
    serialization_states.clear();
    initialized = false;

    return checksums;
}

}
