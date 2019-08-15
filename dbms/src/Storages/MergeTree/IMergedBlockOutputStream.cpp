#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <IO/createWriteBufferFromFileBase.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{
    constexpr auto DATA_FILE_EXTENSION = ".bin";
}


IMergedBlockOutputStream::IMergedBlockOutputStream(
    MergeTreeData & storage_,
    size_t min_compress_block_size_,
    size_t max_compress_block_size_,
    CompressionCodecPtr codec_,
    size_t aio_threshold_,
    bool blocks_are_granules_size_,
    const MergeTreeIndexGranularity & index_granularity_)
    : storage(storage_)
    , min_compress_block_size(min_compress_block_size_)
    , max_compress_block_size(max_compress_block_size_)
    , aio_threshold(aio_threshold_)
    , marks_file_extension(storage.canUseAdaptiveGranularity() ? getAdaptiveMrkExtension() : getNonAdaptiveMrkExtension())
    , blocks_are_granules_size(blocks_are_granules_size_)
    , index_granularity(index_granularity_)
    , compute_granularity(index_granularity.empty())
    , codec(std::move(codec_))
    , with_final_mark(storage.settings.write_final_mark && storage.canUseAdaptiveGranularity())
{
    if (blocks_are_granules_size && !index_granularity.empty())
        throw Exception("Can't take information about index granularity from blocks, when non empty index_granularity array specified", ErrorCodes::LOGICAL_ERROR);
}


void IMergedBlockOutputStream::addStreams(
    const String & path,
    const String & name,
    const IDataType & type,
    const CompressionCodecPtr & effective_codec,
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
            path + stream_name, marks_file_extension,
            effective_codec,
            max_compress_block_size,
            estimated_size,
            aio_threshold);
    };

    IDataType::SubstreamPath stream_path;
    type.enumerateStreams(callback, stream_path);
}


IDataType::OutputStreamGetter IMergedBlockOutputStream::createStreamGetter(
        const String & name, WrittenOffsetColumns & offset_columns, bool skip_offsets)
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

void fillIndexGranularityImpl(
    const Block & block,
    size_t index_granularity_bytes,
    size_t fixed_index_granularity_rows,
    bool blocks_are_granules,
    size_t index_offset,
    MergeTreeIndexGranularity & index_granularity,
    bool can_use_adaptive_index_granularity)
{
    size_t rows_in_block = block.rows();
    size_t index_granularity_for_block;
    if (!can_use_adaptive_index_granularity)
        index_granularity_for_block = fixed_index_granularity_rows;
    else
    {
        size_t block_size_in_memory = block.bytes();
        if (blocks_are_granules)
            index_granularity_for_block = rows_in_block;
        else if (block_size_in_memory >= index_granularity_bytes)
        {
            size_t granules_in_block = block_size_in_memory / index_granularity_bytes;
            index_granularity_for_block = rows_in_block / granules_in_block;
        }
        else
        {
            size_t size_of_row_in_bytes = block_size_in_memory / rows_in_block;
            index_granularity_for_block = index_granularity_bytes / size_of_row_in_bytes;
        }
    }
    if (index_granularity_for_block == 0) /// very rare case when index granularity bytes less then single row
        index_granularity_for_block = 1;

    /// We should be less or equal than fixed index granularity
    index_granularity_for_block = std::min(fixed_index_granularity_rows, index_granularity_for_block);

    for (size_t current_row = index_offset; current_row < rows_in_block; current_row += index_granularity_for_block)
        index_granularity.appendMark(index_granularity_for_block);
}

void IMergedBlockOutputStream::fillIndexGranularity(const Block & block)
{
    fillIndexGranularityImpl(
        block,
        storage.settings.index_granularity_bytes,
        storage.settings.index_granularity,
        blocks_are_granules_size,
        index_offset,
        index_granularity,
        storage.canUseAdaptiveGranularity());
}

void IMergedBlockOutputStream::writeSingleMark(
    const String & name,
    const IDataType & type,
    WrittenOffsetColumns & offset_columns,
    bool skip_offsets,
    size_t number_of_rows,
    DB::IDataType::SubstreamPath & path)
{
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
         if (storage.canUseAdaptiveGranularity())
             writeIntBinary(number_of_rows, stream.marks);
     }, path);
}

size_t IMergedBlockOutputStream::writeSingleGranule(
    const String & name,
    const IDataType & type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    bool skip_offsets,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state,
    IDataType::SerializeBinaryBulkSettings & serialize_settings,
    size_t from_row,
    size_t number_of_rows,
    bool write_marks)
{
    if (write_marks)
        writeSingleMark(name, type, offset_columns, skip_offsets, number_of_rows, serialize_settings.path);

    type.serializeBinaryBulkWithMultipleStreams(column, from_row, number_of_rows, serialize_settings, serialization_state);

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

    return from_row + number_of_rows;
}

/// column must not be empty. (column.size() !== 0)

std::pair<size_t, size_t> IMergedBlockOutputStream::writeColumn(
    const String & name,
    const IDataType & type,
    const IColumn & column,
    WrittenOffsetColumns & offset_columns,
    bool skip_offsets,
    IDataType::SerializeBinaryBulkStatePtr & serialization_state,
    size_t from_mark)
{
    auto & settings = storage.global_context.getSettingsRef();
    IDataType::SerializeBinaryBulkSettings serialize_settings;
    serialize_settings.getter = createStreamGetter(name, offset_columns, skip_offsets);
    serialize_settings.low_cardinality_max_dictionary_size = settings.low_cardinality_max_dictionary_size;
    serialize_settings.low_cardinality_use_single_dictionary_for_part = settings.low_cardinality_use_single_dictionary_for_part != 0;

    size_t total_rows = column.size();
    size_t current_row = 0;
    size_t current_column_mark = from_mark;
    while (current_row < total_rows)
    {
        size_t rows_to_write;
        bool write_marks = true;

        /// If there is `index_offset`, then the first mark goes not immediately, but after this number of rows.
        if (current_row == 0 && index_offset != 0)
        {
            write_marks = false;
            rows_to_write = index_offset;
        }
        else
        {
            if (index_granularity.getMarksCount() <= current_column_mark)
                throw Exception(
                    "Incorrect size of index granularity expect mark " + toString(current_column_mark) + " totally have marks " + toString(index_granularity.getMarksCount()),
                    ErrorCodes::LOGICAL_ERROR);

            rows_to_write = index_granularity.getMarkRows(current_column_mark);
        }

        current_row = writeSingleGranule(
            name,
            type,
            column,
            offset_columns,
            skip_offsets,
            serialization_state,
            serialize_settings,
            current_row,
            rows_to_write,
            write_marks
        );

        if (write_marks)
            current_column_mark++;
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

    return std::make_pair(current_column_mark, current_row - total_rows);
}

void IMergedBlockOutputStream::writeFinalMark(
    const std::string & column_name,
    const DataTypePtr column_type,
    WrittenOffsetColumns & offset_columns,
    bool skip_offsets,
    DB::IDataType::SubstreamPath & path)
{
    writeSingleMark(column_name, *column_type, offset_columns, skip_offsets, 0, path);
    /// Memoize information about offsets
    column_type->enumerateStreams([&] (const IDataType::SubstreamPath & substream_path)
    {
        bool is_offsets = !substream_path.empty() && substream_path.back().type == IDataType::Substream::ArraySizes;
        if (is_offsets)
        {
            String stream_name = IDataType::getFileNameForStream(column_name, substream_path);
            offset_columns.insert(stream_name);
        }
    }, path);
}


/// Implementation of IMergedBlockOutputStream::ColumnStream.

IMergedBlockOutputStream::ColumnStream::ColumnStream(
    const String & escaped_column_name_,
    const String & data_path_,
    const std::string & data_file_extension_,
    const std::string & marks_path_,
    const std::string & marks_file_extension_,
    const CompressionCodecPtr & compression_codec_,
    size_t max_compress_block_size_,
    size_t estimated_size_,
    size_t aio_threshold_) :
    escaped_column_name(escaped_column_name_),
    data_file_extension{data_file_extension_},
    marks_file_extension{marks_file_extension_},
    plain_file(createWriteBufferFromFileBase(data_path_ + data_file_extension, estimated_size_, aio_threshold_, max_compress_block_size_)),
    plain_hashing(*plain_file), compressed_buf(plain_hashing, compression_codec_), compressed(compressed_buf),
    marks_file(marks_path_ + marks_file_extension, 4096, O_TRUNC | O_CREAT | O_WRONLY), marks(marks_file)
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

}
