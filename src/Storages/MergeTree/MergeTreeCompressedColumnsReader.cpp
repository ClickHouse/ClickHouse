#include <Storages/MergeTree/MergeTreeCompressedColumnsReader.h>

#include <DataTypes/IDataType.h>
#include <Interpreters/Context.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/LoadedMergeTreeDataPartInfoForReader.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeReaderStream.h>

#include <algorithm>
#include <optional>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace
{

/// What a column of values decompressed on the host asks the consumer room for at a time.
constexpr size_t piece_bytes = 4UL << 20;

}

struct MergeTreeCompressedColumnsReader::ColumnReader
{
    ColumnReader(
        const IMergeTreeDataPart & data_part,
        const NameAndTypePair & column,
        const MergeTreeReaderSettings & settings,
        bool compressed_,
        const MarkRanges & mark_ranges)
        : name(column.name)
        , is_variable(isString(*column.type))
        , compressed(compressed_)
        , element_size(is_variable ? 1 : column.type->getSizeOfValueInMemory())
        , rows_of_part(data_part.rows_count)
    {
        if (!mark_ranges.empty())
        {
            if (is_variable)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} of varying width of part {} is read in ranges of marks", column.name, data_part.name);
            compressed_bytes_of_part = data_part.getColumnSize(column.name).data_compressed;
        }

        stream = openStream(data_part, column, settings, mark_ranges);
        expected_bytes = compressed ? data_part.getColumnSize(column.name).data_compressed
                                    : (is_variable ? data_part.getColumnSize(column.name).data_uncompressed : data_part.rows_count * element_size);

        if (is_variable)
        {
            sizes_stream = openStream(data_part, column, settings, mark_ranges, stringSizesPath());
            chars_total = streamBytesOf(data_part, column, {});
            sizes_total = streamBytesOf(data_part, column, stringSizesPath());
        }
    }

    const String name;
    const bool is_variable;
    /// Whether the stream is read as its compressed blocks, or through the buffer that decompresses them.
    const bool compressed;
    std::unique_ptr<MergeTreeReaderStream> stream;
    const size_t element_size;
    size_t bytes_read = 0;
    /// What the consumer is given of the column: its compressed file when it is read compressed, its values otherwise.
    /// Only sizes what the consumer keeps.
    size_t expected_bytes = 0;
    size_t given_bytes = 0;
    bool done = false;

    /// Of a column of varying width: the stream of its sizes, the bytes of them read, and the bytes of both of its
    /// streams in the part.
    std::unique_ptr<MergeTreeReaderStream> sizes_stream;
    size_t sizes_bytes_read = 0;
    size_t sizes_total = 0;
    size_t chars_total = 0;
    bool sizes_done = false;
    bool bytes_done = false;

    /// Of a read in ranges of marks: of the range being read, the bytes of its rows and the bytes before them in the
    /// block it starts in, which are left out.
    std::optional<size_t> range_bytes;
    size_t bytes_to_skip = 0;
    size_t compressed_bytes_of_part = 0;
    const size_t rows_of_part;

    /// From the first block of the range, of its compressed blocks, or from its first row, of the values decompressed
    /// on the host; the stream reads no further than the end of the range.
    void startRange(const MarkRange & range, size_t num_rows)
    {
        bytes_read = 0;
        given_bytes = 0;
        done = false;
        range_bytes = num_rows * element_size;
        stream->adjustRightMark(range.end);
        stream->seekToMark(range.begin);
        if (compressed)
        {
            bytes_to_skip = stream->getMark(range.begin).offset_in_decompressed_block;
            expected_bytes = rows_of_part == 0 ? 0 : compressed_bytes_of_part * num_rows / rows_of_part + 1;
        }
        else
        {
            expected_bytes = *range_bytes;
        }
    }

    /// Of a column of varying width, the rows the bytes read make were every row as long as the average one.
    size_t rowsRead() const
    {
        if (!is_variable)
            return bytes_read / element_size;
        if (chars_total == 0)
            return sizes_bytes_read / sizeof(UInt64);
        return static_cast<size_t>(static_cast<unsigned __int128>(bytes_read) * rows_of_part / chars_total);
    }

    size_t remainingBytes() const { return expected_bytes > given_bytes ? expected_bytes - given_bytes : 0; }

    size_t remainingSizesBytes() const { return sizes_total > sizes_bytes_read ? sizes_total - sizes_bytes_read : 0; }

    /// A column of varying width reads the sizes of its rows ahead of their bytes: a block of sizes whenever those read
    /// cover no more rows than the bytes read make, and only sizes once the bytes have ended.
    bool readsSizesNext() const
    {
        return is_variable && !sizes_done && (bytes_done || sizes_bytes_read / sizeof(UInt64) <= rowsRead());
    }

    /// The file of the column's values, or of the bytes of a column of varying width, has ended.
    void finishBytes()
    {
        bytes_done = true;
        done = !is_variable || sizes_done;
    }

    void finishSizes()
    {
        sizes_done = true;
        done = bytes_done;
    }
};

MergeTreeCompressedColumnsReader::MergeTreeCompressedColumnsReader(
    const IMergeTreeDataPart & part_,
    const std::vector<NameAndTypePair> & columns,
    const MergeTreeReaderSettings & settings,
    bool compressed,
    const MarkRanges & ranges_)
    : part(part_)
    , ranges(ranges_)
{
    readers.reserve(columns.size());
    for (const auto & column : columns)
        readers.push_back(std::make_unique<ColumnReader>(part, column, settings, compressed, ranges));
}

MergeTreeCompressedColumnsReader::~MergeTreeCompressedColumnsReader() = default;

std::unique_ptr<MergeTreeReaderStream> MergeTreeCompressedColumnsReader::openStream(
    const IMergeTreeDataPart & part,
    const NameAndTypePair & column,
    const MergeTreeReaderSettings & settings,
    const MarkRanges & ranges,
    const ISerialization::SubstreamPath & substream_path)
{
    chassert(part.getType() == MergeTreeDataPartType::Wide);

    const auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(
        column, substream_path, ".bin", part.checksums, part.storage.getSettings());
    if (!stream_name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} has no file for a stream of column {}", part.name, column.name);

    const size_t marks_count = part.getMarksCount();
    auto marks_loader = std::make_shared<MergeTreeMarksLoader>(
        std::make_shared<LoadedMergeTreeDataPartInfoForReader>(part.shared_from_this(), std::make_shared<AlterConversions>()),
        part.storage.getContext()->getMarkCache().get(),
        part.index_granularity_info.getMarksFilePath(*stream_name),
        marks_count,
        part.index_granularity_info,
        settings.save_marks_in_cache,
        settings.read_settings,
        /*load_marks_threadpool=*/ nullptr,
        /*num_columns_in_mark=*/ 1,
        settings.use_streaming_marks_compression);

    return std::make_unique<MergeTreeReaderStreamSingleColumn>(
        part.getDataPartStoragePtr(),
        *stream_name,
        ".bin",
        marks_count,
        ranges.empty() ? MarkRanges{MarkRange(0, marks_count)} : ranges,
        settings,
        /*uncompressed_cache=*/ nullptr,
        part.getFileSizeOrZero(*stream_name + ".bin"),
        std::move(marks_loader),
        ReadBufferFromFileBase::ProfileCallback{},
        CLOCK_MONOTONIC_COARSE);
}

ISerialization::SubstreamPath MergeTreeCompressedColumnsReader::stringSizesPath()
{
    ISerialization::SubstreamPath path;
    path.push_back(ISerialization::Substream::StringSizes);
    return path;
}

size_t MergeTreeCompressedColumnsReader::streamBytesOf(
    const IMergeTreeDataPart & part, const NameAndTypePair & column, const ISerialization::SubstreamPath & substream_path)
{
    const auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column, substream_path, ".bin", part.checksums, part.storage.getSettings());
    if (!stream_name)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} has no file for a stream of column {}", part.name, column.name);
    return part.checksums.files.at(*stream_name + ".bin").uncompressed_size;
}

size_t MergeTreeCompressedColumnsReader::read(Consumer & consumer)
{
    if (ranges.empty())
    {
        readRows(consumer, part.rows_count);
        return part.rows_count;
    }

    size_t num_rows_read = 0;
    for (const MarkRange & range : ranges)
    {
        const size_t num_rows = part.index_granularity->getRowsCountInRange(range);
        if (num_rows == 0)
            continue;
        for (auto & reader : readers)
            reader->startRange(range, num_rows);
        readRows(consumer, num_rows);
        num_rows_read += num_rows;
    }
    return num_rows_read;
}

void MergeTreeCompressedColumnsReader::readSizes(Consumer & consumer, size_t column_index)
{
    ColumnReader & column = *readers[column_index];

    if (!column.compressed)
    {
        const size_t left = column.remainingSizesBytes();
        if (left == 0)
        {
            column.finishSizes();
            return;
        }
        const std::span<char> room = consumer.reserveBytes(column_index, Stream::Sizes, piece_bytes, left);
        const size_t wanted = std::min(room.size() / sizeof(UInt64) * sizeof(UInt64), left);
        if (wanted == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "No room for a size of column {} of part {}", column.name, part.name);
        size_t read = 0;
        while (read < wanted)
        {
            const size_t got = column.sizes_stream->getDataBuffer()->readBig(room.data() + read, wanted - read);
            if (got == 0)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "The sizes of column {} of part {} ended after {} bytes of {}",
                    column.name,
                    part.name,
                    column.sizes_bytes_read + read,
                    column.sizes_total);
            read += got;
        }
        consumer.commitBytes(column_index, Stream::Sizes, read);
        column.sizes_bytes_read += read;
        return;
    }

    const auto block = column.sizes_stream->readCompressedBlock();
    if (!block)
    {
        column.finishSizes();
        return;
    }

    consumer.addCompressedBlock(column_index, Stream::Sizes, *block, 0, block->decompressed_bytes, column.remainingSizesBytes());
    column.sizes_bytes_read += block->decompressed_bytes;
}

void MergeTreeCompressedColumnsReader::readRows(Consumer & consumer, size_t num_rows)
{
    while (true)
    {
        size_t behind = readers.size();
        for (size_t i = 0; i < readers.size(); ++i)
        {
            if (!readers[i]->done && (behind == readers.size() || readers[i]->rowsRead() < readers[behind]->rowsRead()))
                behind = i;
        }
        if (behind == readers.size())
            break;

        ColumnReader & column = *readers[behind];
        if (column.readsSizesNext())
        {
            readSizes(consumer, behind);
        }
        else if (!column.compressed)
        {
            if (column.remainingBytes() == 0)
            {
                /// A range ends where its rows do. Of a whole part, every value is given, and the consumer's room holds
                /// exactly them. Asking for more only to learn that the file has ended would make it grow; a byte past
                /// the end is counted instead, so the check below reports it.
                if (!column.range_bytes)
                {
                    char past_end;
                    column.bytes_read += column.stream->getDataBuffer()->readBig(&past_end, 1);
                }
                column.finishBytes();
                continue;
            }

            const std::span<char> room = consumer.reserveBytes(behind, Stream::Values, piece_bytes, column.remainingBytes());
            const size_t read = column.stream->getDataBuffer()->readBig(
                room.data(), column.range_bytes ? std::min(room.size(), column.remainingBytes()) : room.size());
            consumer.commitBytes(behind, Stream::Values, read);
            if (read == 0)
            {
                column.finishBytes();
                continue;
            }

            column.bytes_read += read;
            column.given_bytes += read;
        }
        else
        {
            const auto block = column.stream->readCompressedBlock();
            if (!block)
            {
                column.finishBytes();
                continue;
            }

            /// Of a range, the rows before it in its first block and after it in its last are left out.
            size_t skipped = 0;
            size_t kept = block->decompressed_bytes;
            if (column.range_bytes)
            {
                skipped = std::min(column.bytes_to_skip, block->decompressed_bytes);
                column.bytes_to_skip -= skipped;
                kept = std::min(block->decompressed_bytes - skipped, *column.range_bytes - column.bytes_read);
            }

            if (kept != 0)
            {
                consumer.addCompressedBlock(behind, Stream::Values, *block, skipped, kept, column.remainingBytes());
                column.given_bytes += block->payload.size();
            }
            column.bytes_read += kept;
            if (column.range_bytes && column.bytes_read == *column.range_bytes)
                column.finishBytes();
        }

        consumer.checkCancelled();
    }

    for (const auto & reader : readers)
    {
        if (reader->is_variable)
        {
            if (reader->sizes_bytes_read != num_rows * sizeof(UInt64) || reader->bytes_read != reader->chars_total)
                throw Exception(
                    ErrorCodes::LOGICAL_ERROR,
                    "Column {} of varying width of part {} holds {} bytes of sizes and {} bytes of {}, where the part has {} rows",
                    reader->name,
                    part.name,
                    reader->sizes_bytes_read,
                    reader->bytes_read,
                    reader->chars_total,
                    num_rows);
            continue;
        }

        if (reader->bytes_read != num_rows * reader->element_size)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of part {} holds {} bytes, where {} rows of {} bytes are read",
                reader->name,
                part.name,
                reader->bytes_read,
                num_rows,
                reader->element_size);
    }
}

}
