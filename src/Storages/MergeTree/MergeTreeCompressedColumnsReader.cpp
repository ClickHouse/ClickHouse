#include <Storages/MergeTree/MergeTreeCompressedColumnsReader.h>

#include <Common/Priority.h>

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

/// A stream of a column read by ranges of marks: of a range, from the block its first row is in, the rows before it left
/// out. A stream of a fixed width takes the bytes of the rows of the range; that of the bytes of strings, whose rows are
/// as long as their sizes say, the blocks up to the end of the range whole, of which the consumer takes what it needs.
struct MergeTreeCompressedColumnsReader::RangeStream
{
    RangeStream(
        const IMergeTreeDataPart & data_part,
        const NameAndTypePair & column,
        const MergeTreeReaderSettings & settings,
        const MarkRanges & mark_ranges,
        const ISerialization::SubstreamPath & path,
        std::optional<size_t> element_size_)
        : stream(openStream(data_part, column, settings, mark_ranges, path))
        , element_size(element_size_)
        , compressed_bytes(data_part.getColumnSize(column.name).data_compressed)
        , rows_of_part(data_part.rows_count)
    {
    }

    std::unique_ptr<MergeTreeReaderStream> stream;
    /// The bytes of a row, of a stream of a fixed width.
    const std::optional<size_t> element_size;
    const size_t compressed_bytes;
    const size_t rows_of_part;

    size_t range_bytes = 0;
    size_t bytes_to_skip = 0;
    size_t bytes_read = 0;
    /// What the consumer is given of the stream, of its compressed file, and about what the range has. Only sizes what
    /// the consumer keeps.
    size_t given_bytes = 0;
    size_t expected_bytes = 0;
    bool done = false;

    void startRange(const MarkRange & range, size_t num_rows)
    {
        bytes_read = 0;
        given_bytes = 0;
        done = false;
        range_bytes = num_rows * element_size.value_or(0);
        stream->adjustRightMark(range.end);
        stream->seekToMark(range.begin);
        bytes_to_skip = stream->getMark(range.begin).offset_in_decompressed_block;
        expected_bytes = rows_of_part == 0 ? 0 : compressed_bytes * num_rows / rows_of_part + 1;
    }

    /// Reads its next block of the range to `consumer`.
    void readBlock(Consumer & consumer, size_t column, Stream which)
    {
        const auto block = stream->readCompressedBlock();
        if (!block)
        {
            done = true;
            return;
        }

        const size_t skipped = std::min(bytes_to_skip, block->decompressed_bytes);
        bytes_to_skip -= skipped;
        size_t kept = block->decompressed_bytes - skipped;
        if (element_size)
            kept = std::min(kept, range_bytes - bytes_read);

        if (kept != 0)
        {
            consumer.addCompressedBlock(column, which, *block, skipped, kept, expected_bytes > given_bytes ? expected_bytes - given_bytes : 0);
            given_bytes += block->payload.size();
        }
        bytes_read += kept;
        if (element_size && bytes_read == range_bytes)
            done = true;
    }

    /// Reads the next piece in the background, if it does not already: a read buffer of a remote disk then has it once
    /// the consumer asks for it, rather than waiting for the request then.
    void prefetch() const
    {
        if (!done)
            stream->getDataBuffer()->prefetch(Priority{});
    }
};

/// A column: its values, or of a column of varying width - a `String` - its bytes and its sizes.
struct MergeTreeCompressedColumnsReader::ColumnReader
{
    ColumnReader(const IMergeTreeDataPart & data_part, const NameAndTypePair & column, const MergeTreeReaderSettings & settings, const MarkRanges & mark_ranges)
        : name(column.name)
        , values(
              data_part,
              column,
              settings,
              mark_ranges,
              {},
              isString(*column.type) ? std::nullopt : std::optional<size_t>(column.type->getSizeOfValueInMemory()))
        , rows_of_part(data_part.rows_count)
    {
        if (!isString(*column.type))
            return;
        sizes.emplace(data_part, column, settings, mark_ranges, stringSizesPath(), sizeof(UInt64));
        chars_total = streamBytesOf(data_part, column, {});
    }

    const String name;
    RangeStream values;
    std::optional<RangeStream> sizes;
    size_t chars_total = 0;
    const size_t rows_of_part;

    bool done() const { return values.done && (!sizes || sizes->done); }

    /// Of a `String`, the rows the bytes read make were every row as long as the average one.
    size_t rowsRead() const
    {
        if (!sizes)
            return values.bytes_read / *values.element_size;
        if (chars_total == 0)
            return sizes->bytes_read / sizeof(UInt64);
        return static_cast<size_t>(static_cast<unsigned __int128>(values.bytes_read) * rows_of_part / chars_total);
    }

    /// A `String` reads the sizes of its rows ahead of their bytes: a block of sizes whenever those read cover no more
    /// rows than the bytes read make, and only sizes once the bytes have ended.
    bool readsSizesNext() const { return sizes && !sizes->done && (values.done || sizes->bytes_read / sizeof(UInt64) <= rowsRead()); }

    void startRange(const MarkRange & range, size_t num_rows)
    {
        values.startRange(range, num_rows);
        if (sizes)
            sizes->startRange(range, num_rows);
    }

    void prefetch() const
    {
        values.prefetch();
        if (sizes)
            sizes->prefetch();
    }
};

MergeTreeCompressedColumnsReader::MergeTreeCompressedColumnsReader(
    const IMergeTreeDataPart & part_,
    const std::vector<NameAndTypePair> & columns,
    const MergeTreeReaderSettings & settings,
    const MarkRanges & ranges_)
    : part(part_)
    , ranges(ranges_.empty() ? MarkRanges{MarkRange(0, part_.index_granularity->getMarksCountWithoutFinal())} : ranges_)
    , prefetches(part_.isStoredOnRemoteDisk() && settings.read_settings.remote_fs_settings.prefetch)
{
    readers.reserve(columns.size());
    for (const auto & column : columns)
        readers.push_back(std::make_unique<ColumnReader>(part, column, settings, ranges));
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
    while (readNext(consumer))
    {
    }
    return rows_read;
}

bool MergeTreeCompressedColumnsReader::readNext(Consumer & consumer)
{
    while (true)
    {
        if (!range_rows && !startNextRange())
            return false;

        if (readPiece(consumer))
            return true;

        checkRowsRead(*range_rows);
        consumer.finishRange(*range_rows);
        rows_read += *range_rows;
        range_rows.reset();
    }
}

void MergeTreeCompressedColumnsReader::prefetchStart()
{
    if (!prefetches)
        return;
    if (!range_rows && !startNextRange())
        return;
    for (const auto & reader : readers)
        reader->prefetch();
}

bool MergeTreeCompressedColumnsReader::startNextRange()
{
    while (next_range < ranges.size())
    {
        const MarkRange & range = ranges[next_range++];
        const size_t num_rows = part.index_granularity->getRowsCountInRange(range);
        if (num_rows == 0)
            continue;
        for (auto & reader : readers)
            reader->startRange(range, num_rows);
        range_rows = num_rows;
        return true;
    }
    return false;
}

bool MergeTreeCompressedColumnsReader::readPiece(Consumer & consumer)
{
    size_t behind = readers.size();
    for (size_t i = 0; i < readers.size(); ++i)
    {
        if (!readers[i]->done() && (behind == readers.size() || readers[i]->rowsRead() < readers[behind]->rowsRead()))
            behind = i;
    }
    if (behind == readers.size())
        return false;

    ColumnReader & column = *readers[behind];
    if (column.readsSizesNext())
        column.sizes->readBlock(consumer, behind, Stream::Sizes);
    else
        column.values.readBlock(consumer, behind, Stream::Values);

    /// The column read gets its next piece in the background while the others are read and the consumer stages this one,
    /// so that a reader has a request in flight for each of its columns rather than one at a time.
    if (prefetches)
        column.prefetch();

    consumer.checkCancelled();
    return true;
}

void MergeTreeCompressedColumnsReader::checkRowsRead(size_t num_rows) const
{
    /// The bytes of a `String` are as many as its sizes say, which the consumer checks.
    for (const auto & reader : readers)
    {
        const RangeStream & fixed = reader->sizes ? *reader->sizes : reader->values;
        if (fixed.bytes_read != fixed.range_bytes)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Column {} of part {} holds {} bytes{}, where {} rows of {} bytes are read",
                reader->name,
                part.name,
                fixed.bytes_read,
                reader->sizes ? " of sizes" : "",
                num_rows,
                *fixed.element_size);
    }
}

}
