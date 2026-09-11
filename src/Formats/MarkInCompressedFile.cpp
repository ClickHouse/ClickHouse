#include <Formats/MarkInCompressedFile.h>

#include <algorithm>
#include <array>
#include <iterator>
#include <mutex>
#include <Common/BitHelpers.h>
#include <Common/Exception.h>
#include <Compression/CompressedReadBufferFromFile.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int CORRUPTED_DATA;
    extern const int CANNOT_READ_ALL_DATA;
}

namespace
{
    /// Index blocks using the same validation as `CompressedReadBufferFromFile`.
    class CompressedMarksReader : public CompressedReadBufferBase
    {
    public:
        using CompressedReadBufferBase::CompressedReadBufferBase;
        using CompressedReadBufferBase::readCompressedData;
    };
}

struct MarksInCompressedFile::CompressedFile
{
    struct Block
    {
        size_t compressed_offset;
        size_t decompressed_offset;
    };

    PODArray<char, 4096, JemallocCacheAllocator> content;
    PODArray<Block, 4096, JemallocCacheAllocator> blocks;
    String file_name;
    size_t num_columns;
    size_t row_size;
};

struct MarksInCompressedFile::Reader::Impl
{
    explicit Impl(std::shared_ptr<const CompressedFile> file_) : file(std::move(file_))
    {
        for (auto & reader : readers)
        {
            reader = std::make_unique<CompressedReadBufferFromFile>(std::make_unique<ReadBufferFromOutsideMemoryFile>(
                file->file_name, std::string_view(file->content.data(), file->content.size())));
            /// The immutable compressed bytes were already checksummed while indexing.
            reader->disableChecksumming();
        }
    }

    const std::shared_ptr<const CompressedFile> file;
    std::mutex mutex;
    std::array<std::unique_ptr<CompressedReadBufferFromFile>, 2> readers;
    size_t next_eviction = 0;

    MarkInCompressedFile get(size_t index)
    {
        std::lock_guard lock(mutex);
        size_t offset = index / file->num_columns * file->row_size + index % file->num_columns * sizeof(MarkInCompressedFile);
        auto end = std::upper_bound(file->blocks.begin(), file->blocks.end(), offset,
            [](size_t value, const CompressedFile::Block & block) { return value < block.decompressed_offset; });
        for (size_t i = 0; i < readers.size(); ++i)
        {
            if (!readers[i]->buffer().empty() && static_cast<size_t>(readers[i]->getPosition()) == end->compressed_offset)
            {
                next_eviction = i;
                break;
            }
        }
        auto & reader = *readers[next_eviction];
        if (reader.isCanceled())
            throw Exception(ErrorCodes::CANNOT_READ_ALL_DATA, "Cannot reuse a failed marks reader for {}", file->file_name);
        next_eviction = (next_eviction + 1) % readers.size();
        const auto & block = *std::prev(end);
        reader.seek(block.compressed_offset, offset - block.decompressed_offset);
        MarkInCompressedFile mark;
        readBinaryLittleEndian(mark.offset_in_compressed_file, reader);
        readBinaryLittleEndian(mark.offset_in_decompressed_block, reader);
        return mark;
    }
};

MarksInCompressedFile::Reader::Reader(std::shared_ptr<const CompressedFile> file) : impl(std::make_unique<Impl>(std::move(file))) {}
MarksInCompressedFile::Reader::~Reader() = default;

MarkInCompressedFile MarksInCompressedFile::Reader::get(size_t idx)
{
    return impl->get(idx);
}

String MarkInCompressedFile::toString() const
{
    return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ")";
}

String MarkInCompressedFile::toStringWithRows(size_t rows_num) const
{
    return "(" + DB::toString(offset_in_compressed_file) + "," + DB::toString(offset_in_decompressed_block) + ","
        + DB::toString(rows_num) + ")";
}

std::shared_ptr<MarksInCompressedFile> MarksInCompressedFile::create(const PlainArray & marks)
{
    Builder builder(marks.size());
    builder.addAllMarks(marks.data(), marks.size());
    return builder.finish();
}

std::shared_ptr<MarksInCompressedFile> MarksInCompressedFile::createFromCompressedFile(
    PODArray<char, 4096, JemallocCacheAllocator> && content, size_t num_rows,
    size_t num_columns, bool adaptive, const String & file_name)
{
    if (!num_columns || num_columns > (std::numeric_limits<size_t>::max() - sizeof(UInt64)) / sizeof(MarkInCompressedFile))
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid column count {} in marks file {}", num_columns, file_name);

    auto file = std::make_shared<CompressedFile>();
    file->content = std::move(content);
    file->file_name = file_name;
    file->num_columns = num_columns;
    file->row_size = num_columns * sizeof(MarkInCompressedFile) + (adaptive ? sizeof(UInt64) : 0);
    if (num_rows > std::numeric_limits<size_t>::max() / file->row_size)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid row count {} in marks file {}", num_rows, file_name);
    size_t expected_size = num_rows * file->row_size;
    size_t decompressed_offset = 0;
    ReadBufferFromMemory input(file->content.data(), file->content.size());
    CompressedMarksReader reader(&input);
    try
    {
        while (!input.eof())
        {
            size_t offset = input.count();
            size_t decompressed_size = 0;
            size_t compressed_size = 0;
            size_t total_size = reader.readCompressedData(decompressed_size, compressed_size, false);
            if (!total_size || !decompressed_size || decompressed_size > expected_size - decompressed_offset)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected size of compressed marks block at offset {}", offset);

            file->blocks.push_back(CompressedFile::Block{offset, decompressed_offset});
            decompressed_offset += decompressed_size;
        }
        if (decompressed_offset != expected_size)
            throw Exception(ErrorCodes::CORRUPTED_DATA, "Unexpected decompressed size {}, expected {}", decompressed_offset, expected_size);
        /// Sentinel for locating the last block and recognizing a reader positioned at its end.
        file->blocks.push_back(CompressedFile::Block{file->content.size(), decompressed_offset});
    }
    catch (Exception & e)
    {
        e.addMessage("While indexing compressed marks from {}", file_name);
        throw;
    }
    auto result = std::shared_ptr<MarksInCompressedFile>(new MarksInCompressedFile(num_rows * num_columns, {}, {}));
    result->compressed_file = std::move(file);
    return result;
}

std::unique_ptr<MarksInCompressedFile::Reader> MarksInCompressedFile::createReader() const
{
    return compressed_file ? std::unique_ptr<Reader>(new Reader(compressed_file)) : nullptr;
}

MarkInCompressedFile MarksInCompressedFile::get(size_t idx, Reader * reader) const
{
    if (idx >= num_marks)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Mark index {} is out of range [0, {})",
            idx, num_marks);

    if (compressed_file)
    {
        if (reader)
        {
            chassert(reader->impl->file == compressed_file);
            return reader->get(idx);
        }
        Reader local_reader(compressed_file);
        return local_reader.get(idx);
    }

    auto [block, offset] = lookUpMark(idx);
    size_t x = block->min_x + readBitsPacked64(packed.data(), offset, block->bits_for_x);
    size_t y = block->min_y + (readBitsPacked64(packed.data(), offset + block->bits_for_x, block->bits_for_y) << block->trailing_zero_bits_in_y);
    return MarkInCompressedFile{.offset_in_compressed_file = x, .offset_in_decompressed_block = y};
}

std::tuple<const MarksInCompressedFile::BlockInfo *, size_t> MarksInCompressedFile::lookUpMark(size_t idx) const
{
    size_t block_idx = idx / MARKS_PER_BLOCK;
    const BlockInfo & block = blocks[block_idx];
    size_t offset = block.bit_offset_in_packed_array + (idx - block_idx * MARKS_PER_BLOCK) * (block.bits_for_x + block.bits_for_y);
    return {&block, offset};
}

size_t MarksInCompressedFile::approximateMemoryUsage() const
{
    if (compressed_file)
        return sizeof(*this) + sizeof(CompressedFile) + compressed_file->content.allocated_bytes()
            + compressed_file->blocks.allocated_bytes() + compressed_file->file_name.capacity();
    return sizeof(*this) + blocks.allocated_bytes() + packed.allocated_bytes();
}

MarksInCompressedFile::MarksInCompressedFile(
    size_t num_marks_,
    PODArray<BlockInfo, 4096, JemallocCacheAllocator> && blocks_,
    PODArray<UInt64, 4096, JemallocCacheAllocator> && packed_)
    : num_marks(num_marks_)
    , blocks(std::move(blocks_))
    , packed(std::move(packed_))
{
}

MarksInCompressedFile::Builder::Builder(size_t total_marks_)
    : total_marks(total_marks_)
    , blocks((total_marks_ + MARKS_PER_BLOCK - 1) / MARKS_PER_BLOCK, BlockInfo{})
{
}

void MarksInCompressedFile::Builder::addAllMarks(const MarkInCompressedFile * marks, size_t count)
{
    chassert(pending.empty() && marks_flushed == 0);
    chassert(count == total_marks);

    while (count > 0)
    {
        size_t chunk = std::min(MARKS_PER_BLOCK, count);
        flushBlock(marks, chunk);
        marks += chunk;
        count -= chunk;
    }
}

void MarksInCompressedFile::Builder::addMarks(const MarkInCompressedFile * marks, size_t count)
{
    chassert(marks_flushed + pending.size() + count <= total_marks);

    /// If there are pending marks from a previous call, fill up that block first.
    if (!pending.empty())
    {
        size_t marks_to_copy = std::min(MARKS_PER_BLOCK - pending.size(), count);
        pending.insert(pending.end(), marks, marks + marks_to_copy);
        marks += marks_to_copy;
        count -= marks_to_copy;

        if (pending.size() == MARKS_PER_BLOCK)
        {
            flushBlock(pending.data(), pending.size());
            pending.clear();
        }
    }

    /// Process full blocks directly from input without copying.
    while (count >= MARKS_PER_BLOCK)
    {
        flushBlock(marks, MARKS_PER_BLOCK);
        marks += MARKS_PER_BLOCK;
        count -= MARKS_PER_BLOCK;
    }

    /// Buffer remaining marks for the next call.
    if (count > 0)
        pending.insert(pending.end(), marks, marks + count);
}

void MarksInCompressedFile::Builder::flushBlock(const MarkInCompressedFile * data, size_t count)
{
    chassert(count > 0 && count <= MARKS_PER_BLOCK);

    size_t block_idx = marks_flushed / MARKS_PER_BLOCK;
    BlockInfo & block = blocks[block_idx];
    block.bit_offset_in_packed_array = packed_bits;

    /// Compute block metadata: min values, bit widths.
    size_t max_x = 0;
    size_t max_y = 0;
    for (size_t i = 0; i < count; ++i)
    {
        block.min_x = std::min(block.min_x, data[i].offset_in_compressed_file);
        max_x = std::max(max_x, data[i].offset_in_compressed_file);
        block.min_y = std::min(block.min_y, data[i].offset_in_decompressed_block);
        max_y = std::max(max_y, data[i].offset_in_decompressed_block);
        block.trailing_zero_bits_in_y
            = std::min(block.trailing_zero_bits_in_y, static_cast<UInt8>(getTrailingZeroBits(data[i].offset_in_decompressed_block)));
    }

    block.bits_for_x = static_cast<UInt8>(sizeof(size_t) * 8 - getLeadingZeroBits(max_x - block.min_x));
    block.bits_for_y
        = static_cast<UInt8>(sizeof(size_t) * 8 - getLeadingZeroBits((max_y - block.min_y) >> block.trailing_zero_bits_in_y));

    /// Grow packed array to fit new bits + 1 overallocation element for writeBitsPacked64 safety.
    size_t new_bits = count * (block.bits_for_x + block.bits_for_y);
    size_t new_packed_length = (packed_bits + new_bits + 63) / 64 + 1;
    if (new_packed_length > packed.size())
        packed.resize_fill(new_packed_length);

    /// Write bit-packed deltas.
    size_t bit_offset = packed_bits;
    for (size_t i = 0; i < count; ++i)
    {
        writeBitsPacked64(packed.data(), bit_offset, data[i].offset_in_compressed_file - block.min_x);
        writeBitsPacked64(
            packed.data(),
            bit_offset + block.bits_for_x,
            (data[i].offset_in_decompressed_block - block.min_y) >> block.trailing_zero_bits_in_y);
        bit_offset += block.bits_for_x + block.bits_for_y;
    }

    packed_bits += new_bits;
    marks_flushed += count;
}

std::shared_ptr<MarksInCompressedFile> MarksInCompressedFile::Builder::finish()
{
    /// Flush remaining buffered marks (last incomplete block).
    if (!pending.empty())
    {
        flushBlock(pending.data(), pending.size());
        pending.clear();
    }

    chassert(marks_flushed == total_marks);

    /// +1 overallocation element is needed by readBitsPacked64, but only for non-empty marks.
    size_t required_length = total_marks == 0 ? 0 : (packed_bits + 63) / 64 + 1;
    if (packed.size() < required_length)
        packed.resize_fill(required_length);

    /// Shrink packed to exact size so approximateMemoryUsage reports the true
    /// compressed size without power-of-two slack from incremental growth.
    PODArray<UInt64, 4096, JemallocCacheAllocator> exact_packed;
    exact_packed.reserve_exact(required_length);
    exact_packed.resize_fill(required_length);
    if (required_length > 0)
        memcpy(exact_packed.data(), packed.data(), required_length * sizeof(UInt64));
    packed = std::move(exact_packed);

    return std::shared_ptr<MarksInCompressedFile>(
        new MarksInCompressedFile(total_marks, std::move(blocks), std::move(packed)));
}

}
