#include <Compression/CachedCompressedReadBuffer.h>

#include <IO/WriteHelpers.h>
#include <Compression/LZ4_decompress_faster.h>

#include <utility>


namespace DB
{

namespace ErrorCodes
{
    extern const int SEEK_POSITION_OUT_OF_BOUND;
}


void CachedCompressedReadBuffer::initInput()
{
    if (!file_in)
    {
        file_in = file_in_creator();
        compressed_in = file_in.get();

        if (profile_callback)
            file_in->setProfileCallback(profile_callback, clock_type);
    }
}


void CachedCompressedReadBuffer::prefetch(Priority priority)
{
    initInput();
    file_in->prefetch(priority);
}


bool CachedCompressedReadBuffer::nextImpl()
{
    /// Let's check for the presence of a decompressed block in the cache, grab the ownership of this block, if it exists.
    UInt128 key = UncompressedCache::hash(path, file_pos);

    owned_cell = cache->getOrSet(key, [&]()
    {
        initInput();
        file_in->seek(file_pos, SEEK_SET);

        auto cell = std::make_shared<UncompressedCacheCell>();

        size_t size_decompressed = 0;
        size_t size_compressed_without_checksum = 0;
        cell->compressed_size = readCompressedData(size_decompressed, size_compressed_without_checksum, false);

        if (cell->compressed_size)
        {
            cell->additional_bytes = codec->getAdditionalSizeAtTheEndOfBuffer();
            cell->data.resize(size_decompressed + cell->additional_bytes);
            decompressTo(cell->data.data(), size_decompressed, size_compressed_without_checksum);
        }

        return cell;
    });

    if (owned_cell->data.size() == 0)
        return false;

    working_buffer = Buffer(owned_cell->data.data(), owned_cell->data.data() + owned_cell->data.size() - owned_cell->additional_bytes);

    /// nextimpl_working_buffer_offset is set in the seek function (lazy seek). So we have to
    /// check that we are not seeking beyond working buffer.
    if (nextimpl_working_buffer_offset > working_buffer.size())
        throw Exception(ErrorCodes::SEEK_POSITION_OUT_OF_BOUND, "Seek position is beyond the decompressed block (pos: "
        "{}, block size: {})", nextimpl_working_buffer_offset, toString(working_buffer.size()));

    file_pos += owned_cell->compressed_size;

    return true;
}

CachedCompressedReadBuffer::CachedCompressedReadBuffer(
    const std::string & path_, std::function<std::unique_ptr<ReadBufferFromFileBase>()> file_in_creator_, UncompressedCache * cache_, bool allow_different_codecs_)
    : ReadBuffer(nullptr, 0), file_in_creator(std::move(file_in_creator_)), cache(cache_), path(path_), file_pos(0)
{
    allow_different_codecs = allow_different_codecs_;
}

size_t CachedCompressedReadBuffer::getCompressedBlockEnd(size_t block_start_offset)
{
    initInput();
    char checksum_and_header[CHECKSUM_AND_HEADER_SIZE];
    /// Clear the bound before seeking: a stale bound would clamp the seek, and the header bound must be set from
    /// the block start so it is ahead of the read position (setting it behind already-read data is rejected).
    file_in->setReadUntilEnd();
    file_in->seek(block_start_offset, SEEK_SET);
    /// Bound the underlying read to the header so peeking it does not fetch the whole block.
    file_in->setReadUntilPosition(block_start_offset + CHECKSUM_AND_HEADER_SIZE);
    file_in->readStrict(checksum_and_header, CHECKSUM_AND_HEADER_SIZE);
    /// Rewind and unbound so the next data read, which bounds to an earlier position, is not left behind
    /// already-read data.
    file_in->setReadUntilEnd();
    file_in->seek(0, SEEK_SET);
    resetWorkingBuffer();
    owned_cell.reset();
    file_pos = 0;
    return computeCompressedBlockEnd(block_start_offset, checksum_and_header);
}

void CachedCompressedReadBuffer::seek(size_t offset_in_compressed_file, size_t offset_in_decompressed_block)
{
    /// Nothing to do if we already at required position
    if (!owned_cell && file_pos == offset_in_compressed_file
        && ((!buffer().empty() && offset() == offset_in_decompressed_block) ||
            nextimpl_working_buffer_offset == offset_in_decompressed_block))
        return;

    if (owned_cell &&
        offset_in_compressed_file == file_pos - owned_cell->compressed_size &&
        offset_in_decompressed_block <= working_buffer.size())
    {
        pos = working_buffer.begin() + offset_in_decompressed_block;
    }
    else
    {
        /// Remember position in compressed file (will be moved in nextImpl)
        file_pos = offset_in_compressed_file;
        /// We will discard our working_buffer, but have to account rest bytes
        bytes += offset();
        /// No data, everything discarded
        resetWorkingBuffer();
        owned_cell.reset();

        /// Remember required offset in decompressed block which will be set in
        /// the next ReadBuffer::next() call
        nextimpl_working_buffer_offset = offset_in_decompressed_block;
    }
}

}
