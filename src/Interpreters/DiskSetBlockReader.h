#pragma once

#include <Interpreters/DiskSetImpl.h>

#include <Compression/CompressedReadBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/SeekableReadBuffer.h>
#include <Common/ProfileEvents.h>

#include <algorithm>
#include <optional>
#include <span>
#include <vector>

namespace ProfileEvents
{
extern const Event ExternalSetReadBlocks;
extern const Event ExternalSetCacheHits;
}

namespace DB
{

/// Reads the blocks that one lookup call needs: it takes the blocks that the cache of the set holds from the
/// cache, and reads the others from the file through its own buffer. Each lookup call creates its own reader,
/// so concurrent calls share only the immutable file and directory, and the cache, which the reader calls
/// once to get the blocks of the call and once to add the blocks that it read.
///
/// The constructor plans the call, since its decisions need all the blocks of the call: it queries the cache
/// for all of them in one batch, and sizes the reads by how many blocks the cache lacks and how far apart
/// they are. It reads no blocks. `read` then produces the keys of one block at a time, in ascending order, as
/// the lookups reach the block. The decoder holds the keys of one block, so at most one decoded block exists
/// at a time.
template <typename Key>
class DiskSetImpl<Key>::BlockReader
{
public:
    /// Prepares to read `blocks_`, which ascend. The reader opens the file only if the cache lacks some of
    /// the blocks, and sizes its reads for the missing ones.
    BlockReader(const DiskSetImpl & set_, std::vector<size_t> blocks_)
        : set(set_)
        , blocks(std::move(blocks_))
    {
        /// Step 1: Query the cache for all the blocks of the call at once, so that the call locks each shard
        /// once. The cache returns the bytes of the blocks that it holds and tells which of the other blocks
        /// it will take once they are read.
        if (set.block_cache)
        {
            auto call_blocks = set.block_cache->getMany(blocks);
            cached_blocks = std::move(call_blocks.cached);
            admitted_blocks = std::move(call_blocks.admitted);
        }
        else
        {
            cached_blocks.resize(blocks.size()); /// All null: the reader reads every block
            admitted_blocks.resize(blocks.size()); /// All false: no cache takes the blocks
        }

        /// Step 2: Find the blocks that the cache lacks, which the reader reads from the file. Their number
        /// and span decide whether one read takes the frames of neighboring blocks together, as
        /// `getBufferSize` describes.
        size_t missing_blocks = 0;
        std::optional<size_t> first_missing_block;
        size_t last_missing_block = 0;
        for (size_t i = 0; i < blocks.size(); ++i)
        {
            if (cached_blocks[i])
                continue;
            ++missing_blocks;
            if (!first_missing_block)
                first_missing_block = blocks[i];
            last_missing_block = blocks[i];
        }
        cache_hits = blocks.size() - missing_blocks;

        /// Step 3: Open the file only if some blocks are missing, so a call whose blocks are all cached never
        /// touches the file.
        if (missing_blocks)
        {
            in = set.file->readRaw(getBufferSize(set, missing_blocks, last_missing_block - *first_missing_block + 1));
            compressed.emplace(*in, /*allow_different_codecs_=*/true);
        }
    }

    ~BlockReader()
    {
        ProfileEvents::increment(ProfileEvents::ExternalSetReadBlocks, read_blocks);
        ProfileEvents::increment(ProfileEvents::ExternalSetCacheHits, cache_hits);
    }

    /// Returns the keys of `block`, which is one of the blocks of the reader. The keys stay valid until the
    /// next call. Calls come in ascending order of blocks, so that a coalesced read also serves the blocks
    /// after the one that it starts at. Calling again for the block of the previous call returns its keys
    /// without another read.
    std::span<const Key> read(size_t block)
    {
        /// Consecutive lookups often need the same block, which is decoded only once.
        if (block == current_block)
            return current_keys;

        /// Calls come in ascending order of blocks, so the search starts at the block of the previous call.
        position = static_cast<size_t>(std::find(blocks.begin() + position, blocks.end(), block) - blocks.begin());
        chassert(position < blocks.size()); /// `block` is one of the blocks of the reader
        current_block = block;

        /// A cached block is decoded from its cached bytes, without reading the file.
        if (const auto & cached_block = cached_blocks[position])
        {
            ReadBufferFromMemory cached_bytes(cached_block->bytes.data(), cached_block->bytes.size()); /// The decompressed frame
            const DiskSetBlockHeader cached_header = BlockFormat::Decoder::readHeader(cached_bytes);
            current_keys = decoder.decode(cached_header, cached_bytes, set.directory.getFirstKey(block));
            return current_keys;
        }

        /// A missing block is read from the file. The decoder has consumed the block that the reader last
        /// read from the file, so the compressed buffer takes the next frame from the new position, which the
        /// file buffer may already hold.
        in->seek(set.directory.getOffset(block), SEEK_SET);
        compressed->next();
        const DiskSetBlockHeader header = BlockFormat::Decoder::readHeader(*compressed);

        /// The working buffer of the compressed buffer now holds the decompressed bytes of the block. If the
        /// cache admitted the block, the reader keeps a copy for `addReadBlocksToCache`.
        if (admitted_blocks[position])
        {
            auto read_block = std::make_shared<DiskSetCachedBlock>();
            read_block->bytes.assign(compressed->buffer().begin(), compressed->buffer().end()); /// Allocates the exact size of the frame
            blocks_to_cache.push_back(block);
            read_blocks_to_cache.push_back(std::move(read_block));
        }
        ++read_blocks;
        current_keys = decoder.decode(header, *compressed, set.directory.getFirstKey(block));
        return current_keys;
    }

    /// Adds to the cache of the set the blocks that the reader read from the file and the cache admitted, so
    /// that later calls do not read them again.
    void addReadBlocksToCache()
    {
        if (!blocks_to_cache.empty()) /// Only a set with a cache admits blocks
            set.block_cache->setMany(blocks_to_cache, read_blocks_to_cache);
    }

private:
    /// A coalesced read takes the frames of this many neighboring blocks. It saves reads when the needed
    /// blocks are on average closer than this, and it costs more than it saves when they are farther apart,
    /// because it copies all the frames between them.
    static constexpr size_t frames_per_coalesced_read = 16;

    /// Returns the size of the buffer: the largest frame of a block, with the bytes that its codec needs
    /// after it, or the frames of a coalesced read when the needed blocks are close enough.
    static size_t getBufferSize(const DiskSetImpl & set, size_t needed_blocks, size_t range_blocks)
    {
        const size_t max_frame_bytes = std::max(
            CompressedReadBufferBase::getInPlaceInputBufferSize(*set.compressible_block_codec, BlockFormat::max_block_bytes_with_header),
            CompressedReadBufferBase::getInPlaceInputBufferSize(*set.incompressible_block_codec, BlockFormat::max_block_bytes_with_header));
        const bool coalesce_reads = range_blocks <= needed_blocks * frames_per_coalesced_read;
        return (coalesce_reads ? frames_per_coalesced_read : 1) * max_frame_bytes;
    }

    /// The set whose file, directory and cache the reader uses. The set outlives the reader, which serves a
    /// single call.
    const DiskSetImpl & set;

    /// The blocks of the call, with the cached block of each, or null for a block that the reader reads from
    /// the file, and whether the cache admits a block that the reader reads. The reader holds the cached
    /// blocks, so the cache may evict them before the reader decodes them. `position` is the index of the
    /// block that `read` returned last, where `read` starts looking for the next one.
    std::vector<size_t> blocks;
    std::vector<DiskSetBlockCache::BlockPtr> cached_blocks;
    std::vector<bool> admitted_blocks;
    size_t position = 0;

    /// The buffer holds whole frames, so that the frame of a block is decompressed where it is read instead
    /// of being copied first, except a frame that the end of a coalesced read cuts.
    std::unique_ptr<SeekableReadBuffer> in; /// Only when the cache lacks some of the blocks

    /// One compressed buffer serves all the blocks of the call, so that it allocates its memory once. It
    /// accepts frames of both codecs, because the file compresses only the blocks that a codec can shrink.
    std::optional<CompressedReadBuffer> compressed; /// Created with `in`

    typename BlockFormat::Decoder decoder;

    /// The reader remembers the block that it returned last and its keys, so that the probes of one block
    /// share one read.
    std::optional<size_t> current_block;
    std::span<const Key> current_keys;

    /// The blocks that the reader read from the file and the cache admitted, with their bytes, which the
    /// reader adds to the cache at the end of the call.
    std::vector<size_t> blocks_to_cache;
    std::vector<DiskSetBlockCache::BlockPtr> read_blocks_to_cache;

    /// The reader reports the blocks that it read and the blocks that it took from the cache once, at the end
    /// of the call: all lookup threads increment the same counters of profile events, so an increment for
    /// each block would be a noticeable part of reading it.
    size_t read_blocks = 0;
    size_t cache_hits = 0;
};

}
