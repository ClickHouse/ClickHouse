#pragma once

#include <Interpreters/DiskSetImpl.h>

#include <IO/ReadBufferFromMemory.h>
#include <Interpreters/TemporaryFileFrameReader.h>
#include <Common/CacheLine.h>
#include <Common/ProfileEvents.h>

#include <algorithm>
#include <functional>
#include <optional>
#include <span>
#include <string_view>
#include <vector>

namespace ProfileEvents
{
extern const Event ExternalSetReadBlocks;
extern const Event ExternalSetCacheHits;
}

namespace DB
{

/// Reads the blocks that one lookup call needs: it takes the blocks that the cache of the set holds from the
/// cache, and reads the others from the file. Each lookup call creates its own reader, so concurrent calls
/// share only the immutable file and directory, and the cache, which the reader calls once to get the blocks
/// of the call and in batches to add the blocks that it read.
///
/// The constructor queries the cache for all the blocks of the call in one batch, and gives the frames of the
/// blocks that the cache lacks to a `TemporaryFileFrameReader`, which reads the frames that are close to each
/// other together. It reads no blocks. `read` then produces the keys of one block at a time, in ascending
/// order, as the lookups reach the block. The decoder holds the keys of one block, so at most one decoded
/// block exists at a time.
template <typename Key>
class DiskSetImpl<Key>::BlockReader
{
public:
    /// Prepares to read `blocks_`, which are distinct and ascend. The reader reads the file only if the cache
    /// lacks some of the blocks.
    BlockReader(const DiskSetImpl & set_, std::vector<size_t> blocks_)
        : set(set_)
        , blocks(std::move(blocks_))
    {
        chassert(std::ranges::adjacent_find(blocks, std::greater_equal<>()) == blocks.end());

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

        /// Step 2: Prepare to read the frames of the blocks that the cache lacks. A call whose blocks are all
        /// cached never reads the file.
        const size_t missing_blocks = static_cast<size_t>(std::ranges::count(cached_blocks, nullptr));
        cache_hits = blocks.size() - missing_blocks;
        if (missing_blocks)
        {
            std::vector<TemporaryFileFrameReader::Frame> missing_frames;
            missing_frames.reserve(missing_blocks);
            for (size_t i = 0; i < blocks.size(); ++i)
                if (!cached_blocks[i])
                    missing_frames.push_back({set.directory.getOffset(blocks[i]), set.directory.getEndOffset(blocks[i])});
            frame_reader.emplace(*set.shared_file, std::move(missing_frames));
        }
    }

    ~BlockReader()
    {
        ProfileEvents::increment(ProfileEvents::ExternalSetReadBlocks, read_blocks);
        ProfileEvents::increment(ProfileEvents::ExternalSetCacheHits, cache_hits);
    }

    /// Returns the keys of `block`, which is one of the blocks of the reader. The keys stay valid until the
    /// next call. Calls come in ascending order of blocks, so that one read of the file can serve several
    /// missing blocks (see `TemporaryFileFrameReader`), and the next cached block can load while the lookups
    /// search the current one (see `prefetchNextCachedBlock`). Calling again for the block of the previous
    /// call returns its keys without another read.
    std::span<const Key> read(size_t block)
    {
        /// Consecutive lookups often need the same block, which is decoded only once.
        if (block == current_block)
            return current_keys;

        /// Calls come in ascending order of blocks, so the search starts at the block of the previous call.
        position = static_cast<size_t>(std::find(blocks.begin() + position, blocks.end(), block) - blocks.begin());
        chassert(position < blocks.size()); /// `block` is one of the blocks of the reader
        current_block = block;

        /// A cached block is decoded from its header and body, without reading the file. The reader releases
        /// the block right after decoding it (see `cached_blocks`).
        if (auto & cached_block = cached_blocks[position])
        {
            ReadBufferFromMemory body(cached_block->body.data(), cached_block->body.size());
            current_keys = decoder.decode(cached_block->header, body, set.directory.getFirstKey(block));
            cached_block.reset();
            prefetchNextCachedBlock();
            return current_keys;
        }

        /// A missing block is read from the file.
        const std::string_view block_bytes = frame_reader->read(set.directory.getOffset(block));
        ReadBufferFromMemory block_in(block_bytes.data(), block_bytes.size());
        const DiskSetBlockHeader header = BlockFormat::Decoder::readHeader(block_in);

        /// After `readHeader`, the rest of the block is its body. If the cache admitted the block, the reader
        /// keeps the header and a copy of the body for `addReadBlocksToCache`.
        if (admitted_blocks[position])
        {
            chassert(block_in.available() == header.body_bytes); /// A frame holds one block
            auto read_block = std::make_shared<DiskSetCachedBlock>();
            read_block->header = header;
            read_block->body.assign(block_in.position(), block_in.position() + header.body_bytes); /// Allocates the exact size
            blocks_to_cache.push_back(block);
            read_blocks_to_cache.push_back(std::move(read_block));
            if (blocks_to_cache.size() == blocks_per_cache_batch)
                addReadBlocksToCache();
        }
        ++read_blocks;
        current_keys = decoder.decode(header, block_in, set.directory.getFirstKey(block));
        prefetchNextCachedBlock();
        return current_keys;
    }

    /// Adds to the cache of the set the blocks that the reader read from the file and the cache admitted
    /// since the last batch, so that later calls do not read them again.
    void addReadBlocksToCache()
    {
        if (blocks_to_cache.empty()) /// None since the last batch, as always for a set without a cache
            return;
        set.block_cache->setMany(blocks_to_cache, read_blocks_to_cache);
        blocks_to_cache.clear();
        read_blocks_to_cache.clear();
    }

private:
    /// Starts loading the body of the block after the one at `position` into the caches of the CPU, if the
    /// cache of the set holds that block.
    void prefetchNextCachedBlock() const
    {
        if (position + 1 == blocks.size())
            return;
        if (const auto & next_block = cached_blocks[position + 1])
            for (size_t offset = 0; offset < next_block->body.size(); offset += CH_CACHE_LINE_SIZE)
                __builtin_prefetch(next_block->body.data() + offset);
    }

    /// The set whose file, directory and cache the reader uses. The set outlives the reader, which serves a
    /// single call.
    const DiskSetImpl & set;

    /// The blocks of the call, with the cached block of each, or null for a block that the reader reads from
    /// the file, and whether the cache admits a block that the reader reads. `position` is the index of the
    /// block that `read` returned last, where `read` starts looking for the next one.
    ///
    /// The reader holds each cached block until it decodes the block, so the cache may evict the block before
    /// then. It releases the block right after decoding it.
    std::vector<size_t> blocks;
    std::vector<DiskSetBlockCache::BlockPtr> cached_blocks;
    std::vector<bool> admitted_blocks;
    size_t position = 0;

    std::optional<TemporaryFileFrameReader> frame_reader; /// Only when the cache lacks some of the blocks

    typename BlockFormat::Decoder decoder;

    /// The reader remembers the block that it returned last and its keys, so that the probes of one block
    /// share one read.
    std::optional<size_t> current_block;
    std::span<const Key> current_keys;

    /// The blocks that the reader read from the file and the cache admitted, with their bytes, which the
    /// reader adds to the cache in batches of `blocks_per_cache_batch` and at the end of the call. Until
    /// then, their bytes are outside the cache and its budget. A call of random lookups can read tens of
    /// thousands of blocks, so adding them all at the end of the call would keep many times the budget
    /// outside the cache across concurrent calls. A batch of 1024 blocks takes about 4 MiB, and adding it
    /// locks each of the 64 shards of the default cache once for about 16 of its blocks.
    static constexpr size_t blocks_per_cache_batch = 1024;
    std::vector<size_t> blocks_to_cache;
    std::vector<DiskSetBlockCache::BlockPtr> read_blocks_to_cache;

    /// The reader reports the blocks that it read and the blocks that it took from the cache once, at the end
    /// of the call: all lookup threads increment the same counters of profile events, so an increment for
    /// each block would be a noticeable part of reading it.
    size_t read_blocks = 0;
    size_t cache_hits = 0;
};

}
