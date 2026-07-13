#include <Coordination/Storage/SortedFile.h>

#include <Coordination/Storage/BlockCache.h>
#include <Coordination/Storage/Memtable.h>
#include <Coordination/Storage/Node.h>
#include <Coordination/Storage/StorageState.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperContext.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Disks/IDisk.h>
#include <IO/CompressionMethod.h>
#include <IO/ReadBufferFromMemory.h>
#include <base/defines.h>

#include <algorithm>

#include <atomic>

namespace DB::ErrorCodes
{
    extern const int CORRUPTED_DATA;
}

namespace DB::CoordinationSetting
{
    extern const CoordinationSettingsUInt64 file_block_size;
}

namespace Coordination::Storage
{

SortedFile::~SortedFile()
{
    if (delete_when_destroyed && file_deleter)
        file_deleter->enqueueFileToRemove(std::move(file_path));
}

BlockPtr SortedFile::getOrLoadBlock(uint32_t block_idx, BlockCache * block_cache) const
{
    chassert(block_idx < blocks.size());
    const BlockInfo & info = blocks[block_idx];

    BlockPtr block = info.data.load();
    if (block)
        return block;

    chassert(block_cache); // in memory-only mode load() above succeeds because all blocks are pinned

    BlockCacheKey key{.file_id = file_id, .block_idx = block_idx};
    block = block_cache->get(key);
    if (!block)
    {
        /// Find where the block group starts.
        uint32_t group_start_block_idx = block_idx;
        while (blocks[group_start_block_idx].offset_in_group != 0)
        {
            chassert(group_start_block_idx != 0);
            --group_start_block_idx;
        }

        block = block_cache->getBlockOrLoadGroup(
            key, group_start_block_idx,
            [&] { return loadBlockGroup(group_start_block_idx); });
    }

    /// Note: we update blocks[i].data only for the one requested block, even if we loaded multiple
    /// blocks (loadBlockGroup above). This is intentional. We want the next access to those
    /// incidentally-loaded blocks to go through BlockCache to report usage to the eviction policy
    /// (to move to SLRU protected list).
    chassert(block);
    info.data.store(block);

    return block;
}

BlockPtr SortedFile::getBlockCoveringPath(NodePath path, BlockCache * block_cache) const
{
    /// The last block with min_path <= path; if path falls past its max_path (a gap between blocks,
    /// or past the last block) or before the first block, the path is not in this file.
    auto block_it = std::ranges::partition_point(
        blocks,
        [&](const BlockInfo & block) { return block.min_path.compare(path) <= 0; });
    if (block_it == blocks.begin())
        return {};
    --block_it;
    if (path.compare(block_it->max_path) > 0)
        return {};
    return getOrLoadBlock(static_cast<uint32_t>(block_it - blocks.begin()), block_cache);
}

void SortedFile::listChildrenNames(
    NodePath range_start, NodePath range_end, UInt128 parent_path_hash, ChildrenSet2 & out, DB::Arena & arena_, BlockCache * block_cache) const
{
    if (parent_paths_filter && !parent_paths_filter->findHashPair(
            DB::BloomFilterHashPair {parent_path_hash.items[0], parent_path_hash.items[1]}))
        return;

    auto block_it = std::ranges::partition_point(
        blocks,
        [&](const BlockInfo & block) { return block.max_path.compare(range_start) <= 0; });

    std::string path_buf;
    for (; block_it != blocks.end(); ++block_it)
    {
        if (block_it->min_path.compare(range_end) >= 0)
            break;

        const uint32_t block_idx = static_cast<uint32_t>(block_it - blocks.begin());
        BlockPtr block = getOrLoadBlock(block_idx, block_cache);

        NodeRef ref{.block = block};
        NodePath node_path;
        uint32_t serialized_size = 0;
        NodeAction action = NodeAction::Remove;
        for (uint32_t offset = block->entries_start; offset < block->size;)
        {
            ref.offset = offset;
            ref.readPath(node_path, path_buf, serialized_size, action);
            offset += serialized_size;

            if (node_path.compare(range_start) <= 0)
                continue; /// before the range (range_start is exclusive)
            if (node_path.compare(range_end) >= 0)
                return; /// past the range (range_end is exclusive)

            out.insert(node_path.baseName(), action, arena_);
        }
    }
}

void SortedFile::removeFromBlockCache(BlockCache * block_cache) const
{
    if (!block_cache)
        /// Memory-only mode: blocks are pinned, not in any cache.
        return;
    for (uint32_t block_idx = 0; block_idx < blocks.size(); ++block_idx)
        block_cache->remove(BlockCacheKey{.file_id = file_id, .block_idx = block_idx});
}

uint32_t SortedFile::generateFileId()
{
    static std::atomic<uint32_t> next_file_id{1};
    return next_file_id.fetch_add(1, std::memory_order_relaxed);
}

void SortedFile::prepareReadBuffer(StorageState * storage)
{
    /// Open the file for reading. All block loads are positioned reads (readBigAt) on this
    /// buffer; they may run in parallel.
    read_buffer = storage->disk->readFile(file_path, storage->read_settings, /*read_hint*/ {});
    if (!read_buffer->supportsReadAt())
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Keeper data disk '{}' doesn't support positioned reads for file {} (but the check on startup passed)",
            storage->disk->getName(), file_path);
}

std::vector<BlockPtr> SortedFile::loadBlockGroup(uint32_t start_block_idx) const
{
    const auto & group_info = blocks[start_block_idx];

    DB::PODArray<char> compressed_memory(group_info.group_compressed_size);
    size_t bytes_read = read_buffer->readBigAt(compressed_memory.data(), compressed_memory.size(), group_info.group_offset_in_file, /*progress_callback=*/ nullptr);
    if (bytes_read != compressed_memory.size())
        throw DB::Exception(DB::ErrorCodes::CORRUPTED_DATA,
            "Unexpected end of file {} while reading block group at offset {}: expected {} bytes, got {}",
            file_path, group_info.group_offset_in_file, compressed_memory.size(), bytes_read);

    std::unique_ptr<DB::ReadBuffer> compressed_reader = std::make_unique<DB::ReadBufferFromMemory>(compressed_memory.data(), compressed_memory.size());
    std::unique_ptr<DB::ReadBuffer> reader = DB::wrapReadBufferWithCompressionMethod(
        std::move(compressed_reader), DB::CompressionMethod::Zstd);

    std::vector<BlockPtr> res;
    for (uint32_t block_idx = start_block_idx;
         block_idx < blocks.size() && blocks[block_idx].group_offset_in_file == group_info.group_offset_in_file;
         ++block_idx)
    {
        const auto & info = blocks[block_idx];
        BlockPtr block = BlockData::create(info.block_size);
        block->size = info.block_size;
        block->serialization_version = serialization_version;
        block->compatible_digest = digest_version == DB::KEEPER_CURRENT_DIGEST_VERSION;

        reader->readStrict(block->data(), block->size);

        block->parseHeader();

        res.push_back(std::move(block));
    }

    return res;
}

void FileDeleteQueue::enqueueFileToRemove(std::string path)
{
    std::lock_guard lock(mutex);
    paths.push_back(std::move(path));
}

}
