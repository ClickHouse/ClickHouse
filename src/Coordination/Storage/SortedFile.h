#pragma once

#include <Coordination/Storage/Common.h>
#include <Coordination/KeeperCommon.h>
#include <Common/Arena.h>

#include <vector>

namespace Coordination::Storage
{

/// Immutable file containing sequence of nodes/tombstones sorted by NodePath (depth and path).
/// Paths don't repeat. File always contains at least 1 block.
struct SortedFile
{
    struct BlockInfo
    {
        NodePath min_path;
        /// (Should we store max_path like this, or should we use the next block's min_path as upper
        ///  bound for this block's paths? Unclear. Omitting max_path saves memory, but including
        ///  it speeds up lookup if the searched path falls in the gap between blocks (which might
        ///  matter if we choose block boundaries carefully to maximize such gaps; e.g. put the
        ///  boundary where consecutive nodes have the shortest common prefix, within some range of
        ///  allowed block sizes).)
        NodePath max_path;

        /// If in block cache or in pinned_blocks.
        mutable BlockWeakPtrWithSpinlock data;
    };

    String file_path;

    uint32_t serialization_version = 0;
    /// Forward compatibility: the file can be read by readers this old and newer.
    /// E.g. we can add optional fields under Node's varints_size without breaking old readers.
    uint32_t min_compatible_version = 0;
    DB::KeeperDigestVersion digest_version = DB::KeeperDigestVersion::NO_DIGEST;

    size_t total_block_size = 0;
    size_t file_size = 0;

    /// Number of Create-d nodes minus number of Remove-d nodes.
    int64_t node_count_delta = 0;

    /// Unique only within a process, changes on restart.
    uint32_t file_id = generateFileId();

    DB::Arena arena; // for path strings used in `blocks`

    /// BlockInfo::data is the only mutable part of this struct after construction (everything
    /// else must not be mutated after the SortedFile is published to readers).
    std::vector<BlockInfo> blocks;

    /// If we're in memory-only mode, files are not written to disk. Blocks don't go to BlockCache
    /// and are instead owned by this array to always stay in memory.
    std::vector<BlockPtr> pinned_blocks;

    /// TODO: In file, blocks would be grouped, each group compressed (zstd?) and read together.
    ///       Because we probably want smaller blocks for BlockData's delta compression than for
    ///       file IO, at least on S3.
    ///       Another std::vector here would list block groups and their offsets in file.
    ///       When reading a group of blocks, put them all in BlockCache; if already in cache, leave
    ///       it and point `data` to the old copy, to avoid invalidating BlockWeakRef-s in
    ///       NodeRefCache unnecessarily.

    /// TODO: Bloom filter of nodes with at least one child.
    /// TODO: Consider storing children index.

    /// Gets from cache or reads from file. Thread safe.
    BlockPtr getOrLoadBlock(uint32_t block_idx, BlockCache * block_cache) const;

    /// See SortedRun for explanation of these methods.
    BlockPtr getBlockCoveringPath(NodePath path, BlockCache * block_cache) const;
    bool visitChildren(
        NodePath range_start, NodePath range_end, bool full_node,
        const std::function<bool(std::string_view /*name*/, const NodeRef &, const FullNode *)> & check_node,
        ChildrenSet2 & seen, DB::Arena & arena_, BlockCache * block_cache) const;

    /// Hint to the block cache that this file's blocks are no longer needed, e.g. the file was
    /// removed from the visible set and is pending deletion from disk.
    /// (This is not fully reliable as some reader thread may still hold a SortedFilePtr to this
    ///  file, and it may re-load the block into cache right after we remove it here. That's ok and
    ///  should be rare.)
    void removeFromBlockCache(BlockCache * block_cache) const;

private:
    static uint32_t generateFileId();

    BlockPtr loadBlock(uint32_t block_idx) const;
};
using SortedFilePtr = std::shared_ptr<SortedFile>;

}
