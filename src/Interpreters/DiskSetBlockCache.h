#pragma once

#include <Interpreters/DiskSetBlockFormat.h>
#include <Common/PODArray.h>
#include <base/unit.h>

#include <atomic>
#include <memory>
#include <vector>

namespace DB
{

/// A block of a set on disk in the layout of `DiskSetBlockFormat`, not yet decoded into keys: its header, and
/// its body in an allocation of its own. The body of a full block takes exactly 4 KiB, a size class of the
/// allocator, which the block with its header would exceed. Lookups decode the body just as they decode a
/// block read from the file. The cache keeps blocks in this form rather than as keys for two reasons: a
/// decoded block of packed gaps takes many times more memory, 8 times for 64-bit keys, and lookups search
/// keys faster in the buffer of their decoder, which stays in the CPU caches, than in memory written by other
/// threads. A lookup holds a block until it decodes it, so evicting the block from the cache does not free
/// its body.
struct DiskSetCachedBlock
{
    DiskSetBlockHeader header;
    PODArray<char> body;
};

/// The cache of the blocks of a set on disk, keyed by block number and shared by the lookups of all threads.
/// It evicts blocks by the `SLRU` policy: a block that lookups use a second time moves to the protected part
/// of the cache, so blocks that lookups need once do not evict blocks that they keep needing.
///
/// Even a hit changes the order of eviction, so every lookup in the cache takes a lock. A lookup call needs
/// up to thousands of blocks, and with a single lock, the calls of all threads would wait for each other. So
/// the cache is split into shards by block number, each with its share of the budget and its own lock, and a
/// call takes the lock of each shard once. A small budget gets fewer shards, so each shard still holds many
/// blocks. Once the cache is full, it takes only some of the blocks that lookups read, as
/// `admission_period_when_full` describes.
class DiskSetBlockCache
{
public:
    using BlockPtr = std::shared_ptr<DiskSetCachedBlock>;

    explicit DiskSetBlockCache(size_t max_bytes);
    ~DiskSetBlockCache();

    /// What the cache holds for the blocks of a lookup call, and which of the missing blocks it takes. Both
    /// vectors follow the order of the blocks of the call.
    struct CallBlocks
    {
        /// The cached block, or null for a block that the cache lacks, which the call reads from the file.
        std::vector<BlockPtr> cached;

        /// Whether the cache takes the block after the call reads it, which the call passes to `setMany`. The
        /// cache takes only blocks that it lacks: all of them while it has room, and one in
        /// `admission_period_when_full` once it is full.
        std::vector<bool> admitted;
    };

    CallBlocks getMany(const std::vector<size_t> & blocks);

    /// Adds `read_blocks`, the bytes of `blocks`, which `getMany` admitted.
    void setMany(const std::vector<size_t> & blocks, const std::vector<BlockPtr> & read_blocks);

private:
    /// Weighs a cached block by its body plus what the cache keeps for it.
    struct Weight;

    /// A shard of the cache, which marks the cache full when it evicts its first block.
    class Shard;

    /// Once the cache is full, each block that it takes evicts another. When lookups spread over a set much
    /// larger than the cache, most blocks leave the cache before lookups need them again, and inserting and
    /// evicting such a block costs almost half as much as reading it, for nothing. So a full cache takes only
    /// one in this many of the blocks that lookups read, which cuts that cost to about 5% of the reads. The
    /// cache still serves the same share of such lookups, the share of the set that it holds, and a block
    /// that lookups keep needing still enters it after about this many reads.
    static constexpr size_t admission_period_when_full = 8;

    /// With as many shards as a machine typically has lookup threads, the threads rarely wait for each other.
    static constexpr size_t max_shard_count = 64;

    /// Each shard gets at least this much, which is hundreds of blocks. A shard of only a few blocks would
    /// evict most blocks before lookups need them again.
    static constexpr size_t min_bytes_per_shard = 1_MiB;

    /// Calls `function` for each shard that some of `blocks` belong to, with those blocks and their positions
    /// in `blocks`. Neighboring blocks go to different shards, so a call spreads its blocks over the shards.
    template <typename Function>
    void forEachShard(const std::vector<size_t> & blocks, Function && function);

    /// Set when a shard evicts its first block. The shards take blocks evenly, so all are nearly full then.
    std::atomic<bool> full = false;

    std::vector<std::unique_ptr<Shard>> shards;
};

}
