#pragma once

#include <atomic>
#include <memory>
#include <string_view>
#include <vector>

#include <base/defines.h>
#include <base/types.h>

namespace DB
{

/// The staged records route by the two-level bucket of their key's hash, and their partitions nest in those
/// buckets, so the staging and the merge come in the same 256 buckets as the two-level hash tables.
inline constexpr size_t ADAPTIVE_AGGREGATION_NUM_BUCKETS = 256;

/// How the staged records of a session are partitioned. A partition is a sub-range of a two-level bucket:
/// `partition = (bucket << sub_bits) | sub`, where `bucket` is hash bits 24..31, as for the two-level tables, and
/// `sub` the next `sub_bits` bits below them. The bits are free: the 32-bit hashes of the two-level methods use
/// 24..31 for the bucket and the low bits for the slot inside a bucket's table, which keeps a partition's table
/// well spread up to 2^20 cells. Since the partitions nest in the buckets, every bucket-level contract of the
/// merge holds unchanged.
struct AdaptivePartitionLayout
{
    /// The partitions of a bucket follow the number of producers, which is also the number of merge tasks. The merge
    /// tasks share the last-level cache, so the more of them there are, the more units a large bucket needs for each
    /// unit's table to stay in a task's share; every partition, though, costs the producers' appends and the merge's
    /// walk a fixed overhead per chunk, so a small bucket wants few. About the square root of the producers, half the
    /// bits of their count rounded up, balanced the two over the high-cardinality aggregations of ClickBench at 16 and
    /// 64 threads, with 4 and 8 partitions per bucket. With an external-aggregation threshold the first chunks of all
    /// streams also stay within a quarter of it, so the staging floor cannot hold the query over the threshold by
    /// itself. At most 16 partitions per bucket, the four free hash bits.
    static AdaptivePartitionLayout forProducers(size_t producers, size_t max_bytes_before_external_group_by);

    UInt8 sub_bits = 0;

    size_t partitionsPerBucket() const { return size_t{1} << sub_bits; }
    size_t numPartitions() const { return ADAPTIVE_AGGREGATION_NUM_BUCKETS << sub_bits; }
    size_t partitionOf(UInt64 hash) const { return (hash >> (24 - sub_bits)) & (numPartitions() - 1); }
};

/// The staged records of one producer: one chain of chunks per partition, appended to by the producer's thread
/// alone. Once the producer finished appending, the merge tasks read and release the partitions of the buckets
/// they own, distinct partitions concurrently.
///
/// A partition's chunks double in size from a small first chunk up to a cap: most partitions of a producer stay
/// small, and the first chunks of all the partitions of all the producers are the floor of the staged memory,
/// while doubling keeps the unused end of a large partition's last chunk proportional to its records.
///
/// The chunks are carved from blocks rather than allocated one by one: chunks past the allocator's small size
/// classes would each take its arena path, whose locks and extent bookkeeping cost more than the records the chunk
/// holds, while a block is one allocation for many chunks. Each producer carves the chunks of every group of 16
/// consecutive buckets from blocks of that group only, and a block is freed once all its chunks are released, so the
/// memory of a group comes back as soon as the merge, which takes the buckets in ascending order, has passed it.
class AdaptivePartitionBuffers
{
public:
    static constexpr size_t first_chunk_bytes = 2 << 10;
    static constexpr size_t max_chunk_bytes = 32 << 10;
    static constexpr size_t block_bytes = 256 << 10;
    static constexpr size_t buckets_per_group = 16;
    static constexpr size_t num_groups = ADAPTIVE_AGGREGATION_NUM_BUCKETS / buckets_per_group;
    /// The end of every chunk no record occupies: records hold keys that the merge emplaces, compares and copies in
    /// place, with primitives that may read up to 15 bytes past a key and copy primitives that may write as far.
    static constexpr size_t tail_padding_bytes = 64;

    struct Block;
    struct ChunkHeader;

    explicit AdaptivePartitionBuffers(AdaptivePartitionLayout layout_);
    ~AdaptivePartitionBuffers();

    AdaptivePartitionBuffers(const AdaptivePartitionBuffers &) = delete;
    AdaptivePartitionBuffers & operator=(const AdaptivePartitionBuffers &) = delete;

    const AdaptivePartitionLayout & layout() const { return partition_layout; }

    /// Reserves `bytes` at the end of the partition's records and returns where they go. A record never straddles
    /// two chunks, and `tail_padding_bytes` of the chunk follow it.
    ALWAYS_INLINE char * append(size_t partition, size_t bytes)
    {
        Cursor & cursor = cursors[partition];
        if (static_cast<size_t>(cursor.end - cursor.pos) < bytes) [[unlikely]]
            return startChunk(partition, bytes);
        char * at = cursor.pos;
        cursor.pos += bytes;
        return at;
    }

    void countRecords(size_t partition, size_t records) { record_counts[partition] += records; }

    /// The two stages of prefetching an append of a later record: its partition's cursor first, and once that is in
    /// the cache, the place the record goes. An append to a random one of thousands of partitions otherwise stalls
    /// on both.
    ALWAYS_INLINE void prefetchCursor(size_t partition) const { __builtin_prefetch(&cursors[partition]); }
    ALWAYS_INLINE void prefetchAppend(size_t partition) const { __builtin_prefetch(cursors[partition].pos, 1); }

    /// Records the fill of every partition's last chunk and ends the claims on the blocks being carved, so a block
    /// is freed with its last chunk. Called when the producer stops appending, and before a spill reads the chunks;
    /// appends may resume after a spill, into new chunks and blocks.
    void finishAppending();

    /// Calls `callback(std::string_view records)` for each chunk of the partition, in append order.
    template <typename Callback>
    void forEachChunk(size_t partition, Callback && callback) const;

    bool hasRecords(size_t partition) const { return chains[partition].first != nullptr; }
    UInt64 recordsOf(size_t partition) const { return record_counts[partition]; }

    /// Releases the partition's chunks; its records are gone afterwards. The merge tasks release distinct
    /// partitions concurrently.
    void releasePartition(size_t partition);

    /// The bytes of the chunks the producer took since it was created or last spilled, for its spill decision;
    /// maintained on the producer's thread alone.
    size_t heldBytes() const { return held_bytes; }

    /// Called by the producer after a spill released all its partitions.
    void resetHeldBytes() { held_bytes = 0; }

private:
    /// The hot header of a partition: the append position in its current chunk and that chunk's end.
    struct Cursor
    {
        char * pos = nullptr;
        char * end = nullptr;
    };

    /// The cold side of a partition: its chunks, linked through their headers.
    struct Chain
    {
        ChunkHeader * first = nullptr;
        ChunkHeader * last = nullptr;
    };

    /// A producer's position in the block it carves the chunks of one bucket group from.
    struct Carver
    {
        char * pos = nullptr;
        char * end = nullptr;
        Block * block = nullptr;
    };

    /// The cold path of `append`: closes the partition's current chunk and starts one that holds `bytes`.
    NO_INLINE char * startChunk(size_t partition, size_t bytes);

    const AdaptivePartitionLayout partition_layout;
    std::unique_ptr<Cursor[]> cursors;
    std::unique_ptr<Chain[]> chains;
    std::unique_ptr<UInt32[]> record_counts;
    Carver carvers[num_groups];
    size_t held_bytes = 0;
};

/// The head of every chunk: the next chunk of the partition, the block it was carved from (null for a chunk
/// allocated on its own), and how many of its bytes hold records. The records follow it.
struct AdaptivePartitionBuffers::ChunkHeader
{
    ChunkHeader * next;
    Block * block;
    UInt32 used;
    UInt32 capacity;

    char * records() { return reinterpret_cast<char *>(this) + sizeof(ChunkHeader); }
    const char * records() const { return reinterpret_cast<const char *>(this) + sizeof(ChunkHeader); }
};

template <typename Callback>
void AdaptivePartitionBuffers::forEachChunk(size_t partition, Callback && callback) const
{
    for (const ChunkHeader * chunk = chains[partition].first; chunk; chunk = chunk->next)
        callback(std::string_view(chunk->records(), chunk->used));
}

using AdaptivePartitionBuffersPtr = std::unique_ptr<AdaptivePartitionBuffers>;

}
