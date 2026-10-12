#pragma once

#include <memory>
#include <string_view>
#include <vector>

#include <base/defines.h>
#include <base/types.h>

namespace DB
{

/// Owns variable-length records in one chain of chunks per partition. One thread appends records; after
/// `finishAppending`, consumers may read and release distinct partitions concurrently. The caller chooses
/// the record format and partition for each append.
///
/// A partition's chunks double in size from a small first chunk up to a cap: most partitions of a producer stay
/// small, and the first chunks of all partitions form the minimum allocation for their records,
/// while doubling keeps the unused end of a large partition's last chunk proportional to its records.
///
/// The chunks are carved from blocks rather than allocated one by one: chunks past the allocator's small size
/// classes would each take its arena path, whose locks and extent bookkeeping cost more than the records the chunk
/// holds, while a block is one allocation for many chunks. Consecutive partitions share blocks in groups of
/// `partitions_per_group`, chosen by the caller to match the consumption order. A block is freed once all its
/// chunks are released, allowing consumers to reclaim memory without waiting for unrelated groups.
class PartitionedRecordBuffer
{
public:
    static constexpr size_t first_chunk_bytes = 2 << 10;
    /// Padding after the last record permits bounded reads and writes past a record
    /// by vectorized comparison and copy routines.
    static constexpr size_t tail_padding_bytes = 64;

    /// Both counts must be positive. A final allocation group may contain fewer partitions.
    PartitionedRecordBuffer(size_t num_partitions_, size_t partitions_per_group_);
    ~PartitionedRecordBuffer();

    PartitionedRecordBuffer(const PartitionedRecordBuffer &) = delete;
    PartitionedRecordBuffer & operator=(const PartitionedRecordBuffer &) = delete;

    /// Appends one record of positive `bytes` and returns its storage. A record never straddles two chunks, and
    /// `tail_padding_bytes` of the chunk follow it.
    ALWAYS_INLINE char * append(size_t partition, size_t bytes)
    {
        chassert(bytes > 0);
        Cursor & cursor = cursors[partition];
        if (cursor.remaining < bytes) [[unlikely]]
            startChunk(partition, bytes);
        char * at = cursor.pos;
        cursor.pos += bytes;
        cursor.remaining -= bytes;
        ++cursor.records;
        return at;
    }

    /// Prefetches the storage of a later record, whose append can target any partition.
    ALWAYS_INLINE void prefetchAppend(size_t partition) const { __builtin_prefetch(cursors[partition].pos, 1); }

    /// Records the fill of every partition's last chunk and ends the claims on the blocks being carved, so a block
    /// is freed with its last chunk. Appends may resume after consumers have finished; released partitions
    /// start new chunks, while retained partitions continue from their current positions.
    void finishAppending();

    /// Calls `callback(std::string_view records)` for each chunk of the partition, in append order.
    template <typename Callback>
    void forEachChunk(size_t partition, Callback && callback) const;

    bool hasRecords(size_t partition) const { return chains[partition].first != nullptr; }
    UInt64 recordsOf(size_t partition) const { return chains[partition].closed_chunk_records + cursors[partition].records; }

    /// Releases the partition's chunks and resets its cursor. Consumers may release distinct partitions
    /// concurrently, after `finishAppending`.
    void releasePartition(size_t partition);

    /// The total capacity of chunks allocated since construction or `clear`. This counts chunk headers and
    /// padding, excludes unused block capacity, and is not reduced by concurrent partition releases.
    size_t allocatedChunkBytes() const { return allocated_chunk_bytes; }

    /// Releases all records and blocks and resets allocation accounting. Requires exclusive access.
    void clear();

private:
    static constexpr size_t max_chunk_bytes = 32 << 10;
    static constexpr size_t block_bytes = 256 << 10;

    struct Block;
    struct ChunkHeader;

    /// The fields updated by every append share a compact header. Each chunk's capacity and record count
    /// fit in 32 bits, so they fit beside the append pointer. The chain holds the count of earlier chunks.
    struct Cursor
    {
        char * pos = nullptr;
        UInt32 remaining = 0;
        UInt32 records = 0;
    };

    /// The cold side of a partition: its chunks, linked through their headers.
    struct Chain
    {
        ChunkHeader * first = nullptr;
        ChunkHeader * last = nullptr;
        UInt64 closed_chunk_records = 0;
    };

    /// The position and remaining capacity in one allocation group's current block.
    struct Carver
    {
        char * pos = nullptr;
        size_t remaining = 0;
        Block * block = nullptr;
    };

    /// The cold path of `append`: closes the partition's current chunk and starts one that holds `bytes`.
    NO_INLINE void startChunk(size_t partition, size_t bytes);

    void releaseCurrentBlocks();

    const size_t num_partitions;
    const size_t partitions_per_group;
    std::unique_ptr<Cursor[]> cursors;
    std::unique_ptr<Chain[]> chains;
    std::vector<Carver> carvers;
    size_t allocated_chunk_bytes = 0;
};

/// The head of every chunk: the next chunk of the partition, the block it was carved from (null for a chunk
/// allocated on its own), and how many of its bytes hold records. The records follow it.
struct PartitionedRecordBuffer::ChunkHeader
{
    ChunkHeader * next;
    Block * block;
    UInt32 used;
    UInt32 capacity;

    char * records() { return reinterpret_cast<char *>(this) + sizeof(ChunkHeader); }
    const char * records() const { return reinterpret_cast<const char *>(this) + sizeof(ChunkHeader); }
};

template <typename Callback>
void PartitionedRecordBuffer::forEachChunk(size_t partition, Callback && callback) const
{
    for (const ChunkHeader * chunk = chains[partition].first; chunk; chunk = chunk->next)
        callback(std::string_view(chunk->records(), chunk->used));
}

}
