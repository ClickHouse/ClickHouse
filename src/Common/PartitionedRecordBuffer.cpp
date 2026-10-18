#include <Common/PartitionedRecordBuffer.h>

#include <algorithm>
#include <atomic>
#include <bit>
#include <limits>
#include <new>

#include <Common/Allocator.h>
#include <Common/CacheLine.h>
#include <Common/Exception.h>
#include <Common/memory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_LARGE_STRING_SIZE;
}

struct alignas(CH_CACHE_LINE_SIZE) PartitionedRecordBuffer::Block
{
    explicit Block(size_t bytes_) : bytes(bytes_) {}

    /// One reference for the carver while it carves from the block, and one per chunk carved and not yet released.
    std::atomic<UInt32> references{1};
    /// The size of the block's allocation. Blocks differ in size, since the blocks of a group grow.
    const size_t bytes;

    void release()
    {
        if (references.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            const size_t allocated = bytes;
            this->~Block();
            Allocator<false, false>().free(this, allocated, alignof(Block));
        }
    }
};

PartitionedRecordBuffer::PartitionedRecordBuffer(
    size_t num_partitions_, size_t partitions_per_group_, size_t first_chunk_bytes_)
    : num_partitions(num_partitions_)
    , partitions_per_group(partitions_per_group_)
    , first_chunk_bytes(first_chunk_bytes_)
{
    chassert(num_partitions > 0 && partitions_per_group > 0);
    chassert(first_chunk_bytes >= min_first_chunk_bytes && first_chunk_bytes <= max_first_chunk_bytes);
    cursors = std::make_unique<Cursor[]>(num_partitions);
    chains = std::make_unique<Chain[]>(num_partitions);
    carvers.resize((num_partitions - 1) / partitions_per_group + 1);
}

PartitionedRecordBuffer::~PartitionedRecordBuffer()
{
    clear();
}

void PartitionedRecordBuffer::clear()
{
    for (size_t partition = 0; partition < num_partitions; ++partition)
        releasePartition(partition);
    releaseCurrentBlocks();
    for (auto & carver : carvers)
        carver.allocated_block_bytes = 0;
    allocated_chunk_bytes = 0;
}

void PartitionedRecordBuffer::releaseCurrentBlocks()
{
    for (auto & carver : carvers)
    {
        if (carver.block)
            carver.block->release();
        carver.pos = nullptr;
        carver.remaining = 0;
        carver.block = nullptr;
    }
}

size_t PartitionedRecordBuffer::nextBlockBytes(size_t group) const
{
    if (const size_t allocated = carvers[group].allocated_block_bytes)
        return std::min(allocated, max_block_bytes);

    /// The first size is a power of two, so the totals of the group reach the cap exactly. It leaves out the
    /// block header, so it stays a size class of the allocator, and the chunk that reaches the end of the
    /// block takes the rest of it.
    const size_t partitions = std::min(partitions_per_group, num_partitions - group * partitions_per_group);
    return std::min(std::bit_ceil(partitions * first_chunk_bytes), max_block_bytes);
}

void PartitionedRecordBuffer::startChunk(size_t partition, size_t bytes)
{
    /// A chunk is sized in multiples of the header's alignment, so the chunk carved after it in the block starts
    /// aligned. Records need no alignment, and a chunk for a record larger than the doubled capacity is
    /// sized by that record. The header keeps the size in 32 bits, so a record whose chunk would not fit them is
    /// rejected before anything is allocated or changed.
    const size_t needed = ::Memory::alignUp(sizeof(ChunkHeader) + bytes + tail_padding_bytes, alignof(ChunkHeader));
    if (needed > std::numeric_limits<decltype(ChunkHeader::capacity)>::max())
        throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "A record of {} bytes does not fit a chunk of the partitioned record buffer", bytes);

    Cursor & cursor = cursors[partition];
    Chain & chain = chains[partition];
    if (chain.last)
        chain.last->used = static_cast<UInt32>(cursor.pos - chain.last->records());

    size_t capacity = 0;
    char * data = nullptr;
    Block * block = nullptr;
    if (needed > max_chunk_bytes)
    {
        capacity = needed;
        data = static_cast<char *>(Allocator<false, false>().alloc(capacity));
    }
    else
    {
        const size_t group = partition / partitions_per_group;
        Carver & carver = carvers[group];
        if (carver.remaining < needed)
        {
            /// A chunk larger than the group's next block gets a block of its own size.
            const size_t block_size = std::max(nextBlockBytes(group), sizeof(Block) + needed);
            char * memory = static_cast<char *>(Allocator<false, false>().alloc(block_size, alignof(Block)));
            if (carver.block)
                carver.block->release();
            carver.block = new (memory) Block(block_size);
            carver.pos = memory + sizeof(Block);
            carver.remaining = block_size - sizeof(Block);
            carver.allocated_block_bytes += block_size;
        }

        /// Double the previous chunk up to the cap, but take the rest of the block instead where it is less and
        /// still holds the record, so the end of a block is not left unused.
        const size_t doubled = chain.last ? std::min<size_t>(chain.last->capacity * 2, max_chunk_bytes) : first_chunk_bytes;
        capacity = std::min(std::max(doubled, needed), carver.remaining);
        data = carver.pos;
        carver.pos += capacity;
        carver.remaining -= capacity;
        block = carver.block;
        block->references.fetch_add(1, std::memory_order_relaxed);
    }

    auto * chunk = new (data) ChunkHeader{.next = nullptr, .block = block, .used = 0, .capacity = static_cast<UInt32>(capacity)};
    if (chain.last)
    {
        chain.last->next = chunk;
        chain.closed_chunk_records += cursor.records;
    }
    else
        chain.first = chunk;
    chain.last = chunk;
    allocated_chunk_bytes += capacity;

    cursor.pos = chunk->records();
    cursor.remaining = static_cast<UInt32>(capacity - sizeof(ChunkHeader) - tail_padding_bytes);
    cursor.records = 0;
}

void PartitionedRecordBuffer::finishAppending()
{
    for (size_t partition = 0; partition < num_partitions; ++partition)
        if (ChunkHeader * last = chains[partition].last)
            last->used = static_cast<UInt32>(cursors[partition].pos - last->records());
    releaseCurrentBlocks();
}

void PartitionedRecordBuffer::releasePartition(size_t partition)
{
    ChunkHeader * chunk = chains[partition].first;
    while (chunk)
    {
        ChunkHeader * next = chunk->next;
        if (chunk->block)
            chunk->block->release();
        else
            Allocator<false, false>().free(chunk, chunk->capacity);
        chunk = next;
    }
    chains[partition] = {};
    cursors[partition] = {};
}

}
