#include <Common/PartitionedRecordBuffer.h>

#include <algorithm>
#include <array>
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

    /// Counts a chunk carved from the block. Only the appending thread changes the count while it appends: consumers
    /// release chunks after `finishAppending`, and appends resume once they finish. So the count takes no locked
    /// instruction, which would wait for the stores of the appends to drain at every chunk.
    void addChunk() { references.store(references.load(std::memory_order_relaxed) + 1, std::memory_order_relaxed); }

    /// Gives back `count` references, and frees the block with the last one. Returns whether it freed the block.
    bool release(UInt32 count)
    {
        const UInt32 held = references.fetch_sub(count, std::memory_order_acq_rel);
        chassert(held >= count);
        if (held != count)
            return false;
        const size_t allocated = bytes;
        this->~Block();
        Allocator<false, false>().free(this, allocated, alignof(Block));
        return true;
    }
};

PartitionedRecordBuffer::PartitionedRecordBuffer(
    size_t num_partitions_, size_t partitions_per_group_, size_t first_chunk_bytes_, size_t max_chunk_bytes_)
    : num_partitions(num_partitions_)
    , partitions_per_group(partitions_per_group_)
    , first_chunk_bytes(first_chunk_bytes_)
    , max_chunk_bytes(max_chunk_bytes_)
{
    chassert(num_partitions > 0 && partitions_per_group > 0);
    chassert(first_chunk_bytes >= min_first_chunk_bytes && first_chunk_bytes <= max_first_chunk_bytes);
    chassert(max_chunk_bytes >= first_chunk_bytes);
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
    /// The carvers drop their claims first, so only chunks hold the blocks. Every chunk is either carved from a block
    /// or allocated on its own, and both count as live allocations. With none left, no partition holds a chunk: the
    /// consumers released them all, which also reset their chains and cursors. The walk over the partitions is then
    /// skipped. It matters because a merge that released every partition leaves the buffers of many producers, with
    /// thousands of partitions each, to be destroyed.
    releaseCurrentBlocks();
    if (live_allocations.load(std::memory_order_acquire))
        releasePartitions(0, num_partitions);
    chassert(!live_allocations.load(std::memory_order_relaxed));
    chassert(std::none_of(chains.get(), chains.get() + num_partitions, [](const Chain & chain) { return chain.first; }));
    for (auto & carver : carvers)
        carver.allocated_block_bytes = 0;
    allocated_chunk_bytes = 0;
}

void PartitionedRecordBuffer::releaseBlock(Block * block, UInt32 references)
{
    if (block->release(references))
        live_allocations.fetch_sub(1, std::memory_order_release);
}

void PartitionedRecordBuffer::releaseCurrentBlocks()
{
    for (auto & carver : carvers)
    {
        if (carver.block)
            releaseBlock(carver.block);
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
    /// sized by that record. A record that takes more than half of the chunk cap gets a chunk of its own size,
    /// allocated apart from the blocks (see the class comment). The header keeps the size in 32 bits, so a record
    /// whose chunk would not fit them is rejected before anything is allocated or changed.
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
    if (needed > max_chunk_bytes / 2)
    {
        capacity = needed;
        data = static_cast<char *>(Allocator<false, false>().alloc(capacity, alignof(ChunkHeader)));
        live_allocations.fetch_add(1, std::memory_order_relaxed);
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
                releaseBlock(carver.block);
            carver.block = new (memory) Block(block_size);
            live_allocations.fetch_add(1, std::memory_order_relaxed);
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
        block->addChunk();
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

void PartitionedRecordBuffer::releasePartitions(size_t first_partition, size_t count)
{
    /// The blocks of the chunks that the walk has passed, each with the number of those chunks, whose references are
    /// given back together. Other threads wrote the counters of the blocks last, and a locked decrement waits for its
    /// cache line and holds back the loads after it. So a block's counter is prefetched when the walk first meets the
    /// block and decremented once, with the rest of the batch, for all the chunks that the batch found in the block.
    /// For example, the first chunks of the 8 partitions of a bucket usually sit in one block of their group, which
    /// then takes one decrement instead of 8. A block outlives the batch that names it: a chunk further along a
    /// chain holds a reference to its block until the walk has passed it.
    struct ReleaseBatch
    {
        struct BlockChunks
        {
            Block * block;
            UInt32 chunks;
        };

        PartitionedRecordBuffer & buffer;
        std::array<BlockChunks, release_batch_blocks> blocks{};
        size_t size = 0;

        void addChunkOf(Block * block)
        {
            for (size_t i = 0; i < size; ++i)
            {
                if (blocks[i].block == block)
                {
                    ++blocks[i].chunks;
                    return;
                }
            }
            if (size == blocks.size())
                release();
            __builtin_prefetch(block, /*rw=*/1);
            blocks[size++] = {block, 1};
        }

        void release()
        {
            for (size_t i = 0; i < size; ++i)
                buffer.releaseBlock(blocks[i].block, blocks[i].chunks);
            size = 0;
        }
    };
    ReleaseBatch batch{.buffer = *this};

    /// The appending thread wrote the headers of the chunks long ago, so the walk prefetches the first chunk of the
    /// partitions ahead.
    const size_t end_partition = first_partition + count;
    for (size_t partition = first_partition; partition < std::min(end_partition, first_partition + release_prefetch_partitions); ++partition)
        __builtin_prefetch(chains[partition].first);

    for (size_t partition = first_partition; partition < end_partition; ++partition)
    {
        if (partition + release_prefetch_partitions < end_partition)
            __builtin_prefetch(chains[partition + release_prefetch_partitions].first);

        for (ChunkHeader * chunk = chains[partition].first; chunk;)
        {
            ChunkHeader * const next = chunk->next;
            if (Block * const block = chunk->block)
            {
                batch.addChunkOf(block);
            }
            else
            {
                Allocator<false, false>().free(chunk, chunk->capacity, alignof(ChunkHeader));
                live_allocations.fetch_sub(1, std::memory_order_release);
            }
            chunk = next;
        }
        chains[partition] = {};
        cursors[partition] = {};
    }
    batch.release();
}

}
