#include <Common/PartitionedRecordBuffer.h>

#include <algorithm>
#include <atomic>
#include <new>

#include <Common/Allocator.h>
#include <Common/CacheLine.h>
#include <Common/memory.h>

namespace DB
{

struct alignas(CH_CACHE_LINE_SIZE) PartitionedRecordBuffer::Block
{
    /// One reference for the carver while it carves from the block, and one per chunk carved and not yet released.
    std::atomic<UInt32> references{1};

    void release()
    {
        if (references.fetch_sub(1, std::memory_order_acq_rel) == 1)
        {
            this->~Block();
            Allocator<false, false>().free(this, block_bytes, alignof(Block));
        }
    }
};

PartitionedRecordBuffer::PartitionedRecordBuffer(size_t num_partitions_, size_t partitions_per_group_)
    : num_partitions(num_partitions_)
    , partitions_per_group(partitions_per_group_)
{
    chassert(num_partitions > 0 && partitions_per_group > 0);
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
    allocated_chunk_bytes = 0;
}

void PartitionedRecordBuffer::releaseCurrentBlocks()
{
    for (auto & carver : carvers)
    {
        if (carver.block)
            carver.block->release();
        carver = {};
    }
}

void PartitionedRecordBuffer::startChunk(size_t partition, size_t bytes)
{
    Cursor & cursor = cursors[partition];
    Chain & chain = chains[partition];
    if (chain.last)
        chain.last->used = static_cast<UInt32>(cursor.pos - chain.last->records());

    /// A chunk is sized in multiples of the header's alignment, so the chunk carved after it in the block starts
    /// aligned. Records need no alignment, and a chunk for a record larger than the doubled capacity is
    /// sized by that record.
    const size_t needed = ::Memory::alignUp(sizeof(ChunkHeader) + bytes + tail_padding_bytes, alignof(ChunkHeader));
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
        Carver & carver = carvers[partition / partitions_per_group];
        if (carver.remaining < needed)
        {
            char * memory = static_cast<char *>(Allocator<false, false>().alloc(block_bytes, alignof(Block)));
            if (carver.block)
                carver.block->release();
            carver.block = new (memory) Block;
            carver.pos = memory + sizeof(Block);
            carver.remaining = block_bytes - sizeof(Block);
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
