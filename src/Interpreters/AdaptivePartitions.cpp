#include <Interpreters/AdaptivePartitions.h>

#include <algorithm>
#include <bit>
#include <new>

#include <Common/Allocator.h>

namespace DB
{

AdaptivePartitionLayout AdaptivePartitionLayout::forProducers(size_t producers, size_t max_bytes_before_external_group_by)
{
    constexpr size_t max_sub_bits = 4;

    const size_t threads = std::max<size_t>(producers, 1);
    size_t sub_bits = std::min<size_t>((std::bit_width(threads - 1) + 1) / 2, max_sub_bits);
    if (max_bytes_before_external_group_by)
    {
        const size_t streams = max_bytes_before_external_group_by / 4 / AdaptivePartitionBuffers::first_chunk_bytes;
        const size_t affordable_per_bucket = std::max<size_t>(streams / threads / ADAPTIVE_AGGREGATION_NUM_BUCKETS, 1);
        sub_bits = std::min<size_t>(sub_bits, std::countr_zero(std::bit_floor(affordable_per_bucket)));
    }
    return {.sub_bits = static_cast<UInt8>(sub_bits)};
}

struct alignas(64) AdaptivePartitionBuffers::Block
{
    /// One reference for the carver while it carves from the block, and one per chunk carved and not yet released.
    std::atomic<UInt32> references{1};
};

namespace
{

constexpr size_t block_header_bytes = sizeof(AdaptivePartitionBuffers::Block);

void releaseBlockReference(AdaptivePartitionBuffers::Block * block)
{
    if (block->references.fetch_sub(1, std::memory_order_acq_rel) != 1)
        return;
    block->~Block();
    Allocator<false, false>().free(block, AdaptivePartitionBuffers::block_bytes);
}

}

AdaptivePartitionBuffers::AdaptivePartitionBuffers(AdaptivePartitionLayout layout_)
    : partition_layout(layout_)
    , cursors(std::make_unique<Cursor[]>(layout_.numPartitions()))
    , chains(std::make_unique<Chain[]>(layout_.numPartitions()))
    , record_counts(std::make_unique<UInt32[]>(layout_.numPartitions()))
{
}

AdaptivePartitionBuffers::~AdaptivePartitionBuffers()
{
    for (size_t partition = 0; partition < partition_layout.numPartitions(); ++partition)
        releasePartition(partition);
    for (auto & carver : carvers)
        if (carver.block)
            releaseBlockReference(carver.block);
}

char * AdaptivePartitionBuffers::startChunk(size_t partition, size_t bytes)
{
    Cursor & cursor = cursors[partition];
    Chain & chain = chains[partition];
    if (chain.last)
        chain.last->used = static_cast<UInt32>(cursor.pos - chain.last->records());

    const size_t needed = sizeof(ChunkHeader) + bytes + tail_padding_bytes;
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
        Carver & carver = carvers[(partition >> partition_layout.sub_bits) / buckets_per_group];
        if (static_cast<size_t>(carver.end - carver.pos) < needed)
        {
            char * memory = static_cast<char *>(Allocator<false, false>().alloc(block_bytes));
            if (carver.block)
                releaseBlockReference(carver.block);
            carver.block = new (memory) Block;
            carver.pos = memory + block_header_bytes;
            carver.end = memory + block_bytes;
        }

        /// Double the previous chunk up to the cap, but take the rest of the block instead where it is less and
        /// still holds the record, so the end of a block is not left unused.
        const size_t doubled = chain.last ? std::min<size_t>(chain.last->capacity * 2, max_chunk_bytes) : first_chunk_bytes;
        capacity = std::min(std::max(doubled, needed), static_cast<size_t>(carver.end - carver.pos));
        data = carver.pos;
        carver.pos += capacity;
        block = carver.block;
        block->references.fetch_add(1, std::memory_order_relaxed);
    }

    auto * chunk = new (data) ChunkHeader{.next = nullptr, .block = block, .used = 0, .capacity = static_cast<UInt32>(capacity)};
    if (chain.last)
        chain.last->next = chunk;
    else
        chain.first = chunk;
    chain.last = chunk;
    held_bytes += capacity;

    char * records = chunk->records();
    cursor = {records + bytes, data + capacity - tail_padding_bytes};
    return records;
}

void AdaptivePartitionBuffers::finishAppending()
{
    for (size_t partition = 0; partition < partition_layout.numPartitions(); ++partition)
        if (ChunkHeader * last = chains[partition].last)
            last->used = static_cast<UInt32>(cursors[partition].pos - last->records());
    for (auto & carver : carvers)
    {
        if (carver.block)
            releaseBlockReference(carver.block);
        carver = {};
    }
}

void AdaptivePartitionBuffers::releasePartition(size_t partition)
{
    ChunkHeader * chunk = chains[partition].first;
    while (chunk)
    {
        ChunkHeader * next = chunk->next;
        if (chunk->block)
            releaseBlockReference(chunk->block);
        else
            Allocator<false, false>().free(chunk, chunk->capacity);
        chunk = next;
    }
    chains[partition] = {};
    record_counts[partition] = 0;
    cursors[partition] = {};
}

}
