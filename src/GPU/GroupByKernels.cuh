#pragma once

#include <GPU/GroupByLayouts.cuh>

/** The kernels of the `GROUP BY`: one that folds a chunk's rows straight into the table, the
  * three of the two-pass path through buckets, and the ones that empty, grow and write out the
  * table. Every kernel takes the table, the chunk and the counters as they are laid out in
  * `GroupByLayouts.cuh`, and nothing else the host has.
  */
namespace DB::GPU::Grouping
{

/// Every slot's record, the spare one included, starts as the identity of its aggregate.
__global__ void initRecords(Accumulators accumulators, size_t capacity, Identities identities)
{
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t slot = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; slot <= capacity; slot += stride)
    {
        uint64_t * record = accumulators.of(slot);
        for (uint32_t i = 0; i < identities.count; ++i)
            record[i] = identities.values[i];
    }
}

/// A thread per row: the row's key finds or makes its slot, and the row's values fold into the
/// slot's record.
__global__ void aggregateRows(TableRef table, Chunk chunk, Counters counters)
{
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t i = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; i < chunk.rows; i += stride)
    {
        const size_t row = chunk.order ? chunk.order[i] : i;
        if (!keeps(chunk, row))
            continue;

        const Key key = packKey(chunk.keys, row);
        foldRow(chunk.values, row, table.accumulators.of(slotOf(table, chunk.keys, key, counters)));
    }
}

/// The first pass: each row's bucket, from the top bits of its key's hash, and its number, for the
/// sort to pair up. Rows the filter drops and rows whose key is the sentinel go to the last
/// bucket, whose block tells them apart.
__global__ void bucketRows(Chunk chunk, uint8_t * buckets, uint32_t * indices)
{
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t row = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; row < chunk.rows; row += stride)
    {
        indices[row] = static_cast<uint32_t>(row);

        if (!keeps(chunk, row))
        {
            buckets[row] = last_bucket;
            continue;
        }

        const Key key = packKey(chunk.keys, row);
        if (chunk.keys.may_equal_sentinel && key == key_sentinel)
            buckets[row] = last_bucket;
        else
            buckets[row] = static_cast<uint8_t>((key * bucket_multiplier) >> (64 - bucket_bits));
    }
}

/// The first position at or after which the sorted buckets are `bucket` or more.
__device__ inline size_t lowerBound(const uint8_t * sorted_buckets, size_t num_rows, uint32_t bucket)
{
    size_t low = 0;
    size_t high = num_rows;
    while (low < high)
    {
        const size_t middle = low + (high - low) / 2;
        if (sorted_buckets[middle] < bucket)
            low = middle + 1;
        else
            high = middle;
    }
    return low;
}

/// The buckets' side of the second pass: the sorted rows, a shared table's worth of slots per
/// bucket to write the partial groups to, and the list of rows that found no slot.
struct Buckets
{
    const uint8_t * sorted_buckets = nullptr;
    const uint32_t * order = nullptr;
    uint32_t shared_capacity = 0;
    Identities identities;
    Key * partial_keys = nullptr;
    Accumulators partials;
    uint32_t * overflow = nullptr;
    uint32_t * num_overflow = nullptr;
};

/// The second pass, a block per bucket. The rows of the bucket are grouped in a table in shared
/// memory, whose keys come first and whose records follow, and the table is then written out as
/// it is, empty slots and all, as the bucket's partial groups for `mergePartials`. A row whose key
/// finds no slot within `max_probe` goes on the overflow list, for `aggregateRows` to take one by
/// one. The last block also meets the rows the filter drops, which it drops, and the rows whose
/// key is the sentinel, which it folds straight into the table's spare slot.
__global__ void aggregateBuckets(Chunk chunk, Buckets buckets, TableRef table, Counters counters)
{
    extern __shared__ uint64_t shared[];
    __shared__ size_t range[2];

    const uint32_t bucket = blockIdx.x;
    const bool mixed = bucket == last_bucket;

    if (threadIdx.x == 0)
    {
        range[0] = lowerBound(buckets.sorted_buckets, chunk.rows, bucket);
        range[1] = mixed ? chunk.rows : lowerBound(buckets.sorted_buckets, chunk.rows, bucket + 1);
    }
    __syncthreads();

    const size_t begin = range[0];
    const size_t end = range[1];

    Key * shared_keys = reinterpret_cast<Key *>(shared);
    const Accumulators shared_records{.records = shared + buckets.shared_capacity, .num_values = chunk.values.count};

    for (uint32_t slot = threadIdx.x; slot < buckets.shared_capacity; slot += blockDim.x)
    {
        shared_keys[slot] = key_sentinel;
        uint64_t * record = shared_records.of(slot);
        for (uint32_t v = 0; v < chunk.values.count; ++v)
            record[v] = buckets.identities.values[v];
    }
    __syncthreads();

    /// The bucket took the top bits of the hash; the slot takes the bits below them.
    const uint32_t mask = buckets.shared_capacity - 1;
    const uint32_t shift = 64 - bucket_bits - static_cast<uint32_t>(__popc(mask));

    for (size_t i = begin + threadIdx.x; i < end; i += blockDim.x)
    {
        const size_t row = buckets.order[i];

        if (mixed && chunk.filters.present && !passesFilter(chunk.filter, chunk.filters, row))
            continue;

        const Key key = packKey(chunk.keys, row);

        if (mixed && chunk.keys.may_equal_sentinel && key == key_sentinel)
        {
            *counters.sentinel_seen = 1;
            foldRow(chunk.values, row, table.accumulators.of(table.capacity));
            continue;
        }

        uint32_t slot = static_cast<uint32_t>((key * bucket_multiplier) >> shift) & mask;
        uint64_t * record = nullptr;
        for (uint32_t probe = 0; probe < max_probe; ++probe, slot = (slot + 1) & mask)
        {
            const Key seen = atomicCAS(
                reinterpret_cast<unsigned long long *>(shared_keys + slot),
                static_cast<unsigned long long>(key_sentinel),
                static_cast<unsigned long long>(key));
            if (seen == key_sentinel || seen == key)
            {
                record = shared_records.of(slot);
                break;
            }
        }

        if (record)
            foldRow(chunk.values, row, record);
        else
            buckets.overflow[atomicAdd(buckets.num_overflow, 1u)] = static_cast<uint32_t>(row);
    }
    __syncthreads();

    const size_t base = static_cast<size_t>(bucket) * buckets.shared_capacity;
    for (uint32_t slot = threadIdx.x; slot < buckets.shared_capacity; slot += blockDim.x)
    {
        buckets.partial_keys[base + slot] = shared_keys[slot];
        uint64_t * out = buckets.partials.of(base + slot);
        const uint64_t * partial = shared_records.of(slot);
        for (uint32_t v = 0; v < chunk.values.count; ++v)
            out[v] = partial[v];
    }
}

/// A run of the buckets' partial groups, empty slots included.
struct Partials
{
    const Key * keys = nullptr;
    Accumulators records;
    size_t count = 0;
};

/// Folds the buckets' partial groups into the table: a partial group's record folds into its
/// group's record the way a row's values would.
__global__ void mergePartials(TableRef table, Partials partials, ValueLayouts values, Counters counters)
{
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t i = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; i < partials.count; i += stride)
    {
        const Key key = partials.keys[i];
        if (key == key_sentinel)
            continue;

        const auto [it, inserted] = table.set.insert_and_find(key);
        if (inserted)
            atomicAdd(counters.num_groups, 1u);

        uint64_t * record = table.accumulators.of(static_cast<size_t>(it - table.slots));
        const uint64_t * partial = partials.records.of(i);
        for (uint32_t v = 0; v < values.count; ++v)
            foldBits(values.columns[v].fold, partial[v], record + v);
    }
}

/// Files every occupied slot of an old table into a new, larger one and carries its accumulators
/// over. The keys are distinct, so each lands in a slot of its own and plain stores do.
__global__ void moveGroups(const Key * old_slots, size_t old_capacity, Accumulators from, TableRef to)
{
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t old_slot = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; old_slot < old_capacity; old_slot += stride)
    {
        const Key key = old_slots[old_slot];
        if (key == key_sentinel)
            continue;

        const uint64_t * old_record = from.of(old_slot);
        uint64_t * record = to.accumulators.of(static_cast<size_t>(to.set.insert_and_find(key).first - to.slots));
        for (uint32_t i = 0; i < from.num_values; ++i)
            record[i] = old_record[i];
    }
}

/// The groups to write out: the occupied slots, and whether the spare slot is a group too.
struct GroupList
{
    const uint32_t * slots = nullptr;
    size_t num_regular = 0;
    bool with_sentinel = false;
};

/// A thread per group: unpacks the slot's key into the key columns and writes the record into the
/// value columns, each in its output's width.
__global__ void writeGroups(GroupList groups, TableRef table, OutputLayouts out)
{
    const size_t num_groups = groups.num_regular + (groups.with_sentinel ? 1 : 0);
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t group = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; group < num_groups; group += stride)
    {
        const bool sentinel = group == groups.num_regular;
        const size_t slot = sentinel ? table.capacity : groups.slots[group];
        const uint64_t * record = table.accumulators.of(slot);
        const Key key = sentinel ? key_sentinel : table.slots[slot];

        for (uint32_t i = 0; i < out.num_keys; ++i)
            storeBits(out.keys[i].data, group, out.keys[i].size, key >> out.keys[i].shift);

        for (uint32_t i = 0; i < out.num_values; ++i)
        {
            const OutputLayout & value = out.values[i];
            const uint64_t bits = record[i];
            switch (value.store)
            {
                case Store::Bits:
                    reinterpret_cast<uint64_t *>(value.data)[group] = bits;
                    break;
                case Store::Truncate:
                    storeBits(value.data, group, value.size, bits);
                    break;
                case Store::Narrow:
                    reinterpret_cast<float *>(value.data)[group] = static_cast<float>(__longlong_as_double(static_cast<long long>(bits)));
                    break;
            }
        }
    }
}

struct IsOccupied
{
    __device__ bool operator()(Key key) const { return key != key_sentinel; }
};

}
