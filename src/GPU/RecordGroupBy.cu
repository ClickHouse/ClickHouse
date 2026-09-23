#include <GPU/RecordGroupBy.cuh>

#include <GPU/Cudf.h>

#include <cuco/static_set.cuh>

#include <cub/device/device_radix_sort.cuh>

#include <rmm/device_scalar.hpp>
#include <rmm/device_uvector.hpp>
#include <rmm/exec_policy.hpp>
#include <rmm/mr/polymorphic_allocator.hpp>

#include <thrust/copy.h>
#include <thrust/iterator/counting_iterator.h>

#include <cuda/std/functional>

#include <algorithm>
#include <bit>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <vector>

namespace DB::GPU
{

namespace
{

using Key = uint64_t;

/// Marks an empty slot of the set. A packed key shorter than eight bytes never equals it.
constexpr Key key_sentinel = std::numeric_limits<Key>::max();

using Set = cuco::static_set<
    Key,
    cuco::extent<size_t>,
    cuda::thread_scope_device,
    cuda::std::equal_to<Key>,
    cuco::linear_probing<1, cuco::xxhash_64<Key>>,
    rmm::mr::polymorphic_allocator<char>,
    cuco::storage<1>>;

using InsertRef = decltype(std::declval<const Set &>().ref(cuco::op::insert_and_find));

constexpr double max_load = 0.6;

constexpr size_t min_capacity = 1UL << 19;

constexpr size_t max_capacity = size_t{std::numeric_limits<uint32_t>::max()} - 1;

constexpr size_t min_chunk_rows = 1UL << 18;

constexpr unsigned threads_per_block = 256;
constexpr size_t max_blocks = 32768;

constexpr uint32_t bucket_bits = 8;
constexpr uint32_t num_buckets = 1u << bucket_bits;
constexpr uint8_t last_bucket = num_buckets - 1;
constexpr int bucket_end_bit = bucket_bits;
constexpr unsigned bucket_threads = 512;
constexpr size_t shared_table_bytes = 48UL * 1024;
constexpr uint32_t max_probe = 32;
constexpr uint64_t bucket_multiplier = 0x9E3779B97F4A7C15ULL;

constexpr size_t min_partitioned_rows = size_t{num_buckets} * 4096;

constexpr size_t measurement_rows = 2UL << 20;

constexpr double min_direct_cost_for_buckets = 2.0;

struct Accumulators
{
    uint64_t * records = nullptr;
    uint32_t num_values = 0;

    __host__ __device__ __forceinline__ uint64_t * of(size_t slot) const
    {
        return records + slot * num_values;
    }
};

struct KeyLayout
{
    const char * data = nullptr;
    uint32_t size = 0;
    uint32_t shift = 0;
};

struct KeyLayouts
{
    KeyLayout columns[max_group_by_keys];
    uint32_t count = 0;
    /// The packed key fills all eight bytes, so a row's key can equal `key_sentinel`.
    bool may_equal_sentinel = false;
};

enum class Fold : int
{
    SumInt,
    SumFloat,
    MinSigned,
    MaxSigned,
    MinUnsigned,
    MaxUnsigned,
    MinFloat,
    MaxFloat,
};

struct ValueLayout
{
    const char * data = nullptr;
    GPUElementType type = GPUElementType::UInt8;
    Fold fold = Fold::SumInt;
};

struct ValueLayouts
{
    ValueLayout columns[max_group_by_values];
    uint32_t count = 0;
};

struct FilterColumnLayout
{
    const char * data = nullptr;
    GPUElementType type = GPUElementType::UInt8;
};

/// The columns a `WHERE` reads, and whether there is one at all.
struct FilterLayouts
{
    FilterColumnLayout columns[max_filter_columns];
    uint32_t count = 0;
    bool present = false;
};

/// A value in a register of the predicate: an integer with or without a sign, a double, or a boolean.
struct FilterValue
{
    GPUFilterValueKind kind;
    uint64_t bits;
};

/// What each accumulator of a record holds before any row is folded into it.
struct Identities
{
    uint64_t values[max_group_by_values];
    uint32_t count = 0;
};

enum class Store : int
{
    /// All eight bytes as they are.
    Bits,
    /// The low `size` bytes of the integer.
    Truncate,
    /// A double narrowed to a float.
    Narrow,
};

struct OutputLayout
{
    char * data = nullptr;
    uint32_t size = 0;
    uint32_t shift = 0;
    Store store = Store::Bits;
};

struct OutputLayouts
{
    OutputLayout keys[max_group_by_keys];
    uint32_t num_keys = 0;
    OutputLayout values[max_group_by_values];
    uint32_t num_values = 0;
};

__device__ __forceinline__ uint64_t loadBits(const char * data, size_t row, uint32_t size)
{
    switch (size)
    {
        case 1: return reinterpret_cast<const uint8_t *>(data)[row];
        case 2: return reinterpret_cast<const uint16_t *>(data)[row];
        case 4: return reinterpret_cast<const uint32_t *>(data)[row];
        default: return reinterpret_cast<const uint64_t *>(data)[row];
    }
}

/// An integer widened to 64 bits with its sign, or without one when it has none.
__device__ __forceinline__ int64_t loadInteger(const char * data, size_t row, GPUElementType type)
{
    switch (type)
    {
        case GPUElementType::Int8: return reinterpret_cast<const int8_t *>(data)[row];
        case GPUElementType::Int16: return reinterpret_cast<const int16_t *>(data)[row];
        case GPUElementType::Int32: return reinterpret_cast<const int32_t *>(data)[row];
        case GPUElementType::Int64: return reinterpret_cast<const int64_t *>(data)[row];
        default: return static_cast<int64_t>(loadBits(data, row, sizeOf(type)));
    }
}

__device__ __forceinline__ double loadFloat(const char * data, size_t row, GPUElementType type)
{
    if (type == GPUElementType::Float32)
        return reinterpret_cast<const float *>(data)[row];
    return reinterpret_cast<const double *>(data)[row];
}

__device__ __forceinline__ void storeBits(char * data, size_t row, uint32_t size, uint64_t bits)
{
    switch (size)
    {
        case 1: reinterpret_cast<uint8_t *>(data)[row] = static_cast<uint8_t>(bits); break;
        case 2: reinterpret_cast<uint16_t *>(data)[row] = static_cast<uint16_t>(bits); break;
        case 4: reinterpret_cast<uint32_t *>(data)[row] = static_cast<uint32_t>(bits); break;
        default: reinterpret_cast<uint64_t *>(data)[row] = bits; break;
    }
}

__device__ __forceinline__ void atomicMinDouble(uint64_t * accumulator, double value)
{
    if (value >= 0)
        atomicMin(reinterpret_cast<long long *>(accumulator), __double_as_longlong(value));
    else
        atomicMax(reinterpret_cast<unsigned long long *>(accumulator), static_cast<unsigned long long>(__double_as_longlong(value)));
}

__device__ __forceinline__ void atomicMaxDouble(uint64_t * accumulator, double value)
{
    if (value >= 0)
        atomicMax(reinterpret_cast<long long *>(accumulator), __double_as_longlong(value));
    else
        atomicMin(reinterpret_cast<unsigned long long *>(accumulator), static_cast<unsigned long long>(__double_as_longlong(value)));
}

__device__ __forceinline__ uint64_t loadValueBits(const ValueLayout & value, size_t row)
{
    switch (value.fold)
    {
        case Fold::SumInt:
        case Fold::MinSigned:
        case Fold::MaxSigned:
            return static_cast<uint64_t>(loadInteger(value.data, row, value.type));
        case Fold::SumFloat:
        case Fold::MinFloat:
        case Fold::MaxFloat:
            return static_cast<uint64_t>(__double_as_longlong(loadFloat(value.data, row, value.type)));
        case Fold::MinUnsigned:
        case Fold::MaxUnsigned:
            return loadBits(value.data, row, sizeOf(value.type));
    }
    return 0;
}

__device__ __forceinline__ void foldBits(Fold fold, uint64_t bits, uint64_t * accumulator)
{
    switch (fold)
    {
        case Fold::SumInt:
            atomicAdd(reinterpret_cast<unsigned long long *>(accumulator), static_cast<unsigned long long>(bits));
            break;
        case Fold::SumFloat:
            atomicAdd(reinterpret_cast<double *>(accumulator), __longlong_as_double(static_cast<long long>(bits)));
            break;
        case Fold::MinSigned:
            atomicMin(reinterpret_cast<long long *>(accumulator), static_cast<long long>(bits));
            break;
        case Fold::MaxSigned:
            atomicMax(reinterpret_cast<long long *>(accumulator), static_cast<long long>(bits));
            break;
        case Fold::MinUnsigned:
            atomicMin(reinterpret_cast<unsigned long long *>(accumulator), static_cast<unsigned long long>(bits));
            break;
        case Fold::MaxUnsigned:
            atomicMax(reinterpret_cast<unsigned long long *>(accumulator), static_cast<unsigned long long>(bits));
            break;
        case Fold::MinFloat:
            atomicMinDouble(accumulator, __longlong_as_double(static_cast<long long>(bits)));
            break;
        case Fold::MaxFloat:
            atomicMaxDouble(accumulator, __longlong_as_double(static_cast<long long>(bits)));
            break;
    }
}

__device__ __forceinline__ void fold(const ValueLayout & value, size_t row, uint64_t * accumulator)
{
    foldBits(value.fold, loadValueBits(value, row), accumulator);
}

__device__ __forceinline__ FilterValue loadFilterValue(const FilterColumnLayout & column, size_t row)
{
    switch (column.type)
    {
        case GPUElementType::Int8:
        case GPUElementType::Int16:
        case GPUElementType::Int32:
        case GPUElementType::Int64:
            return {GPUFilterValueKind::Signed, static_cast<uint64_t>(loadInteger(column.data, row, column.type))};
        case GPUElementType::Float32:
        case GPUElementType::Float64:
            return {GPUFilterValueKind::Float, static_cast<uint64_t>(__double_as_longlong(loadFloat(column.data, row, column.type)))};
        default:
            return {GPUFilterValueKind::Unsigned, loadBits(column.data, row, sizeOf(column.type))};
    }
}

__device__ __forceinline__ int compareFilterValues(const FilterValue & a, const FilterValue & b)
{
    if (a.kind == GPUFilterValueKind::Float || b.kind == GPUFilterValueKind::Float)
    {
        const double x = __longlong_as_double(static_cast<long long>(a.bits));
        const double y = __longlong_as_double(static_cast<long long>(b.bits));
        if (x < y)
            return -1;
        if (x > y)
            return 1;
        if (x == y)
            return 0;
        return 2;
    }

    const bool a_negative = a.kind == GPUFilterValueKind::Signed && static_cast<int64_t>(a.bits) < 0;
    const bool b_negative = b.kind == GPUFilterValueKind::Signed && static_cast<int64_t>(b.bits) < 0;
    if (a_negative != b_negative)
        return a_negative ? -1 : 1;
    if (a_negative)
    {
        const int64_t x = static_cast<int64_t>(a.bits);
        const int64_t y = static_cast<int64_t>(b.bits);
        return x < y ? -1 : (x > y ? 1 : 0);
    }
    return a.bits < b.bits ? -1 : (a.bits > b.bits ? 1 : 0);
}

__device__ __forceinline__ bool isTrue(const FilterValue & value)
{
    if (value.kind == GPUFilterValueKind::Float)
        return __longlong_as_double(static_cast<long long>(value.bits)) != 0.0;
    return value.bits != 0;
}

/// Whether the row passes the `WHERE`.
__device__ __forceinline__ bool passesFilter(const GPUFilterProgram & program, const FilterLayouts & filters, size_t row)
{
    FilterValue registers[max_filter_registers];

    for (uint32_t pc = 0; pc < program.length; ++pc)
    {
        const GPUFilterInstruction & instruction = program.code[pc];
        FilterValue & result = registers[instruction.result];
        switch (instruction.op)
        {
            case GPUFilterOp::LoadColumn:
                result = loadFilterValue(filters.columns[instruction.first], row);
                break;
            case GPUFilterOp::LoadConstant:
                result = {program.constants[instruction.first].kind, program.constants[instruction.first].bits};
                break;
            case GPUFilterOp::Move:
                result = registers[instruction.first];
                break;
            case GPUFilterOp::Equals:
            case GPUFilterOp::NotEquals:
            case GPUFilterOp::Less:
            case GPUFilterOp::LessOrEquals:
            case GPUFilterOp::Greater:
            case GPUFilterOp::GreaterOrEquals:
            {
                const int order = compareFilterValues(registers[instruction.first], registers[instruction.second]);
                bool holds = false;
                switch (instruction.op)
                {
                    case GPUFilterOp::Equals: holds = order == 0; break;
                    case GPUFilterOp::NotEquals: holds = order != 0; break;
                    case GPUFilterOp::Less: holds = order == -1; break;
                    case GPUFilterOp::LessOrEquals: holds = order == -1 || order == 0; break;
                    case GPUFilterOp::Greater: holds = order == 1; break;
                    default: holds = order == 1 || order == 0; break;
                }
                result = {GPUFilterValueKind::Unsigned, holds ? 1ULL : 0ULL};
                break;
            }
            case GPUFilterOp::And:
                result = {GPUFilterValueKind::Unsigned, (isTrue(registers[instruction.first]) && isTrue(registers[instruction.second])) ? 1ULL : 0ULL};
                break;
            case GPUFilterOp::Or:
                result = {GPUFilterValueKind::Unsigned, (isTrue(registers[instruction.first]) || isTrue(registers[instruction.second])) ? 1ULL : 0ULL};
                break;
            case GPUFilterOp::Not:
                result = {GPUFilterValueKind::Unsigned, isTrue(registers[instruction.first]) ? 0ULL : 1ULL};
                break;
        }
    }

    return isTrue(registers[program.result]);
}

__device__ __forceinline__ Key packKey(const KeyLayouts & keys, size_t row)
{
    Key key = 0;
    for (uint32_t i = 0; i < keys.count; ++i)
        key |= loadBits(keys.columns[i].data, row, keys.columns[i].size) << keys.columns[i].shift;
    return key;
}

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

__global__ void aggregateRows(
    InsertRef set,
    const Key * slots,
    size_t capacity,
    Accumulators accumulators,
    KeyLayouts keys,
    ValueLayouts values,
    FilterLayouts filters,
    GPUFilterProgram filter,
    const uint32_t * order,
    size_t num_rows,
    uint32_t * num_groups,
    uint32_t * sentinel_seen)
{
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t i = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; i < num_rows; i += stride)
    {
        /// Rows that come in an order have passed the filter already.
        const size_t row = order ? order[i] : i;
        if (!order && filters.present && !passesFilter(filter, filters, row))
            continue;

        const Key key = packKey(keys, row);

        size_t slot;
        if (keys.may_equal_sentinel && key == key_sentinel)
        {
            slot = capacity;
            *sentinel_seen = 1;
        }
        else
        {
            const auto [it, inserted] = set.insert_and_find(key);
            slot = static_cast<size_t>(it - slots);
            if (inserted)
                atomicAdd(num_groups, 1u);
        }

        uint64_t * record = accumulators.of(slot);
        for (uint32_t i = 0; i < values.count; ++i)
            fold(values.columns[i], row, record + i);
    }
}

__global__ void bucketRows(
    KeyLayouts keys, FilterLayouts filters, GPUFilterProgram filter, size_t num_rows, uint8_t * buckets, uint32_t * indices)
{
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t row = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; row < num_rows; row += stride)
    {
        indices[row] = static_cast<uint32_t>(row);

        if (filters.present && !passesFilter(filter, filters, row))
        {
            buckets[row] = last_bucket;
            continue;
        }

        const Key key = packKey(keys, row);
        if (keys.may_equal_sentinel && key == key_sentinel)
            buckets[row] = last_bucket;
        else
            buckets[row] = static_cast<uint8_t>((key * bucket_multiplier) >> (64 - bucket_bits));
    }
}

__device__ size_t lowerBound(const uint8_t * sorted_buckets, size_t num_rows, uint32_t bucket)
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

/// A block per bucket. The rows of the bucket are grouped in a table in shared memory, whose keys
/// come first and whose records follow, and the table is then written out as it is, empty slots
/// and all, as the bucket's partial groups for `mergePartials`. A row whose key finds no slot
/// within `max_probe` goes on the overflow list, for `aggregateRows` to take one by one. The last
/// block also meets the rows the filter drops, which it drops, and the rows whose key is the
/// sentinel, which it folds straight into the table's spare slot.
__global__ void aggregateBuckets(
    const uint8_t * sorted_buckets,
    const uint32_t * order,
    size_t num_rows,
    uint32_t shared_capacity,
    KeyLayouts keys,
    ValueLayouts values,
    FilterLayouts filters,
    GPUFilterProgram filter,
    Identities identities,
    Key * partial_keys,
    Accumulators partials,
    uint32_t * overflow,
    uint32_t * num_overflow,
    Accumulators accumulators,
    size_t capacity,
    uint32_t * sentinel_seen)
{
    extern __shared__ uint64_t shared[];
    __shared__ size_t range[2];

    const uint32_t bucket = blockIdx.x;

    if (threadIdx.x == 0)
    {
        range[0] = lowerBound(sorted_buckets, num_rows, bucket);
        range[1] = bucket == last_bucket ? num_rows : lowerBound(sorted_buckets, num_rows, bucket + 1);
    }
    __syncthreads();

    const size_t begin = range[0];
    const size_t end = range[1];
    const bool mixed = bucket == last_bucket;

    Key * shared_keys = reinterpret_cast<Key *>(shared);
    Accumulators shared_accumulators{.records = shared + shared_capacity, .num_values = values.count};

    for (uint32_t slot = threadIdx.x; slot < shared_capacity; slot += blockDim.x)
    {
        shared_keys[slot] = key_sentinel;
        uint64_t * record = shared_accumulators.of(slot);
        for (uint32_t v = 0; v < values.count; ++v)
            record[v] = identities.values[v];
    }
    __syncthreads();

    const uint32_t mask = shared_capacity - 1;
    const uint32_t shift = 64 - bucket_bits - static_cast<uint32_t>(__popc(mask));

    for (size_t i = begin + threadIdx.x; i < end; i += blockDim.x)
    {
        const size_t row = order[i];

        if (mixed && filters.present && !passesFilter(filter, filters, row))
            continue;

        const Key key = packKey(keys, row);

        if (mixed && keys.may_equal_sentinel && key == key_sentinel)
        {
            *sentinel_seen = 1;
            uint64_t * spare = accumulators.of(capacity);
            for (uint32_t v = 0; v < values.count; ++v)
                fold(values.columns[v], row, spare + v);
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
                record = shared_accumulators.of(slot);
                break;
            }
        }

        if (!record)
        {
            overflow[atomicAdd(num_overflow, 1u)] = static_cast<uint32_t>(row);
            continue;
        }

        for (uint32_t v = 0; v < values.count; ++v)
            fold(values.columns[v], row, record + v);
    }
    __syncthreads();

    const size_t base = static_cast<size_t>(bucket) * shared_capacity;
    for (uint32_t slot = threadIdx.x; slot < shared_capacity; slot += blockDim.x)
    {
        partial_keys[base + slot] = shared_keys[slot];
        uint64_t * out = partials.of(base + slot);
        const uint64_t * partial = shared_accumulators.of(slot);
        for (uint32_t v = 0; v < values.count; ++v)
            out[v] = partial[v];
    }
}

/// Folds the buckets' partial groups into the table.
__global__ void mergePartials(
    InsertRef set,
    const Key * slots,
    Accumulators accumulators,
    const Key * partial_keys,
    Accumulators partials,
    size_t num_partials,
    ValueLayouts values,
    uint32_t * num_groups)
{
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t i = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; i < num_partials; i += stride)
    {
        const Key key = partial_keys[i];
        if (key == key_sentinel)
            continue;

        const auto [it, inserted] = set.insert_and_find(key);
        if (inserted)
            atomicAdd(num_groups, 1u);

        uint64_t * record = accumulators.of(static_cast<size_t>(it - slots));
        const uint64_t * partial = partials.of(i);
        for (uint32_t v = 0; v < values.count; ++v)
            foldBits(values.columns[v].fold, partial[v], record + v);
    }
}

/// Files every occupied slot of an old table into a new, larger one and carries its accumulators
/// over. The keys are distinct, so each lands in a slot of its own and plain stores do.
__global__ void moveGroups(
    const Key * old_slots, size_t old_capacity, Accumulators from, InsertRef set, const Key * slots, Accumulators to)
{
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t old_slot = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; old_slot < old_capacity; old_slot += stride)
    {
        const Key key = old_slots[old_slot];
        if (key == key_sentinel)
            continue;

        const uint64_t * old_record = from.of(old_slot);
        uint64_t * record = to.of(static_cast<size_t>(set.insert_and_find(key).first - slots));
        for (uint32_t i = 0; i < from.num_values; ++i)
            record[i] = old_record[i];
    }
}

__global__ void writeGroups(
    const uint32_t * group_slots, size_t num_regular, bool with_sentinel, const Key * slots, size_t capacity, Accumulators accumulators, OutputLayouts out)
{
    const size_t num_groups = num_regular + (with_sentinel ? 1 : 0);
    const size_t stride = static_cast<size_t>(gridDim.x) * blockDim.x;
    for (size_t group = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x; group < num_groups; group += stride)
    {
        const bool sentinel = group == num_regular;
        const size_t slot = sentinel ? capacity : group_slots[group];
        const uint64_t * record = accumulators.of(slot);
        const Key key = sentinel ? key_sentinel : slots[slot];

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


unsigned blocksFor(size_t work)
{
    return static_cast<unsigned>(std::min<size_t>((work + threads_per_block - 1) / threads_per_block, max_blocks));
}

void checkCount(size_t actual, size_t expected, const std::string & what)
{
    if (actual != expected)
        throw CudfError(std::to_string(actual) + " " + what + ", expected " + std::to_string(expected));
}

void checkLaunch(const std::string & what)
{
    checkCuda(cudaGetLastError(), "cannot launch the kernel that " + what);
}

bool isSigned(GPUElementType type)
{
    switch (type)
    {
        case GPUElementType::Int8:
        case GPUElementType::Int16:
        case GPUElementType::Int32:
        case GPUElementType::Int64:
            return true;
        default:
            return false;
    }
}

Fold foldOf(const GPUGroupByValue & value)
{
    const bool is_float = !isInteger(value.element_type);
    switch (value.aggregation)
    {
        case GPUAggregationKind::Sum:
            return is_float ? Fold::SumFloat : Fold::SumInt;
        case GPUAggregationKind::Min:
            return is_float ? Fold::MinFloat : (isSigned(value.element_type) ? Fold::MinSigned : Fold::MinUnsigned);
        case GPUAggregationKind::Max:
            return is_float ? Fold::MaxFloat : (isSigned(value.element_type) ? Fold::MaxSigned : Fold::MaxUnsigned);
    }
    throw CudfError("unknown aggregation " + std::to_string(static_cast<int>(value.aggregation)));
}

/// What an accumulator holds before any row is folded into it.
uint64_t identityOf(Fold fold)
{
    switch (fold)
    {
        case Fold::SumInt:
        case Fold::SumFloat:
        case Fold::MaxUnsigned:
            return 0;
        case Fold::MinSigned:
            return static_cast<uint64_t>(std::numeric_limits<int64_t>::max());
        case Fold::MaxSigned:
            return static_cast<uint64_t>(std::numeric_limits<int64_t>::min());
        case Fold::MinUnsigned:
            return std::numeric_limits<uint64_t>::max();
        case Fold::MinFloat:
            return std::bit_cast<uint64_t>(std::numeric_limits<double>::infinity());
        case Fold::MaxFloat:
            return std::bit_cast<uint64_t>(-std::numeric_limits<double>::infinity());
    }
    return 0;
}

std::pair<Store, uint32_t> storeOf(const GPUGroupByValue & value)
{
    if (value.aggregation == GPUAggregationKind::Sum)
        return {Store::Bits, 8};
    if (value.element_type == GPUElementType::Float32)
        return {Store::Narrow, 4};
    if (value.element_type == GPUElementType::Float64)
        return {Store::Bits, 8};
    return {Store::Truncate, static_cast<uint32_t>(sizeOf(value.element_type))};
}

GPUElementType leftIn(const GPUGroupByValue & value)
{
    return value.aggregation == GPUAggregationKind::Sum ? value.result_type : value.element_type;
}

struct Table
{
    std::unique_ptr<Set> set;
    const Key * slots = nullptr;
    size_t capacity = 0;
    std::optional<rmm::device_uvector<uint64_t>> records;
    Accumulators accumulators;
};

}

struct RecordGroupBy::State
{
    std::vector<GPUElementType> key_element_types;
    std::vector<uint32_t> key_shifts;
    size_t key_bytes = 0;

    std::vector<GPUGroupByValue> values;
    std::vector<Fold> folds;
    Identities identities;

    Table table;

    rmm::device_scalar<uint32_t> num_groups;
    rmm::device_scalar<uint32_t> sentinel_seen;
    size_t num_groups_on_host = 0;

    /// Around each chunk's kernel on its stream, so that its own time can be told from the time it
    /// waited for what was queued before it.
    cudaEvent_t kernel_started = nullptr;
    cudaEvent_t kernel_finished = nullptr;

    /// Slots of a bucket's table in shared memory: a power of two, with the records of that many
    /// groups beside the keys.
    uint32_t shared_capacity = 0;

    /// The sort's buffers and the overflow list, grown to the largest chunk so far, and the
    /// buckets' partial groups, a shared table's worth per bucket.
    rmm::device_uvector<uint8_t> buckets_in;
    rmm::device_uvector<uint8_t> buckets_out;
    rmm::device_uvector<uint32_t> indices_in;
    rmm::device_uvector<uint32_t> indices_out;
    rmm::device_uvector<char> sort_storage;
    rmm::device_uvector<uint32_t> overflow;
    rmm::device_scalar<uint32_t> num_overflow;
    rmm::device_uvector<Key> partial_keys;
    rmm::device_uvector<uint64_t> partial_records;

    std::vector<rmm::device_uvector<char>> output_keys;
    std::vector<rmm::device_uvector<char>> output_values;
    size_t output_groups = 0;
    bool finalized = false;

    State(GPUSpan<GPUElementType> key_element_types_, GPUSpan<GPUGroupByValue> values_)
        : key_element_types(key_element_types_.begin(), key_element_types_.end())
        , values(values_.begin(), values_.end())
        , num_groups(0, cudfStream())
        , sentinel_seen(0, cudfStream())
        , buckets_in(0, cudfStream())
        , buckets_out(0, cudfStream())
        , indices_in(0, cudfStream())
        , indices_out(0, cudfStream())
        , sort_storage(0, cudfStream())
        , overflow(0, cudfStream())
        , num_overflow(0, cudfStream())
        , partial_keys(0, cudfStream())
        , partial_records(0, cudfStream())
    {
        checkCuda(cudaEventCreate(&kernel_started), "cannot create an event");
        checkCuda(cudaEventCreate(&kernel_finished), "cannot create an event");

        if (key_element_types.empty() || key_element_types.size() > max_group_by_keys)
            throw CudfError("a `GROUP BY` of " + std::to_string(key_element_types.size()) + " keys on the device");
        if (values.empty() || values.size() > max_group_by_values)
            throw CudfError("a `GROUP BY` of " + std::to_string(values.size()) + " aggregates on the device");

        for (const GPUElementType key_element_type : key_element_types)
        {
            if (!isInteger(key_element_type))
                throw CudfError("a `GROUP BY` on a floating-point key on the device");
            key_shifts.push_back(static_cast<uint32_t>(key_bytes * 8));
            key_bytes += sizeOf(key_element_type);
        }
        if (key_bytes > max_group_by_key_bytes)
            throw CudfError("a `GROUP BY` on keys of " + std::to_string(key_bytes) + " bytes between them on the device");

        folds.reserve(values.size());
        identities.count = static_cast<uint32_t>(values.size());
        for (size_t i = 0; i < values.size(); ++i)
        {
            if (values[i].aggregation == GPUAggregationKind::Sum && sizeOf(values[i].result_type) != 8)
                throw CudfError("a sum into element type " + std::to_string(static_cast<int>(values[i].result_type)) + ", which is not eight bytes wide");
            folds.push_back(foldOf(values[i]));
            identities.values[i] = identityOf(folds.back());
        }

        /// The block's own few shared variables take a little of the shared memory, so a table of
        /// a whole 48 kilobytes would not launch.
        const size_t slot_bytes = sizeof(Key) + sizeof(uint64_t) * values.size();
        shared_capacity = static_cast<uint32_t>(std::bit_floor((shared_table_bytes - 64) / slot_bytes));

        const size_t num_partials = size_t{num_buckets} * shared_capacity;
        partial_keys.resize(num_partials, cudfStream());
        partial_records.resize(num_partials * values.size(), cudfStream());
    }

    /// Whether a chunk's rows can go in two passes: when the groups so far, spread over the
    /// buckets, fill less than half of a bucket's table in shared memory.
    bool canBucket(size_t rows) const
    {
        return rows >= min_partitioned_rows && rows <= std::numeric_limits<uint32_t>::max()
            && num_groups_on_host <= size_t{num_buckets} * shared_capacity / 2;
    }

    /// Nanoseconds per row each path last took, and the groups there were when both were known.
    std::optional<double> direct_cost;
    std::optional<double> bucketed_cost;
    size_t groups_when_measured = 0;

    /// Whether the next chunk that can go in two passes should: the direct kernel first, until it
    /// is measured, then the two passes until they are, then the cheaper; and both afresh once
    /// the groups have doubled, since the cost of each path moves with them.
    bool buckets()
    {
        if (bucketed_cost && num_groups_on_host > 2 * groups_when_measured)
        {
            direct_cost.reset();
            bucketed_cost.reset();
        }

        if (!direct_cost || *direct_cost < min_direct_cost_for_buckets)
            return false;
        if (!bucketed_cost)
            return true;
        return *bucketed_cost < *direct_cost;
    }

    void growSortBuffers(size_t rows, rmm::cuda_stream_view stream)
    {
        if (buckets_in.size() < rows)
        {
            buckets_in.resize(rows, stream);
            buckets_out.resize(rows, stream);
            indices_in.resize(rows, stream);
            indices_out.resize(rows, stream);
            overflow.resize(rows, stream);
        }

        size_t needed = 0;
        checkCuda(
            cub::DeviceRadixSort::SortPairs(
                nullptr,
                needed,
                buckets_in.data(),
                buckets_out.data(),
                indices_in.data(),
                indices_out.data(),
                rows,
                0,
                bucket_end_bit,
                stream.value()),
            "cannot size the sort of a chunk's rows into buckets");
        if (sort_storage.size() < needed)
            sort_storage.resize(needed, stream);
    }

    ~State()
    {
        if (kernel_started)
            cudaEventDestroy(kernel_started);
        if (kernel_finished)
            cudaEventDestroy(kernel_finished);
    }

    Table makeTable(size_t requested) const
    {
        const rmm::cuda_stream_view stream = cudfStream();

        Table made;
        made.set = std::make_unique<Set>(
            cuco::extent<size_t>{requested},
            cuco::empty_key<Key>{key_sentinel},
            cuda::std::equal_to<Key>{},
            cuco::linear_probing<1, cuco::xxhash_64<Key>>{},
            cuco::cuda_thread_scope<cuda::thread_scope_device>{},
            cuco::storage<1>{},
            rmm::mr::polymorphic_allocator<char>{},
            cuda::stream_ref{stream.value()});
        made.capacity = made.set->capacity();
        made.slots = made.set->ref(cuco::op::insert_and_find).storage_ref().data();

        if (made.capacity > max_capacity)
            throw CudfError("a `GROUP BY` table of " + std::to_string(made.capacity) + " slots on the device");

        made.records.emplace((made.capacity + 1) * values.size(), stream);
        made.accumulators = {.records = made.records->data(), .num_values = static_cast<uint32_t>(values.size())};

        initRecords<<<blocksFor(made.capacity + 1), threads_per_block, 0, stream.value()>>>(made.accumulators, made.capacity, identities);
        checkLaunch("empties the accumulators of a table of groups");

        return made;
    }

    /// How many more rows fit even were every one of them a new key.
    size_t room() const
    {
        if (!table.set)
            return 0;
        const size_t fits = static_cast<size_t>(max_load * static_cast<double>(table.capacity));
        return fits > num_groups_on_host ? fits - num_groups_on_host : 0;
    }

    /// Makes room for at least `more` new groups, growing the table when it has less.
    void ensureRoom(size_t more = min_chunk_rows)
    {
        if (table.set && room() >= more)
            return;

        const size_t needed = num_groups_on_host + more;
        const size_t requested = std::max({static_cast<size_t>(static_cast<double>(needed) / max_load) + 1, table.capacity * 2, min_capacity});
        if (requested > max_capacity)
            throw CudfError("a `GROUP BY` of " + std::to_string(needed) + " groups is too large for the device");

        Table larger = makeTable(requested);

        if (table.set)
        {
            const rmm::cuda_stream_view stream = cudfStream();

            checkCuda(
                cudaMemcpyAsync(
                    larger.accumulators.records + larger.capacity * values.size(),
                    table.accumulators.records + table.capacity * values.size(),
                    values.size() * sizeof(uint64_t),
                    cudaMemcpyDeviceToDevice,
                    stream.value()),
                "cannot carry the sentinel key's accumulators over");

            moveGroups<<<blocksFor(table.capacity), threads_per_block, 0, stream.value()>>>(
                table.slots, table.capacity, table.accumulators, larger.set->ref(cuco::op::insert_and_find), larger.slots, larger.accumulators);
            checkLaunch("moves the groups into a larger table");
        }

        table = std::move(larger);
    }

    /// The kernels' view of the rows from `offset` on.
    struct Layouts
    {
        KeyLayouts keys;
        ValueLayouts values;
        FilterLayouts filters;
        GPUFilterProgram program;
    };

    Layouts layoutsFor(
        GPUSpan<DeviceColumnView> keys,
        GPUSpan<DeviceColumnView> value_views,
        GPUSpan<DeviceColumnView> filter_columns,
        const GPUFilterProgram * filter,
        size_t offset) const
    {
        Layouts layouts;

        if (filter)
        {
            layouts.program = *filter;
            layouts.filters.present = true;
            layouts.filters.count = static_cast<uint32_t>(filter_columns.size());
            for (size_t i = 0; i < filter_columns.size(); ++i)
                layouts.filters.columns[i] = {
                    .data = filter_columns[i].data + offset * sizeOf(filter_columns[i].element_type),
                    .type = filter_columns[i].element_type,
                };
        }

        layouts.keys.count = static_cast<uint32_t>(keys.size());
        layouts.keys.may_equal_sentinel = key_bytes == sizeof(Key);
        for (size_t i = 0; i < keys.size(); ++i)
        {
            const uint32_t size = static_cast<uint32_t>(sizeOf(key_element_types[i]));
            layouts.keys.columns[i] = {.data = keys[i].data + offset * size, .size = size, .shift = key_shifts[i]};
        }

        layouts.values.count = static_cast<uint32_t>(values.size());
        for (size_t i = 0; i < values.size(); ++i)
        {
            layouts.values.columns[i] = {
                .data = value_views[i].data + offset * sizeOf(values[i].element_type),
                .type = values[i].element_type,
                .fold = folds[i],
            };
        }

        return layouts;
    }

    /// Queues the direct kernel over `rows` rows, or over the rows `order` lists, and waits for it,
    /// adding its time to `kernel_microseconds`.
    void aggregateDirectly(const Layouts & layouts, const uint32_t * order, size_t rows, double & kernel_microseconds)
    {
        const rmm::cuda_stream_view stream = cudfStream();

        checkCuda(cudaEventRecord(kernel_started, stream.value()), "cannot record an event");

        aggregateRows<<<blocksFor(rows), threads_per_block, 0, stream.value()>>>(
            table.set->ref(cuco::op::insert_and_find),
            table.slots,
            table.capacity,
            table.accumulators,
            layouts.keys,
            layouts.values,
            layouts.filters,
            layouts.program,
            order,
            rows,
            num_groups.data(),
            sentinel_seen.data());
        checkLaunch("groups a chunk of rows");

        checkCuda(cudaEventRecord(kernel_finished, stream.value()), "cannot record an event");

        num_groups_on_host = num_groups.value(stream);
        kernel_microseconds += elapsedMicroseconds();
    }

    /// Groups all `rows` rows in two passes: sorted into buckets and grouped in shared memory, then
    /// the buckets' partial groups folded into the table, then whatever overflowed the buckets'
    /// tables, directly; the last two in as many at a time as the table has room for, so that the
    /// table is grown by the groups there are and not by the slots the buckets' tables have.
    void aggregateBucketed(const Layouts & layouts, size_t rows, double & kernel_microseconds)
    {
        const size_t num_partials = size_t{num_buckets} * shared_capacity;

        const rmm::cuda_stream_view stream = cudfStream();
        growSortBuffers(rows, stream);
        checkCuda(cudaMemsetAsync(num_overflow.data(), 0, sizeof(uint32_t), stream.value()), "cannot clear the overflow count");

        checkCuda(cudaEventRecord(kernel_started, stream.value()), "cannot record an event");

        bucketRows<<<blocksFor(rows), threads_per_block, 0, stream.value()>>>(
            layouts.keys, layouts.filters, layouts.program, rows, buckets_in.data(), indices_in.data());
        checkLaunch("sorts a chunk's rows into buckets");

        size_t storage_bytes = sort_storage.size();
        checkCuda(
            cub::DeviceRadixSort::SortPairs(
                sort_storage.data(),
                storage_bytes,
                buckets_in.data(),
                buckets_out.data(),
                indices_in.data(),
                indices_out.data(),
                rows,
                0,
                bucket_end_bit,
                stream.value()),
            "cannot sort a chunk's rows into buckets");

        const Accumulators partials{.records = partial_records.data(), .num_values = static_cast<uint32_t>(values.size())};
        const size_t shared_bytes = (sizeof(Key) + sizeof(uint64_t) * values.size()) * shared_capacity;
        aggregateBuckets<<<num_buckets, bucket_threads, shared_bytes, stream.value()>>>(
            buckets_out.data(),
            indices_out.data(),
            rows,
            shared_capacity,
            layouts.keys,
            layouts.values,
            layouts.filters,
            layouts.program,
            identities,
            partial_keys.data(),
            partials,
            overflow.data(),
            num_overflow.data(),
            table.accumulators,
            table.capacity,
            sentinel_seen.data());
        checkLaunch("groups the buckets of a chunk");

        checkCuda(cudaEventRecord(kernel_finished, stream.value()), "cannot record an event");

        const size_t overflowed = num_overflow.value(stream);
        kernel_microseconds += elapsedMicroseconds();

        size_t merged = 0;
        while (merged < num_partials)
        {
            ensureRoom();
            const size_t some = std::min(num_partials - merged, room());

            checkCuda(cudaEventRecord(kernel_started, stream.value()), "cannot record an event");

            mergePartials<<<blocksFor(some), threads_per_block, 0, stream.value()>>>(
                table.set->ref(cuco::op::insert_and_find),
                table.slots,
                table.accumulators,
                partial_keys.data() + merged,
                Accumulators{.records = partials.of(merged), .num_values = partials.num_values},
                some,
                layouts.values,
                num_groups.data());
            checkLaunch("folds the buckets' groups into the table");

            checkCuda(cudaEventRecord(kernel_finished, stream.value()), "cannot record an event");

            num_groups_on_host = num_groups.value(stream);
            kernel_microseconds += elapsedMicroseconds();
            merged += some;
        }

        size_t taken = 0;
        while (taken < overflowed)
        {
            ensureRoom();
            const size_t some = std::min(overflowed - taken, room());
            aggregateDirectly(layouts, overflow.data() + taken, some, kernel_microseconds);
            taken += some;
        }
    }

    double elapsedMicroseconds() const
    {
        float milliseconds = 0;
        checkCuda(cudaEventElapsedTime(&milliseconds, kernel_started, kernel_finished), "cannot time the kernel");
        return milliseconds * 1000.0;
    }

    /// Groups rows from `offset` on - all of them in two passes when they are few groups' worth,
    /// otherwise as many as the table has room for - and answers how many, adding the kernels'
    /// own time to `kernel_microseconds`.
    size_t aggregateChunk(
        GPUSpan<DeviceColumnView> keys,
        GPUSpan<DeviceColumnView> value_views,
        GPUSpan<DeviceColumnView> filter_columns,
        const GPUFilterProgram * filter,
        size_t offset,
        size_t remaining,
        double & kernel_microseconds)
    {
        ensureRoom();

        const Layouts layouts = layoutsFor(keys, value_views, filter_columns, filter, offset);

        if (canBucket(remaining) && buckets())
        {
            const size_t rows = bucketed_cost ? remaining : std::min(remaining, measurement_rows);
            double microseconds = 0;
            aggregateBucketed(layouts, rows, microseconds);
            bucketed_cost = microseconds * 1000.0 / static_cast<double>(rows);
            groups_when_measured = num_groups_on_host;
            kernel_microseconds += microseconds;
            return rows;
        }

        const size_t rows = std::min(remaining, room());
        double microseconds = 0;
        aggregateDirectly(layouts, nullptr, rows, microseconds);
        direct_cost = microseconds * 1000.0 / static_cast<double>(rows);
        kernel_microseconds += microseconds;
        return rows;
    }
};

RecordGroupBy::RecordGroupBy(GPUSpan<GPUElementType> key_element_types_, GPUSpan<GPUGroupByValue> values_)
{
    initializeCudf();
    state = std::make_unique<State>(key_element_types_, values_);
}

RecordGroupBy::~RecordGroupBy() = default;

double RecordGroupBy::addBatch(
    GPUSpan<DeviceColumnView> keys, GPUSpan<DeviceColumnView> value_views, GPUSpan<DeviceColumnView> filter_columns, const GPUFilterProgram * filter)
{
    if (state->finalized)
        throw CudfError("a batch arrived after the aggregation was finalized");

    checkCount(keys.size(), state->key_element_types.size(), "key columns in a batch");
    checkCount(value_views.size(), state->values.size(), "value columns in a batch");

    if (filter)
    {
        checkCount(filter_columns.size(), filter->num_columns, "filter columns in a batch");
        if (filter->length > max_filter_instructions || filter->num_constants > max_filter_constants || filter->num_columns > max_filter_columns)
            throw CudfError("a `WHERE` program too large for the device");
    }

    const size_t num_rows = keys[0].rows;
    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (keys[i].element_type != state->key_element_types[i])
            throw CudfError("key column " + std::to_string(i) + " arrived as another element type");
        checkCount(keys[i].rows, num_rows, "rows in key column " + std::to_string(i));
    }
    for (size_t i = 0; i < value_views.size(); ++i)
    {
        if (value_views[i].element_type != state->values[i].element_type)
            throw CudfError("value column " + std::to_string(i) + " arrived as another element type");
        checkCount(value_views[i].rows, num_rows, "rows in value column " + std::to_string(i));
    }
    for (size_t i = 0; filter && i < filter_columns.size(); ++i)
        checkCount(filter_columns[i].rows, num_rows, "rows in filter column " + std::to_string(i));

    double kernel_microseconds = 0;
    size_t offset = 0;
    while (offset < num_rows)
        offset += state->aggregateChunk(keys, value_views, filter_columns, filter, offset, num_rows - offset, kernel_microseconds);
    return kernel_microseconds;
}

size_t RecordGroupBy::finalize()
{
    state->finalized = true;

    Table & table = state->table;
    if (!table.set)
        return 0;

    const rmm::cuda_stream_view stream = cudfStream();

    const bool with_sentinel = state->sentinel_seen.value(stream) != 0;
    const size_t num_regular = state->num_groups_on_host;
    const size_t num_groups = num_regular + (with_sentinel ? 1 : 0);
    if (num_groups == 0)
        return 0;

    rmm::device_uvector<uint32_t> group_slots(num_regular, stream);
    if (num_regular != 0)
    {
        const auto end = thrust::copy_if(
            rmm::exec_policy_nosync(stream),
            thrust::counting_iterator<uint32_t>(0),
            thrust::counting_iterator<uint32_t>(static_cast<uint32_t>(table.capacity)),
            table.slots,
            group_slots.begin(),
            IsOccupied{});
        checkCount(static_cast<size_t>(end - group_slots.begin()), num_regular, "occupied records");
    }

    OutputLayouts out;
    out.num_keys = static_cast<uint32_t>(state->key_element_types.size());
    for (size_t i = 0; i < state->key_element_types.size(); ++i)
    {
        const uint32_t size = static_cast<uint32_t>(sizeOf(state->key_element_types[i]));
        state->output_keys.emplace_back(num_groups * size, stream);
        out.keys[i] = {.data = state->output_keys.back().data(), .size = size, .shift = state->key_shifts[i], .store = Store::Truncate};
    }

    out.num_values = static_cast<uint32_t>(state->values.size());
    for (size_t i = 0; i < state->values.size(); ++i)
    {
        const auto [store, size] = storeOf(state->values[i]);
        state->output_values.emplace_back(num_groups * size, stream);
        out.values[i] = {.data = state->output_values.back().data(), .size = size, .shift = 0, .store = store};
    }

    writeGroups<<<blocksFor(num_groups), threads_per_block, 0, stream.value()>>>(
        group_slots.data(), num_regular, with_sentinel, table.slots, table.capacity, table.accumulators, out);
    checkLaunch("writes the groups out");

    stream.synchronize();

    table = Table{};
    state->output_groups = num_groups;
    return num_groups;
}

void RecordGroupBy::copyGroupsOut(GPUSpan<HostColumnView> keys, GPUSpan<HostColumnView> value_views)
{
    checkCount(keys.size(), state->key_element_types.size(), "key destinations");
    checkCount(value_views.size(), state->values.size(), "value destinations");

    if (state->output_groups == 0)
        return;

    const rmm::cuda_stream_view stream = cudfStream();

    for (size_t i = 0; i < keys.size(); ++i)
    {
        if (keys[i].element_type != state->key_element_types[i])
            throw CudfError("key column " + std::to_string(i) + " is copied out into a column of another type");
        checkCount(keys[i].rows, state->output_groups, "rows of room for key column " + std::to_string(i));

        checkCuda(
            cudaMemcpyAsync(keys[i].data, state->output_keys[i].data(), state->output_keys[i].size(), cudaMemcpyDeviceToHost, stream.value()),
            "cannot copy a column of group keys back");
    }

    for (size_t i = 0; i < value_views.size(); ++i)
    {
        if (value_views[i].element_type != leftIn(state->values[i]))
            throw CudfError("value column " + std::to_string(i) + " is copied out into a column of another type");
        checkCount(value_views[i].rows, state->output_groups, "rows of room for value column " + std::to_string(i));

        checkCuda(
            cudaMemcpyAsync(
                value_views[i].data, state->output_values[i].data(), state->output_values[i].size(), cudaMemcpyDeviceToHost, stream.value()),
            "cannot copy a column of aggregated values back");
    }

    stream.synchronize();
}

IGroupBy * IGroupBy::create(GPUSpan<GPUElementType> key_element_types, GPUSpan<GPUGroupByValue> values)
{
    return new RecordGroupBy(key_element_types, values);
}

}
