#pragma once

#include <GPU/GPUTypes.cuh>

#include <cuco/static_set.cuh>

#include <rmm/mr/polymorphic_allocator.hpp>

#include <cuda/std/functional>

#include <limits>

namespace DB::GPU::Grouping
{

using Key = uint64_t;

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

struct Accumulators
{
    uint64_t * records = nullptr;
    uint32_t num_values = 0;

    __host__ __device__ __forceinline__ uint64_t * of(size_t slot) const { return records + slot * num_values; }
};

struct TableRef
{
    InsertRef set;
    const Key * slots = nullptr;
    size_t capacity = 0;
    Accumulators accumulators;
};

struct Counters
{
    uint32_t * num_groups = nullptr;
    uint32_t * sentinel_seen = nullptr;
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

struct FilterLayouts
{
    FilterColumnLayout columns[max_filter_columns];
    uint32_t count = 0;
    bool present = false;
};

struct Chunk
{
    KeyLayouts keys;
    ValueLayouts values;
    FilterLayouts filters;
    GPUFilterProgram filter;
    const uint32_t * order = nullptr;
    size_t rows = 0;
};

struct Identities
{
    uint64_t values[max_group_by_values];
    uint32_t count = 0;
};

enum class Store : int
{
    Bits,
    Truncate,
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

struct FilterValue
{
    GPUFilterValueKind kind;
    uint64_t bits;
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

__device__ __forceinline__ void foldRow(const ValueLayouts & values, size_t row, uint64_t * record)
{
    for (uint32_t i = 0; i < values.count; ++i)
        foldBits(values.columns[i].fold, loadValueBits(values.columns[i], row), record + i);
}

__device__ __forceinline__ Key packKey(const KeyLayouts & keys, size_t row)
{
    Key key = 0;
    for (uint32_t i = 0; i < keys.count; ++i)
        key |= loadBits(keys.columns[i].data, row, keys.columns[i].size) << keys.columns[i].shift;
    return key;
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

__device__ __forceinline__ bool keeps(const Chunk & chunk, size_t row)
{
    return chunk.order || !chunk.filters.present || passesFilter(chunk.filter, chunk.filters, row);
}

__device__ __forceinline__ size_t slotOf(TableRef & table, const KeyLayouts & keys, Key key, Counters counters)
{
    if (keys.may_equal_sentinel && key == key_sentinel)
    {
        *counters.sentinel_seen = 1;
        return table.capacity;
    }

    const auto [it, inserted] = table.set.insert_and_find(key);
    if (inserted)
        atomicAdd(counters.num_groups, 1u);
    return static_cast<size_t>(it - table.slots);
}

}
