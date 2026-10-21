#pragma once

#include <GPU/GPUTypes.cuh>

#include <cstddef>
#include <cstdint>

/// Evaluates a `GPUFilterProgram`, the `WHERE` of a GPU aggregation compiled by `GPUFilterCompiler`, for one row on the device.
namespace DB::GPU::Filtering
{

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

/// What a value is as a double, converted the way ClickHouse converts it in arithmetic with one.
__device__ __forceinline__ double asDouble(const FilterValue & value)
{
    switch (value.kind)
    {
        case GPUFilterValueKind::Signed: return static_cast<double>(static_cast<int64_t>(value.bits));
        case GPUFilterValueKind::Unsigned: return static_cast<double>(value.bits);
        case GPUFilterValueKind::Float: return __longlong_as_double(static_cast<long long>(value.bits));
    }
    return 0;
}

__device__ __forceinline__ FilterValue evaluateProgram(const GPUFilterProgram & program, const FilterLayouts & filters, size_t row)
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
            case GPUFilterOp::Plus:
            case GPUFilterOp::Minus:
            case GPUFilterOp::Multiply:
            case GPUFilterOp::Divide:
            {
                const double first = asDouble(registers[instruction.first]);
                const double second = asDouble(registers[instruction.second]);
                double value = 0;
                switch (instruction.op)
                {
                    case GPUFilterOp::Plus: value = first + second; break;
                    case GPUFilterOp::Minus: value = first - second; break;
                    case GPUFilterOp::Multiply: value = first * second; break;
                    default: value = first / second; break;
                }
                result = {GPUFilterValueKind::Float, static_cast<uint64_t>(__double_as_longlong(value))};
                break;
            }
        }
    }

    return registers[program.result];
}

__device__ __forceinline__ bool passesFilter(const GPUFilterProgram & program, const FilterLayouts & filters, size_t row)
{
    return isTrue(evaluateProgram(program, filters, row));
}

}
