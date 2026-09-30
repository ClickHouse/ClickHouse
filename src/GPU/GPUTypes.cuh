#pragma once

#include <rmm/cuda_stream_view.hpp>

#include <cstddef>
#include <cstdint>
#include <exception>

namespace DB::GPU
{

[[noreturn]] __host__ void throwGPUError(const char * message);

__host__ bool isClickHouseException(const std::exception & exception);

template <typename T>
struct GPUSpan
{
    const T * values = nullptr;
    size_t count = 0;

    __host__ GPUSpan() = default;

    __host__ GPUSpan(const T * values_, size_t count_)
        : values(values_), count(count_)
    {
    }

    template <typename Container, typename = typename Container::value_type>
    __host__ GPUSpan(const Container & container)
        : values(container.data()), count(container.size())
    {
    }

    __host__ const T & operator[](size_t index) const { return values[index]; }

    __host__ size_t size() const { return count; }
    __host__ bool empty() const { return count == 0; }

    __host__ const T * begin() const { return values; }
    __host__ const T * end() const { return values + count; }
};

enum class GPUElementType : int
{
    UInt8 = 0,
    UInt16 = 1,
    UInt32 = 2,
    UInt64 = 3,
    Int8 = 4,
    Int16 = 5,
    Int32 = 6,
    Int64 = 7,
    Float32 = 8,
    Float64 = 9,
    String = 10,
};

enum class GPUAggregationKind : int
{
    Sum = 0,
    Min = 1,
    Max = 2,
};

enum class GPUCodec : int
{
    LZ4 = 0,
    ZSTD = 1,
};

struct GPUGroupByValue
{
    GPUElementType element_type;
    GPUElementType result_type;
    GPUAggregationKind aggregation;
};

enum class GPUColumnKind : int
{
    Fixed = 0,
    Variable = 1,
};

__host__ __device__ constexpr GPUColumnKind columnKindOf(GPUElementType type)
{
    return type == GPUElementType::String ? GPUColumnKind::Variable : GPUColumnKind::Fixed;
}

struct DeviceFixedColumn
{
    GPUElementType element_type = GPUElementType::UInt8;
    const char * data = nullptr;
    size_t rows = 0;
};

/// `offsets` holds `rows + 1` values, and may be null when `rows` is 0.
struct DeviceVariableColumn
{
    const uint64_t * offsets = nullptr;
    const char * chars = nullptr;
    size_t rows = 0;
    size_t chars_bytes = 0;
};

struct DeviceColumnView
{
    GPUColumnKind kind;
    union
    {
        DeviceFixedColumn fixed;
        DeviceVariableColumn variable;
    };
    const uint8_t * null_mask = nullptr;

    __host__ DeviceColumnView() : kind(GPUColumnKind::Fixed), fixed{} { }
    __host__ DeviceColumnView(const DeviceFixedColumn & fixed_) : kind(GPUColumnKind::Fixed), fixed(fixed_) { } /// NOLINT
    __host__ DeviceColumnView(const DeviceVariableColumn & variable_) : kind(GPUColumnKind::Variable), variable(variable_) { } /// NOLINT

    __host__ size_t rows() const { return kind == GPUColumnKind::Fixed ? fixed.rows : variable.rows; }

    __host__ GPUElementType type() const { return kind == GPUColumnKind::Fixed ? fixed.element_type : GPUElementType::String; }
};

struct HostColumnView
{
    GPUElementType element_type;
    char * data = nullptr;
    size_t rows = 0;
};

constexpr size_t max_group_by_keys = 8;
constexpr size_t max_group_by_key_bytes = 8;
constexpr size_t max_group_by_values = 8;

enum class GPUFilterOp : int
{
    LoadColumn = 0,
    LoadConstant = 1,
    Move = 2,
    Equals = 3,
    NotEquals = 4,
    Less = 5,
    LessOrEquals = 6,
    Greater = 7,
    GreaterOrEquals = 8,
    And = 9,
    Or = 10,
    Not = 11,
};

struct GPUFilterInstruction
{
    GPUFilterOp op;
    uint32_t result;
    uint32_t first;
    uint32_t second;
};

enum class GPUFilterValueKind : int
{
    Signed = 0,
    Unsigned = 1,
    Float = 2,
};

struct GPUFilterConstant
{
    GPUFilterValueKind kind;
    uint64_t bits;
};

constexpr size_t max_filter_columns = 8;
constexpr size_t max_filter_instructions = 32;
constexpr size_t max_filter_constants = 16;
constexpr size_t max_filter_registers = 16;

struct GPUFilterProgram
{
    GPUFilterInstruction code[max_filter_instructions];
    uint32_t length = 0;
    GPUFilterConstant constants[max_filter_constants];
    uint32_t num_constants = 0;
    uint32_t num_columns = 0;
    uint32_t num_registers = 0;
    uint32_t result = 0;
};

__host__ __device__ constexpr bool isInteger(GPUElementType type)
{
    return type != GPUElementType::Float32 && type != GPUElementType::Float64 && type != GPUElementType::String;
}

__host__ __device__ constexpr size_t sizeOf(GPUElementType type)
{
    switch (type)
    {
        case GPUElementType::UInt8:
        case GPUElementType::Int8:
            return 1;
        case GPUElementType::UInt16:
        case GPUElementType::Int16:
            return 2;
        case GPUElementType::UInt32:
        case GPUElementType::Int32:
        case GPUElementType::Float32:
            return 4;
        case GPUElementType::UInt64:
        case GPUElementType::Int64:
        case GPUElementType::Float64:
            return 8;
        case GPUElementType::String:
            return 0;
    }
    return 0;
}

}
