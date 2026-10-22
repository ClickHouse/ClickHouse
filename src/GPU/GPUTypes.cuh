#pragma once

#include <array>
#include <tuple>
#include <utility>

#include <cuda/stream_ref>

#include <cuda_runtime_api.h>

#include <cstddef>
#include <cstdint>
#include <exception>

namespace DB::GPU
{

[[noreturn]] __host__ void throwGPUError(const char * message);

[[noreturn]] __host__ void throwCudaError(cudaError_t status, const char * message);

__host__ inline void checkCuda(cudaError_t status, const char * message)
{
    if (status != cudaSuccess)
        throwCudaError(status, message);
}

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
    /// Of the rows, of no column. Only in a `GROUP BY`.
    Count = 3,
    /// `avg`, into a `Float64`. Only in a `GROUP BY`.
    Mean = 4,
};

enum class GPUCodec : int
{
    LZ4 = 0,
    ZSTD = 1,
    /// A block in chunks, each a chunk of nvcomp, see `GPU::gdeflate_chunk_bytes`.
    GDEFLATE = 2,
};

/// One aggregate of a `GROUP BY`: of the value column `column`, or of no column for `GPUAggregationKind::Count`, into a
/// column of `result_type`.
struct GPUGroupByAggregate
{
    static constexpr uint32_t no_column = ~uint32_t{0};

    GPUAggregationKind aggregation;
    uint32_t column;
    GPUElementType result_type;
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
    const GPUElementType element_type = GPUElementType::UInt8;
    const char * const data = nullptr;
    const size_t rows = 0;
};

/// `offsets` holds `rows + 1` values, and may be null when `rows` is 0.
struct DeviceVariableColumn
{
    const uint64_t * const offsets = nullptr;
    const char * const chars = nullptr;
    const size_t rows = 0;
    const size_t chars_bytes = 0;
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
    /// Of two values as doubles, into a double: only in a program whose every value is a `Float64` but the constants.
    Plus = 12,
    Minus = 13,
    Multiply = 14,
    Divide = 15,
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

/// A value column of a `GROUP BY` that the device computes from other value columns by `program`, into a `Float64`:
/// the `columns[i]`-th value column is what the program loads as its column `i`.
struct GPUDerivedValue
{
    GPUFilterProgram program;
    uint32_t columns[max_filter_columns];
    uint32_t num_columns = 0;
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
