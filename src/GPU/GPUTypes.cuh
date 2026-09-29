#pragma once

#include <rmm/cuda_stream_view.hpp>

#include <cstddef>
#include <cstdint>
#include <exception>

namespace DB::GPU
{

[[noreturn]] void throwGPUError(const char * message);

bool isClickHouseException(const std::exception & exception);

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

enum class GPUCodec : int
{
    LZ4 = 0,
    ZSTD = 1,
};

enum class GPUColumnKind : int
{
    Fixed = 0,
    Variable = 1,
};

constexpr GPUColumnKind columnKindOf(GPUElementType type)
{
    return type == GPUElementType::String ? GPUColumnKind::Variable : GPUColumnKind::Fixed;
}

struct DeviceFixedColumn
{
    GPUElementType element_type = GPUElementType::UInt8;
    const char * data = nullptr;
    size_t rows = 0;
};

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

    DeviceColumnView() : kind(GPUColumnKind::Fixed), fixed{} { }
    DeviceColumnView(const DeviceFixedColumn & fixed_) : kind(GPUColumnKind::Fixed), fixed(fixed_) { } /// NOLINT
    DeviceColumnView(const DeviceVariableColumn & variable_) : kind(GPUColumnKind::Variable), variable(variable_) { } /// NOLINT

    size_t rows() const { return kind == GPUColumnKind::Fixed ? fixed.rows : variable.rows; }

    GPUElementType type() const { return kind == GPUColumnKind::Fixed ? fixed.element_type : GPUElementType::String; }
};

struct HostColumnView
{
    GPUElementType element_type;
    char * data = nullptr;
    size_t rows = 0;
};

constexpr bool isInteger(GPUElementType type)
{
    return type != GPUElementType::Float32 && type != GPUElementType::Float64 && type != GPUElementType::String;
}

constexpr size_t sizeOf(GPUElementType type)
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
