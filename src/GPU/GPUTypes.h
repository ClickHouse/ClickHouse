#pragma once

#include <cstddef>
#include <cstdint>

/** The vocabulary shared by the two sides of `src/GPU`.
  *
  * The `Cudf*` files talk to cuDF and are compiled by nvcc against libstdc++; everything else is
  * compiled with the rest of ClickHouse by clang against libc++. The two meet in the `I*`
  * interfaces, whose signatures name only the types of this header: nothing of either standard
  * library crosses between them, and the cuDF side only ever sees device pointers.
  *
  * Exceptions do cross. Everything linked into the binary - both sides, cuDF, rmm - resolves its
  * `__cxa_*`, personality and unwinder symbols against ClickHouse's own libc++abi and libunwind,
  * because `libstdc++.so` comes last on the link line (see `cmake/linux/default_libs.cmake`), so a
  * `throw` on the cuDF side unwinds into a `catch (const std::exception &)` on this one, and
  * `what()` dispatches to the thrower's vtable. What must not cross is an object whose layout the
  * two sides disagree on: the standard exception classes exist twice in the binary, and libc++'s
  * `std::exception` is not even the same size as libstdc++'s. The cuDF side therefore throws its
  * own `CudfError`, whose message and `what` are its own - see `Cudf.h`. cuDF's internal
  * `CUDF_EXPECTS` failures are `std::logic_error`s built on the wrong side, and remain fatal.
  */
namespace DB::GPU
{

template <typename T>
struct GPUSpan
{
    const T * values = nullptr;
    size_t count = 0;

    GPUSpan() = default;

    GPUSpan(const T * values_, size_t count_)
        : values(values_), count(count_)
    {
    }

    template <typename Container, typename = typename Container::value_type>
    GPUSpan(const Container & container)
        : values(container.data()), count(container.size())
    {
    }

    const T & operator[](size_t index) const { return values[index]; }

    size_t size() const { return count; }
    bool empty() const { return count == 0; }

    const T * begin() const { return values; }
    const T * end() const { return values + count; }
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

/// One aggregate function of a keyed aggregation: what it reads, and what it leaves. A `sum`
/// leaves a group in `result_type`, eight bytes wide; a `min` or `max` leaves it in `element_type`.
struct GPUGroupByValue
{
    GPUElementType element_type;
    GPUElementType result_type;
    GPUAggregationKind aggregation;
};

/// A column of fixed-width values in device memory, as the cuDF side receives it.
struct DeviceColumnView
{
    GPUElementType element_type;
    const char * data = nullptr;
    size_t rows = 0;
};

/// A column of fixed-width values in host memory, sized for what the cuDF side copies into it.
struct HostColumnView
{
    GPUElementType element_type;
    char * data = nullptr;
    size_t rows = 0;
};

/// What a keyed aggregation on the device takes at most. Its key columns are packed into one
/// eight-byte key, and its layouts travel to the kernels as parameters of fixed size.
constexpr size_t max_group_by_keys = 8;
constexpr size_t max_group_by_key_bytes = 8;
constexpr size_t max_group_by_values = 8;

/** A `WHERE` the device evaluates per row before it groups the row: a program for a stack machine
  * of a few instructions, over the columns of the predicate. A comparison pops two values and
  * pushes a boolean; `And`, `Or` and `Not` work on booleans; `IsTrue` turns a value into a boolean
  * the way ClickHouse reads a `WHERE` on a plain column, true when it is not zero. Integers compare
  * by their value whatever their signs; a comparison between an integer and a float is not
  * compiled, so that the device never rounds an integer to compare it.
  */
enum class GPUFilterOp : int
{
    /// Pushes the row's value of filter column `operand`.
    PushColumn = 0,
    /// Pushes constant `operand`.
    PushConstant = 1,
    Equals = 2,
    NotEquals = 3,
    Less = 4,
    LessOrEquals = 5,
    Greater = 6,
    GreaterOrEquals = 7,
    And = 8,
    Or = 9,
    Not = 10,
    IsTrue = 11,
};

struct GPUFilterInstruction
{
    GPUFilterOp op;
    uint32_t operand;
};

enum class GPUFilterValueKind : int
{
    Signed = 0,
    Unsigned = 1,
    Float = 2,
};

/// A constant of the predicate: the bits of an `Int64`, a `UInt64` or a `Float64`.
struct GPUFilterConstant
{
    GPUFilterValueKind kind;
    uint64_t bits;
};

constexpr size_t max_filter_columns = 8;
constexpr size_t max_filter_instructions = 32;
constexpr size_t max_filter_constants = 16;
constexpr size_t max_filter_stack = 8;

struct GPUFilterProgram
{
    GPUFilterInstruction code[max_filter_instructions];
    uint32_t length = 0;
    GPUFilterConstant constants[max_filter_constants];
    uint32_t num_constants = 0;
    uint32_t num_columns = 0;
};

constexpr bool isInteger(GPUElementType type)
{
    return type != GPUElementType::Float32 && type != GPUElementType::Float64;
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
    }
    return 0;
}

}
