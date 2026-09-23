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

struct HostColumnView
{
    GPUElementType element_type;
    char * data = nullptr;
    size_t rows = 0;
};

constexpr size_t max_group_by_keys = 8;
constexpr size_t max_group_by_key_bytes = 8;
constexpr size_t max_group_by_values = 8;

/** A `WHERE` the device evaluates per row before it groups the row: the actions of the predicate's
  * expression as `ExpressionActions` lays them out, one instruction per action over a few registers.
  * A load puts a row's value of a filter column, or a constant, into its register; a comparison
  * puts a boolean into its register from two others; `And`, `Or` and `Not` read their operands
  * as ClickHouse reads a value in a `WHERE`, true when it is not zero, and so does the row's
  * verdict, which is what `result` holds at the end. Integers compare by their value whatever
  * their signs; a comparison between an integer and a float is compiled only when the integer is
  * a constant a double holds exactly, and then as that double, so that the device never rounds
  * an integer to compare it.
  */
enum class GPUFilterOp : int
{
    /// Loads the row's value of filter column `first`.
    LoadColumn = 0,
    /// Loads constant `first`.
    LoadConstant = 1,
    /// Copies register `first`.
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

/// A constant of the predicate: the bits of an `Int64`, a `UInt64` or a `Float64`.
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
    /// The register that holds the row's verdict.
    uint32_t result = 0;
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
