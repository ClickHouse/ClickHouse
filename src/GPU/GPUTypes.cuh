#pragma once

#include <rmm/cuda_stream_view.hpp>

#include <cstddef>
#include <cstdint>
#include <exception>

/** The vocabulary shared by the two sides of `src/GPU`.
  *
  * The `.cu` files talk to cuDF and are compiled by nvcc against libstdc++; the `.cpp` files are
  * compiled with the rest of ClickHouse by clang against libc++. Whatever nvcc compiles is a `.cu`
  * or a `.cuh`, and a `.h` is the host side's alone. Some `.cuh` are included by both sides: this
  * one, `GPUStreams.cuh`, and the headers of the classes the nvcc side defines - `CudfGroupBy.cuh`,
  * `CudfHashJoin.cuh`, `CudfReduction.cuh`, `RecordGroupBy.cuh` - whose signatures name only the
  * types of this header and `rmm::cuda_stream_view`, which holds a `cudaStream_t` and nothing
  * else: nothing of either standard library crosses between them, and the cuDF side only ever
  * sees device pointers and streams. The host side
  * takes nothing of rmm but that type, and never calls what of it throws - `synchronize` - since
  * an exception built against libstdc++ is not one it can read.
  *
  * Exceptions do cross. Everything linked into the binary - both sides, cuDF, rmm - resolves its
  * `__cxa_*`, personality and unwinder symbols against ClickHouse's own libc++abi and libunwind,
  * because `libstdc++.so` comes last on the link line (see `cmake/linux/default_libs.cmake`), so a
  * `throw` on the cuDF side unwinds into a `catch (const std::exception &)` on this one, and
  * `what()` dispatches to the thrower's vtable. What must not cross is an object whose layout the
  * two sides disagree on: the standard exception classes exist twice in the binary, and the
  * process binds the names both define - `what`, the destructors - to libc++'s definitions,
  * which do not know the layout of a `std::logic_error` built on the cuDF side. So an error of
  * either side is one `DB::Exception` of `GPU_ERROR`, built on the host side by `throwGPUError`
  * wherever it is thrown from, and the cuDF side wraps its calls into cuDF in `guarded`, which
  * turns what cuDF throws into one without asking or destroying the original - see `Cudf.cuh`.
  */
namespace DB::GPU
{

/// Throws a `DB::Exception` of `GPU_ERROR` that says `message`. Defined on the host side, which
/// alone knows the class; the cuDF side calls it for its own errors.
[[noreturn]] void throwGPUError(const char * message);

/// Whether `exception` is a `DB::Exception`, which the cuDF side lets through as it is. The
/// standard `std::exception` is the same class to both sides: a vtable pointer and nothing else.
bool isClickHouseException(const std::exception & exception);

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
    /// Values of varying width: a column of them is `Variable`, its bytes and their offsets.
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

/// One aggregate function of a keyed aggregation: what it reads, and what it leaves. A `sum`
/// leaves a group in `result_type`, eight bytes wide; a `min` or `max` leaves it in `element_type`.
struct GPUGroupByValue
{
    GPUElementType element_type;
    GPUElementType result_type;
    GPUAggregationKind aggregation;
};

/** How a column lies in memory as both sides take it: `rows` values of one width, or bytes and
  * offsets into them. A column of another kind is a struct of its own below, a case of
  * `DeviceColumnView`, and a download on the host side (`GPUColumns.h`) and a view of cuDF's
  * (`Cudf.cuh`) of it.
  */
enum class GPUColumnKind : int
{
    Fixed = 0,
    Variable = 1,
};

constexpr GPUColumnKind columnKindOf(GPUElementType type)
{
    return type == GPUElementType::String ? GPUColumnKind::Variable : GPUColumnKind::Fixed;
}

/// `rows` values of `element_type`, which is not `String`, one after another at `data`, in device
/// memory.
struct DeviceFixedColumn
{
    GPUElementType element_type = GPUElementType::UInt8;
    const char * data = nullptr;
    size_t rows = 0;
};

/// `rows` values of varying width in device memory, their bytes one after another at `chars`,
/// `chars_bytes` of them. `offsets` holds `rows + 1` offsets into `chars`, from 0 to `chars_bytes`:
/// row `i` is from `offsets[i]` up to `offsets[i + 1]`. A column of no rows may have no offsets at
/// all. A column of `String` is one.
struct DeviceVariableColumn
{
    const uint64_t * offsets = nullptr;
    const char * chars = nullptr;
    size_t rows = 0;
    size_t chars_bytes = 0;
};

/** A column in device memory of any kind, as it crosses between the two sides: `kind` says which
  * member of the union it is. A `std::variant` would not do, as the two standard libraries lay it
  * out differently. Each side reads a member through a check of its own - `fixedOrThrow` or a
  * switch on `kind` on the host side, `columnViewOf` on the other.
  *
  * `null_mask`, when there is one, holds a byte per row, not zero for a `NULL`, as a
  * `ColumnNullable` keeps it. Nothing takes one yet, and whatever takes a view refuses one.
  */
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
    return type != GPUElementType::Float32 && type != GPUElementType::Float64 && type != GPUElementType::String;
}

/// The width of a value of a `Fixed` column. The kernels call it too, so it cannot throw: a
/// `String` has no one width, and is 0 here.
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
