#include <Columns/ColumnsScatter.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Core/TypeId.h>
#include <base/defines.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/memcpySmall.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bit>
#include <cstring>
#include <limits>
#include <numeric>
#include <string_view>
#include <type_traits>
#include <typeinfo>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace ColumnsScatter
{

namespace
{

enum class Kind : UInt8
{
    None,
    Fixed,
    String,
    Nullable,
    Tuple,
    Array,
};

struct KernelEntry
{
    Kind kind = Kind::None;
    /// `Const`, `Sparse`, `Replicated` and `ColumnUnique` report the `TypeIndex` of the column they hold, so the kernel
    /// also checks the class it is written for.
    const std::type_info * column_class = nullptr;
};

using KernelTable = std::array<KernelEntry, std::numeric_limits<std::underlying_type_t<TypeIndex>>::max() + 1>;

template <typename Column>
constexpr void setKernel(KernelTable & table, TypeIndex type, Kind kind)
{
    table[static_cast<size_t>(type)] = {kind, &typeid(Column)};
}

template <typename... T>
constexpr void setVectorKernels(KernelTable & table)
{
    (setKernel<ColumnVector<T>>(table, TypeToTypeIndex<T>, Kind::Fixed), ...);
}

template <typename... T>
constexpr void setDecimalKernels(KernelTable & table)
{
    (setKernel<ColumnDecimal<T>>(table, TypeToTypeIndex<T>, Kind::Fixed), ...);
}

constexpr KernelTable buildKernelTable()
{
    KernelTable table{};
    setVectorKernels<
        UInt8, UInt16, UInt32, UInt64, UInt128, UInt256, Int8, Int16, Int32, Int64, Int128, Int256,
        BFloat16, Float32, Float64, UUID, IPv4, IPv6>(table);
    setDecimalKernels<Decimal32, Decimal64, Decimal128, Decimal256, DateTime64, Time64>(table);
    setKernel<ColumnFixedString>(table, TypeIndex::FixedString, Kind::Fixed);
    setKernel<ColumnString>(table, TypeIndex::String, Kind::String);
    setKernel<ColumnNullable>(table, TypeIndex::Nullable, Kind::Nullable);
    setKernel<ColumnTuple>(table, TypeIndex::Tuple, Kind::Tuple);
    setKernel<ColumnArray>(table, TypeIndex::Array, Kind::Array);
    return table;
}

constexpr KernelTable KERNEL_TABLE = buildKernelTable();

Kind kernelKind(const IColumn & column)
{
    const KernelEntry & entry = KERNEL_TABLE[static_cast<size_t>(column.getDataType())];
    if (entry.kind == Kind::None || typeid(column) != *entry.column_class)
        return Kind::None;
    return entry.kind;
}

/// The rule of `hasKernel`. `nested` is true inside an `Array` or a `Nullable`.
bool accepts(const IColumn & column, bool nested)
{
    switch (kernelKind(column))
    {
        case Kind::Fixed:
        case Kind::String:
            return true;
        case Kind::Nullable:
            return accepts(assert_cast<const ColumnNullable &>(column).getNestedColumn(), true);
        case Kind::Array:
            return accepts(assert_cast<const ColumnArray &>(column).getData(), true);
        case Kind::Tuple:
            return nested
                && std::ranges::all_of(
                       assert_cast<const ColumnTuple &>(column).getColumns(), [](const auto & element) { return accepts(*element, true); });
        case Kind::None:
            return false;
    }
    UNREACHABLE();
}

template <typename Id>
void checkPieceCount(size_t num_pieces)
{
    if constexpr (std::is_same_v<Id, UInt16>)
    {
        if (num_pieces > 65536)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot scatter into {} pieces with 16-bit piece ids", num_pieces);
    }
}

/// Where row `row` starts in the nested column. Reads `offsets[row - 1]`. For row 0 this is the 0 that every padded
/// array of offsets holds before its start.
ALWAYS_INLINE UInt64 startOf(const UInt64 * offsets, size_t row)
{
    return offsets[static_cast<ssize_t>(row) - 1];
}

/// The cursors of a `String` piece. One 32-byte record per piece, so that a row touches one cache line of cursors
/// instead of one line in each of three arrays.
struct StringCursor
{
    char * chars = nullptr;
    UInt64 * offsets = nullptr;
    UInt64 last_offset = 0;
    UInt64 padding = 0;
};

/// The cursors of the offsets of an `Array` piece.
struct ArrayCursor
{
    UInt64 * offsets = nullptr;
    UInt64 last_offset = 0;
};

/// The scattered column, or one of its nested columns: the source being scattered, and the cursors of every piece.
struct Node
{
    Kind kind = Kind::None;
    /// Fixed: the size of a row in bytes.
    size_t width = 0;
    /// A `String` or an `Array` is here or below.
    bool needs_sizes = false;
    /// Nullable: the null map, then the nested column. Tuple: the elements. Array: the elements.
    VectorWithMemoryTracking<Node> children;

    /// The source. Fixed: its rows. String: its chars.
    const char * data = nullptr;
    /// The source. String, Array: its offsets.
    const UInt64 * offsets = nullptr;

    /// One per piece. String: the number of bytes of chars. Array: the number of elements.
    PaddedPODArray<size_t> sizes;
    /// Fixed: the cursors, and the staging lines when the column is written with `StoreMode::Combined`.
    ScatterScratch scratch;
    PaddedPODArray<StringCursor> string_cursors;
    PaddedPODArray<ArrayCursor> array_cursors;
};

template <typename T, typename Column>
using WithConstOf = std::conditional_t<std::is_const_v<Column>, const T, T>;

/// Calls `f(i, part)` for each nested column `part` of `column` that `children[i]` of its node stands for. `Column` is
/// `IColumn` or `const IColumn`.
template <typename Column, typename F>
void forEachChild(Kind kind, Column & column, F && f)
{
    switch (kind)
    {
        case Kind::Nullable:
        {
            auto & nullable = assert_cast<WithConstOf<ColumnNullable, Column> &>(column);
            f(0, nullable.getNullMapColumn());
            f(1, nullable.getNestedColumn());
            return;
        }
        case Kind::Tuple:
        {
            auto & tuple = assert_cast<WithConstOf<ColumnTuple, Column> &>(column);
            for (size_t i = 0; i < tuple.tupleSize(); ++i)
                f(i, tuple.getColumn(i));
            return;
        }
        case Kind::Array:
            f(0, assert_cast<WithConstOf<ColumnArray, Column> &>(column).getData());
            return;
        case Kind::Fixed:
        case Kind::String:
        case Kind::None:
            return;
    }
}

/// The nodes of `column` and of its nested columns, with the cursors of `num_pieces` pieces. `rows` is the number of rows
/// scattered from all sources, or 0 when the rows of `column` are scattered by ranges, as the elements of an `Array` are.
Node buildNode(const IColumn & column, size_t num_pieces, size_t rows, bool allow_combining)
{
    Node node;
    node.kind = kernelKind(column);
    switch (node.kind)
    {
        case Kind::Fixed:
            node.width = column.sizeOfValueIfFixed();
            node.scratch.init(num_pieces, chooseStoreMode(node.width, num_pieces, rows, allow_combining));
            break;
        case Kind::String:
            node.needs_sizes = true;
            node.sizes.resize_fill(num_pieces, 0);
            node.string_cursors.resize_fill(num_pieces);
            break;
        case Kind::Array:
            node.needs_sizes = true;
            node.sizes.resize_fill(num_pieces, 0);
            node.array_cursors.resize_fill(num_pieces);
            break;
        case Kind::Nullable:
        case Kind::Tuple:
            break;
        case Kind::None:
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Column {} has no scatter kernel", column.getName());
    }
    forEachChild(
        node.kind,
        column,
        [&](size_t, const IColumn & part)
        {
            node.children.push_back(buildNode(part, num_pieces, node.kind == Kind::Array ? 0 : rows, allow_combining));
            node.needs_sizes = node.needs_sizes || node.children.back().needs_sizes;
        });
    return node;
}

/// Points the node at `column`, the source that is sized or scattered next.
void bindSource(Node & node, const IColumn & column)
{
    if (node.kind == Kind::Fixed)
    {
        node.data = column.getRawData().data();
    }
    else if (node.kind == Kind::String)
    {
        const auto & string = assert_cast<const ColumnString &>(column);
        node.data = reinterpret_cast<const char *>(string.getChars().data());
        node.offsets = string.getOffsets().data();
    }
    else if (node.kind == Kind::Array)
    {
        node.offsets = assert_cast<const ColumnArray &>(column).getOffsets().data();
    }
    forEachChild(node.kind, column, [&](size_t i, const IColumn & part) { bindSource(node.children[i], part); });
}

/// Adds the sizes of rows `[begin, end)` of the source to piece `piece`.
void sizeRange(Node & node, size_t begin, size_t end, size_t piece)
{
    switch (node.kind)
    {
        case Kind::String:
            node.sizes[piece] += startOf(node.offsets, end) - startOf(node.offsets, begin);
            return;
        case Kind::Array:
        {
            const UInt64 first = startOf(node.offsets, begin);
            const UInt64 last = startOf(node.offsets, end);
            node.sizes[piece] += last - first;
            if (node.children[0].needs_sizes)
                sizeRange(node.children[0], first, last, piece);
            return;
        }
        case Kind::Nullable:
        case Kind::Tuple:
            for (Node & child : node.children)
                if (child.needs_sizes)
                    sizeRange(child, begin, end, piece);
            return;
        case Kind::Fixed:
            return;
        case Kind::None:
            UNREACHABLE();
    }
}

/// Adds the sizes of the rows of the source to their pieces.
template <typename Id>
void sizeRows(Node & node, const Id * ids, size_t n)
{
    switch (node.kind)
    {
        case Kind::String:
        {
            size_t * sizes = node.sizes.data();
            const UInt64 * offsets = node.offsets;
            UInt64 prev = 0;
            for (size_t i = 0; i < n; ++i)
            {
                const UInt64 end = offsets[i];
                sizes[ids[i]] += end - prev;
                prev = end;
            }
            return;
        }
        case Kind::Array:
        {
            Node & elements = node.children[0];
            const bool size_elements = elements.needs_sizes;
            size_t * sizes = node.sizes.data();
            const UInt64 * offsets = node.offsets;
            UInt64 prev = 0;
            for (size_t i = 0; i < n; ++i)
            {
                const UInt64 end = offsets[i];
                sizes[ids[i]] += end - prev;
                if (size_elements)
                    sizeRange(elements, prev, end, ids[i]);
                prev = end;
            }
            return;
        }
        case Kind::Nullable:
        case Kind::Tuple:
            for (Node & child : node.children)
                if (child.needs_sizes)
                    sizeRows(child, ids, n);
            return;
        case Kind::Fixed:
            return;
        case Kind::None:
            UNREACHABLE();
    }
}

/// Appends `rows` uninitialized values to an empty fixed-width column and returns them.
std::span<char> growUninitialized(IColumn & column, size_t rows)
{
    /// `insertRawUninitialized` alone would grow by powers of two.
    column.reserve(rows);
    return column.insertRawUninitialized(rows);
}

/// `column` is the part of piece `piece` that `node` stands for. Sizes every array of `column` for its `rows` rows and
/// seeds the cursors of the piece.
void preparePiece(Node & node, IColumn & column, size_t piece, size_t rows)
{
    /// A piece with no rows keeps what `cloneEmpty` gave it.
    if (rows == 0)
        return;

    size_t child_rows = rows;
    switch (node.kind)
    {
        case Kind::Fixed:
        {
            char * cursor = growUninitialized(column, rows).data();
            /// The staging lines of `StoreMode::Combined` fill to 64 bytes only from a cursor that is a multiple of the width.
            chassert(node.scratch.mode() != StoreMode::Combined || reinterpret_cast<uintptr_t>(cursor) % node.width == 0);
            node.scratch.seed(piece, cursor);
            break;
        }
        case Kind::String:
        {
            auto & string = assert_cast<ColumnString &>(column);
            string.getChars().resize_exact(node.sizes[piece]);
            string.getOffsets().resize_exact(rows);
            node.string_cursors[piece] = {reinterpret_cast<char *>(string.getChars().data()), string.getOffsets().data(), 0, 0};
            break;
        }
        case Kind::Tuple:
            assert_cast<ColumnTuple &>(column).addSize(rows);
            break;
        case Kind::Array:
        {
            auto & offsets = assert_cast<ColumnArray &>(column).getOffsets();
            offsets.resize_exact(rows);
            node.array_cursors[piece] = {offsets.data(), 0};
            child_rows = node.sizes[piece];
            break;
        }
        case Kind::Nullable:
        case Kind::None:
            break;
    }
    forEachChild(node.kind, column, [&](size_t i, IColumn & part) { preparePiece(node.children[i], part, piece, child_rows); });
}

/// Copies `width` bytes and writes nothing past `dst + width`. The last copy may rewrite bytes of the row with the same
/// values.
ALWAYS_INLINE void copyRowExact(char * __restrict dst, const char * __restrict src, size_t width)
{
    /// Fixed-size copies, because a `memcpy` of a runtime size is a libc call per row.
    if (width >= 16)
    {
        /// A 32-byte stride, so that the backend still pairs the loads and the stores (ldp/stp on AArch64). The barriers
        /// stop the loop-idiom pass from turning the loop back into a `memcpy` call.
        size_t i = 0;
        for (; i + 32 <= width; i += 32)
        {
            __builtin_memcpy_inline(dst + i, src + i, 32);
            __asm__ __volatile__("" : : : "memory");
        }
        if (i + 16 <= width)
        {
            __builtin_memcpy_inline(dst + i, src + i, 16);
            __asm__ __volatile__("" : : : "memory");
            i += 16;
        }
        if (i != width)
            __builtin_memcpy_inline(dst + width - 16, src + width - 16, 16);
    }
    else if (width >= 8)
    {
        __builtin_memcpy_inline(dst, src, 8);
        __builtin_memcpy_inline(dst + width - 8, src + width - 8, 8);
    }
    else if (width >= 4)
    {
        __builtin_memcpy_inline(dst, src, 4);
        __builtin_memcpy_inline(dst + width - 4, src + width - 4, 4);
    }
    else if (width >= 2)
    {
        __builtin_memcpy_inline(dst, src, 2);
        __builtin_memcpy_inline(dst + width - 2, src + width - 2, 2);
    }
    else if (width == 1)
    {
        dst[0] = src[0];
    }
}

template <size_t width, typename Id>
void scatterFixedRowsOfWidth(const Id * ids, const char * data, size_t n, char ** cursors)
{
    for (size_t i = 0; i < n; ++i)
    {
        const size_t piece = ids[i];
        char * dst = cursors[piece];
        __builtin_memcpy_inline(dst, data + i * width, width);
        cursors[piece] = dst + width;
    }
}

template <typename Id>
void scatterFixedRowsOfAnyWidth(size_t width, const Id * ids, const char * data, size_t n, char ** cursors)
{
    for (size_t i = 0; i < n; ++i)
    {
        const size_t piece = ids[i];
        char * dst = cursors[piece];
        copyRowExact(dst, data + i * width, width);
        cursors[piece] = dst + width;
    }
}

/// Copies row i of `data` to the cursor of piece `ids[i]`, writing exactly `width` bytes per row.
template <typename Id>
/// Not inlined into `scatterRows`: there, the loops of the widths 3, 5, 6 and 7 with `UInt64` ids take half an
/// instruction more per row on x86-64.
NO_INLINE void scatterFixedRows(size_t width, const Id * ids, const char * data, size_t n, char ** cursors)
{
    switch (width)
    {
        case 1: scatterFixedRowsOfWidth<1>(ids, data, n, cursors); return;
        case 2: scatterFixedRowsOfWidth<2>(ids, data, n, cursors); return;
        case 4: scatterFixedRowsOfWidth<4>(ids, data, n, cursors); return;
        case 8: scatterFixedRowsOfWidth<8>(ids, data, n, cursors); return;
        case 16: scatterFixedRowsOfWidth<16>(ids, data, n, cursors); return;
        case 32: scatterFixedRowsOfWidth<32>(ids, data, n, cursors); return;
        default: scatterFixedRowsOfAnyWidth(width, ids, data, n, cursors); return;
    }
}

bool combiningSupportsWidth(size_t width)
{
    return width == 1 || width == 2 || width == 4 || width == 8 || width == 16;
}

using Line = char __attribute__((vector_size(LINE_BYTES)));

/// Stages row i in the line of piece `ids[i]`, and stores the line when it is full. Writes no byte outside the rows of a piece.
template <size_t width, typename Id>
void scatterFixedRowsCombinedOfWidth(const Id * ids, const char * data, size_t n, char * staging, char ** cursors, UInt32 * fill)
{
    for (size_t i = 0; i < n; ++i)
    {
        const size_t piece = ids[i];
        char * line = staging + piece * LINE_BYTES;
        UInt32 filled = fill[piece];
        __builtin_memcpy_inline(line + filled, data + i * width, width);
        filled += width;
        if (filled == LINE_BYTES)
        {
            char * cursor = cursors[piece];
            const UInt32 misalignment = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cursor) & (LINE_BYTES - 1));
            if (misalignment) /// The first line of a piece starts at its cursor, so it takes regular stores.
            {
                __builtin_memcpy(cursor, line + misalignment, LINE_BYTES - misalignment);
                cursors[piece] = cursor + (LINE_BYTES - misalignment);
            }
            else
            {
                __builtin_nontemporal_store(*reinterpret_cast<const Line *>(line), reinterpret_cast<Line *>(cursor));
                cursors[piece] = cursor + LINE_BYTES;
            }
            filled = 0;
        }
        fill[piece] = filled;
    }
}

/// Precondition: `combiningSupportsWidth(width)`, and every cursor is a multiple of `width`.
template <typename Id>
/// Not inlined into `scatterRows`: there, the loop of the width 16 with `UInt64` ids takes one more spill on x86-64.
NO_INLINE void scatterFixedRowsCombined(size_t width, const Id * ids, const char * data, size_t n, ScatterScratch & scratch)
{
    /// Passed as arguments: the non-temporal store through `char *` defeats type-based alias analysis, so the loop would
    /// otherwise reload the members of `scratch` on every row.
    char * const staging = scratch.lines();
    char ** const cursors = scratch.cursors.data();
    UInt32 * const fill = scratch.fills();
    switch (width)
    {
        case 1: scatterFixedRowsCombinedOfWidth<1>(ids, data, n, staging, cursors, fill); return;
        case 2: scatterFixedRowsCombinedOfWidth<2>(ids, data, n, staging, cursors, fill); return;
        case 4: scatterFixedRowsCombinedOfWidth<4>(ids, data, n, staging, cursors, fill); return;
        case 8: scatterFixedRowsCombinedOfWidth<8>(ids, data, n, staging, cursors, fill); return;
        case 16: scatterFixedRowsCombinedOfWidth<16>(ids, data, n, staging, cursors, fill); return;
        default: throw Exception(ErrorCodes::LOGICAL_ERROR, "Write combining does not support rows of {} bytes", width);
    }
}

/// Copies the chars of each row to its piece and appends its offset, rebased on the piece. Reads and writes up to 15
/// bytes past a row. The source and the chars of every piece are padded, and the next row of a piece overwrites the
/// bytes written past the previous one.
template <typename Id>
void scatterStringRows(const Id * ids, const char * chars, const UInt64 * offsets, size_t n, StringCursor * cursors)
{
    UInt64 prev = 0;
    for (size_t i = 0; i < n; ++i)
    {
        const UInt64 end = offsets[i];
        const UInt64 length = end - prev;
        StringCursor & cursor = cursors[ids[i]];
        memcpySmallAllowReadWriteOverflow15(cursor.chars, chars + prev, length);
        cursor.chars += length;
        cursor.last_offset += length;
        *cursor.offsets = cursor.last_offset;
        ++cursor.offsets;
        prev = end;
    }
}

/// Appends the offsets of rows `[begin, end)` to a piece, rebased on its last offset.
ALWAYS_INLINE void appendOffsets(const UInt64 * offsets, size_t begin, size_t end, UInt64 *& cursor, UInt64 & last_offset)
{
    const UInt64 first = startOf(offsets, begin);
    const UInt64 base = last_offset;
    UInt64 * dst = cursor;
    for (size_t row = begin; row < end; ++row)
        *dst++ = base + (offsets[row] - first);
    cursor = dst;
    last_offset = base + (startOf(offsets, end) - first);
}

/// Copies rows `[begin, end)` of `width` bytes each to piece `piece`. Reads and writes up to 15 bytes past the range,
/// as `scatterStringRows` does.
ALWAYS_INLINE void copyFixedRange(const char * data, size_t width, char ** cursors, size_t begin, size_t end, size_t piece)
{
    char * dst = cursors[piece];
    const size_t bytes = (end - begin) * width;
    memcpySmallAllowReadWriteOverflow15(dst, data + begin * width, bytes);
    cursors[piece] = dst + bytes;
}

/// Copies strings `[begin, end)` to piece `piece`. Reads and writes up to 15 bytes past the chars, as
/// `scatterStringRows` does.
ALWAYS_INLINE void
copyStringRange(const char * chars, const UInt64 * offsets, StringCursor * cursors, size_t begin, size_t end, size_t piece)
{
    StringCursor & cursor = cursors[piece];
    const UInt64 first = startOf(offsets, begin);
    const UInt64 bytes = startOf(offsets, end) - first;
    memcpySmallAllowReadWriteOverflow15(cursor.chars, chars + first, bytes);
    cursor.chars += bytes;
    appendOffsets(offsets, begin, end, cursor.offsets, cursor.last_offset);
}

/// Copies rows `[begin, end)` of the source to piece `piece`. The source is a nested column of an `Array`.
void copyRange(Node & node, size_t begin, size_t end, size_t piece)
{
    switch (node.kind)
    {
        case Kind::Fixed:
            copyFixedRange(node.data, node.width, node.scratch.cursors.data(), begin, end, piece);
            return;
        case Kind::String:
            copyStringRange(node.data, node.offsets, node.string_cursors.data(), begin, end, piece);
            return;
        case Kind::Array:
        {
            ArrayCursor & cursor = node.array_cursors[piece];
            appendOffsets(node.offsets, begin, end, cursor.offsets, cursor.last_offset);
            copyRange(node.children[0], startOf(node.offsets, begin), startOf(node.offsets, end), piece);
            return;
        }
        case Kind::Nullable:
        case Kind::Tuple:
            for (Node & child : node.children)
                copyRange(child, begin, end, piece);
            return;
        case Kind::None:
            UNREACHABLE();
    }
}

/// Appends the offset of each row to its piece, rebased on the piece, and copies the row's elements with
/// `copy_elements(begin, end, piece)`.
template <typename Id, typename CopyElements>
void scatterArrayRowsWith(const Id * ids, const UInt64 * offsets, size_t n, ArrayCursor * cursors, CopyElements copy_elements)
{
    UInt64 prev = 0;
    for (size_t i = 0; i < n; ++i)
    {
        const size_t piece = ids[i];
        const UInt64 end = offsets[i];
        ArrayCursor & cursor = cursors[piece];
        cursor.last_offset += end - prev;
        *cursor.offsets = cursor.last_offset;
        ++cursor.offsets;
        copy_elements(prev, end, piece);
        prev = end;
    }
}

template <typename Id>
void scatterArrayRows(Node & node, const Id * ids, size_t n)
{
    Node & elements = node.children[0];
    ArrayCursor * cursors = node.array_cursors.data();
    switch (elements.kind)
    {
        /// Fixed and `String` elements get a loop with the copy inlined, without a `copyRange` call per row.
        case Kind::Fixed:
            scatterArrayRowsWith(
                ids,
                node.offsets,
                n,
                cursors,
                [data = elements.data, width = elements.width, element_cursors = elements.scratch.cursors.data()](
                    size_t begin, size_t end, size_t piece) { copyFixedRange(data, width, element_cursors, begin, end, piece); });
            return;
        case Kind::String:
            scatterArrayRowsWith(
                ids,
                node.offsets,
                n,
                cursors,
                [chars = elements.data, offsets = elements.offsets, element_cursors = elements.string_cursors.data()](
                    size_t begin, size_t end, size_t piece) { copyStringRange(chars, offsets, element_cursors, begin, end, piece); });
            return;
        case Kind::Nullable:
        case Kind::Tuple:
        case Kind::Array:
        case Kind::None:
            scatterArrayRowsWith(
                ids,
                node.offsets,
                n,
                cursors,
                [&elements](size_t begin, size_t end, size_t piece) { copyRange(elements, begin, end, piece); });
            return;
    }
}

/// Stores the rows that are still staged in the lines of the fixed-width columns of `node`.
void drainNode(Node & node)
{
    if (node.scratch.mode() == StoreMode::Combined)
        node.scratch.drain();
    for (Node & child : node.children)
        drainNode(child);
}

/// Scatters the rows of the source to the cursors of their pieces.
template <typename Id>
void scatterRows(Node & node, const Id * ids, size_t n)
{
    switch (node.kind)
    {
        case Kind::Fixed:
            if (node.scratch.mode() == StoreMode::Combined)
                scatterFixedRowsCombined(node.width, ids, node.data, n, node.scratch);
            else
                scatterFixedRows(node.width, ids, node.data, n, node.scratch.cursors.data());
            return;
        case Kind::String:
            scatterStringRows(ids, node.data, node.offsets, n, node.string_cursors.data());
            return;
        case Kind::Array:
            scatterArrayRows(node, ids, n);
            return;
        case Kind::Nullable:
        case Kind::Tuple:
            for (Node & child : node.children)
                scatterRows(child, ids, n);
            return;
        case Kind::None:
            UNREACHABLE();
    }
}

/// Scatters the sources with the typed kernels. Precondition: `hasKernel(*sources[0])`. Throws `LOGICAL_ERROR` when
/// another source does not have the structure of the first one.
template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatterWithKernels(
    std::span<const IColumn * const> sources,
    std::span<const std::span<const Id>> ids,
    std::span<const size_t> counts,
    bool allow_combining)
{
    const IColumn & sample = *sources[0];
    const size_t num_pieces = counts.size();
    Node root = buildNode(sample, num_pieces, std::accumulate(counts.begin(), counts.end(), size_t{0}), allow_combining);

    /// The pieces are clones of source 0, so another class, width, tuple size or `Decimal` scale would misread rows.
    for (size_t b = 1; b < sources.size(); ++b)
        if (!sources[b]->structureEquals(sample))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Source column {} ({}) does not have the structure of source 0 ({})",
                b,
                sources[b]->getName(),
                sample.getName());

    if (root.needs_sizes)
    {
        for (size_t b = 0; b < sources.size(); ++b)
        {
            bindSource(root, *sources[b]);
            sizeRows(root, ids[b].data(), ids[b].size());
        }
    }

    VectorWithMemoryTracking<MutableColumnPtr> pieces(num_pieces);
    for (size_t piece = 0; piece < num_pieces; ++piece)
    {
        pieces[piece] = sample.cloneEmpty();
        preparePiece(root, *pieces[piece], piece, counts[piece]);
    }

    for (size_t b = 0; b < sources.size(); ++b)
    {
        bindSource(root, *sources[b]);
        scatterRows(root, ids[b].data(), ids[b].size());
    }
    drainNode(root);
    return pieces;
}

/// True if every source is a `Const` and all their values serialize to the same bytes, so `+0.0` and `-0.0` differ.
bool allConstWithSameValue(std::span<const IColumn * const> sources)
{
    if (!std::ranges::all_of(sources, [](const IColumn * source) { return source->isConst(); }))
        return false;

    const IColumn & first = assert_cast<const ColumnConst &>(*sources[0]).getDataColumn();
    /// A `ColumnFunction` cannot be serialized.
    if (first.getDataType() == TypeIndex::Function)
        return false;

    Arena arena;
    const char * begin = nullptr;
    const std::string_view value = first.serializeValueIntoArena(0, arena, begin, nullptr);
    return std::ranges::all_of(
        sources.subspan(1),
        [&](const IColumn * source)
        {
            const char * source_begin = nullptr;
            return assert_cast<const ColumnConst &>(*source).getDataColumn().serializeValueIntoArena(0, arena, source_begin, nullptr)
                == value;
        });
}

/// Throws `LOGICAL_ERROR` when the sources differ in `TypeIndex`. `Const`, `Sparse` and `Replicated` report the type of
/// the column they hold, so a wrapped source matches a full source of that type.
void checkSameDataType(std::span<const IColumn * const> sources)
{
    for (size_t b = 1; b < sources.size(); ++b)
        if (sources[b]->getDataType() != sources[0]->getDataType())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Source column {} has type {}, but source 0 has type {}",
                b,
                sources[b]->getName(),
                sources[0]->getName());
}

template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatterSeveral(
    std::span<const IColumn * const> sources,
    std::span<const std::span<const Id>> ids,
    std::span<const size_t> counts,
    bool allow_combining);

/// Several sources of a `Tuple`, one element at a time.
template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatterTupleElements(
    std::span<const IColumn * const> sources,
    std::span<const std::span<const Id>> ids,
    std::span<const size_t> counts,
    bool allow_combining)
{
    const size_t num_elements = assert_cast<const ColumnTuple &>(*sources[0]).tupleSize();
    for (size_t b = 1; b < sources.size(); ++b)
        if (assert_cast<const ColumnTuple &>(*sources[b]).tupleSize() != num_elements)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Source column {} is a tuple of {} elements, but source 0 has {}",
                b,
                assert_cast<const ColumnTuple &>(*sources[b]).tupleSize(),
                num_elements);

    VectorWithMemoryTracking<MutableColumnPtr> pieces(counts.size());
    if (num_elements == 0)
    {
        for (size_t piece = 0; piece < counts.size(); ++piece)
            pieces[piece] = ColumnTuple::create(counts[piece]);
        return pieces;
    }

    VectorWithMemoryTracking<VectorWithMemoryTracking<MutableColumnPtr>> element_pieces(num_elements);
    ColumnRawPtrs element_sources(sources.size());
    for (size_t element = 0; element < num_elements; ++element)
    {
        for (size_t b = 0; b < sources.size(); ++b)
            element_sources[b] = &assert_cast<const ColumnTuple &>(*sources[b]).getColumn(element);
        element_pieces[element] = scatterSeveral(element_sources, ids, counts, allow_combining);
    }

    for (size_t piece = 0; piece < counts.size(); ++piece)
    {
        MutableColumns elements(num_elements);
        for (size_t element = 0; element < num_elements; ++element)
            elements[element] = std::move(element_pieces[element][piece]);
        pieces[piece] = ColumnTuple::create(std::move(elements));
    }
    return pieces;
}

/// Several sources of a `Map`, through their nested `Array` columns, as `ColumnMap::scatter` does with one.
template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatterMapNested(
    std::span<const IColumn * const> sources,
    std::span<const std::span<const Id>> ids,
    std::span<const size_t> counts,
    bool allow_combining)
{
    ColumnRawPtrs nested(sources.size());
    for (size_t b = 0; b < sources.size(); ++b)
        nested[b] = &assert_cast<const ColumnMap &>(*sources[b]).getNestedColumn();
    auto pieces = scatterSeveral(nested, ids, counts, allow_combining);
    const auto & statistics = assert_cast<const ColumnMap &>(*sources[0]).getStatistics();
    for (auto & piece : pieces)
        piece = ColumnMap::create(std::move(piece), statistics);
    return pieces;
}

/// Each source's own `scatter`, joined with `insertRangeFrom`.
template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatterEachSource(
    std::span<const IColumn * const> sources, std::span<const std::span<const Id>> ids, std::span<const size_t> counts)
{
    VectorWithMemoryTracking<MutableColumnPtr> pieces(counts.size());
    for (size_t piece = 0; piece < counts.size(); ++piece)
    {
        pieces[piece] = sources[0]->cloneEmpty();
        if (counts[piece] > 0)
            pieces[piece]->reserve(counts[piece]);
    }

    PaddedPODArray<Id> selector;
    for (size_t b = 0; b < sources.size(); ++b)
    {
        selector.assign(ids[b].data(), ids[b].data() + ids[b].size());
        const auto parts = sources[b]->scatter(counts.size(), selector);
        for (size_t piece = 0; piece < counts.size(); ++piece)
            if (!parts[piece]->empty())
                pieces[piece]->insertRangeFrom(*parts[piece], 0, parts[piece]->size());
    }
    return pieces;
}

/// Several sources with no wrappers.
template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatterSeveral(
    std::span<const IColumn * const> sources,
    std::span<const std::span<const Id>> ids,
    std::span<const size_t> counts,
    bool allow_combining)
{
    checkSameDataType(sources);
    const IColumn & sample = *sources[0];

    if (kernelKind(sample) == Kind::Tuple)
        return scatterTupleElements(sources, ids, counts, allow_combining);
    if (typeid_cast<const ColumnMap *>(&sample))
        return scatterMapNested(sources, ids, counts, allow_combining);
    if (accepts(sample, false))
        return scatterWithKernels(sources, ids, counts, allow_combining);

    return scatterEachSource(sources, ids, counts);
}

template <typename Id, typename Counter>
void histogramImpl(const Id * ids, size_t n, Counter * hist, Counter * lanes, size_t fanout)
{
    if (!lanes)
    {
        for (size_t i = 0; i < n; ++i)
            ++hist[ids[i]];
        return;
    }

    /// Four consecutive rows increment counters in four different lanes. This breaks the load-increment-store
    /// dependency chain between rows with the same id.
    size_t i = 0;
    for (; i + 4 <= n; i += 4)
    {
        ++lanes[0 * fanout + ids[i + 0]];
        ++lanes[1 * fanout + ids[i + 1]];
        ++lanes[2 * fanout + ids[i + 2]];
        ++lanes[3 * fanout + ids[i + 3]];
    }
    for (; i < n; ++i)
        ++lanes[(i & 3) * fanout + ids[i]];
}

template <typename Counter>
void reduceLanesImpl(Counter * hist, const Counter * lanes, size_t fanout)
{
    for (size_t piece = 0; piece < fanout; ++piece)
        hist[piece] += lanes[piece] + lanes[fanout + piece] + lanes[2 * fanout + piece] + lanes[3 * fanout + piece];
}

}

bool hasKernel(const IColumn & column)
{
    return accepts(column, false);
}

template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatter(
    const IColumn & column, const PaddedPODArray<Id> & selector, std::span<const size_t> counts)
{
    const std::array<const IColumn *, 1> sources{&column};
    const std::array<std::span<const Id>, 1> ids{std::span<const Id>(selector.data(), selector.size())};
    return scatterWithKernels<Id>(sources, ids, counts, false);
}

template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatter(
    std::span<const IColumn * const> sources,
    std::span<const std::span<const Id>> ids,
    std::span<const size_t> counts,
    bool allow_combining)
{
    checkPieceCount<Id>(counts.size());
    if (sources.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot scatter an empty list of source columns");
    if (sources.size() != ids.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Cannot scatter {} source columns with {} spans of piece ids", sources.size(), ids.size());

    size_t rows = 0;
    for (size_t b = 0; b < sources.size(); ++b)
    {
        if (sources[b]->size() != ids[b].size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR, "Source column {} has {} rows, but {} piece ids", b, sources[b]->size(), ids[b].size());
        rows += ids[b].size();
    }

    size_t counted = 0;
    for (size_t count : counts)
        counted += count;
    if (counted != rows)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Scatter counts sum to {}, but the sources have {} rows", counted, rows);

#if defined(DEBUG_OR_SANITIZER_BUILD)
    if (!std::ranges::equal(countRowsPerPiece(ids, counts.size()), counts))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Scatter counts do not match the piece ids");
#endif

    if (sources.size() == 1)
    {
        if (hasKernel(*sources[0]))
            return scatterWithKernels(sources, ids, counts, allow_combining);
        const PaddedPODArray<Id> selector(ids[0].data(), ids[0].data() + ids[0].size());
        return sources[0]->scatterWithCounts(selector, counts);
    }

    checkSameDataType(sources);
    if (allConstWithSameValue(sources))
    {
        VectorWithMemoryTracking<MutableColumnPtr> pieces(counts.size());
        for (size_t piece = 0; piece < counts.size(); ++piece)
            pieces[piece] = sources[0]->cloneResized(counts[piece]);
        return pieces;
    }

    Columns holders;
    ColumnRawPtrs normalized;
    holders.reserve(sources.size());
    normalized.reserve(sources.size());
    for (const IColumn * source : sources)
    {
        holders.push_back(source->convertToFullIfWrapped());
        normalized.push_back(holders.back().get());
    }
    return scatterSeveral<Id>(normalized, ids, counts, allow_combining);
}

template <typename Id>
VectorWithMemoryTracking<size_t> countRowsPerPiece(std::span<const std::span<const Id>> ids, size_t num_pieces)
{
    checkPieceCount<Id>(num_pieces);

    /// The counting below writes through raw pointers, so an out-of-range id must throw first.
    for (const auto & source_ids : ids)
    {
        const Id * data = source_ids.data();
        Id max_id = 0;
        for (size_t i = 0; i < source_ids.size(); ++i)
            max_id = std::max(max_id, data[i]);
        if (!source_ids.empty() && static_cast<size_t>(max_id) >= num_pieces)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Piece id {} is out of range for {} pieces", max_id, num_pieces);
    }

    VectorWithMemoryTracking<size_t> counts(num_pieces);
    if (num_pieces <= HIST_INTERLEAVE_MAX_FANOUT)
    {
        PaddedPODArray<size_t> lanes;
        lanes.resize_fill(4 * num_pieces, 0);
        for (const auto & source_ids : ids)
            histogramImpl(source_ids.data(), source_ids.size(), counts.data(), lanes.data(), num_pieces);
        reduceLanesImpl(counts.data(), lanes.data(), num_pieces);
    }
    else
    {
        for (const auto & source_ids : ids)
            histogramImpl<Id, size_t>(source_ids.data(), source_ids.size(), counts.data(), nullptr, num_pieces);
    }
    return counts;
}

template VectorWithMemoryTracking<MutableColumnPtr>
    scatter<UInt16>(const IColumn &, const PaddedPODArray<UInt16> &, std::span<const size_t>);
template VectorWithMemoryTracking<MutableColumnPtr>
    scatter<UInt64>(const IColumn &, const PaddedPODArray<UInt64> &, std::span<const size_t>);
template VectorWithMemoryTracking<MutableColumnPtr>
    scatter<UInt16>(std::span<const IColumn * const>, std::span<const std::span<const UInt16>>, std::span<const size_t>, bool);
template VectorWithMemoryTracking<MutableColumnPtr>
    scatter<UInt64>(std::span<const IColumn * const>, std::span<const std::span<const UInt64>>, std::span<const size_t>, bool);
template VectorWithMemoryTracking<size_t> countRowsPerPiece<UInt16>(std::span<const std::span<const UInt16>>, size_t);
template VectorWithMemoryTracking<size_t> countRowsPerPiece<UInt64>(std::span<const std::span<const UInt64>>, size_t);

VectorWithMemoryTracking<size_t> computePassBits(size_t fanout, size_t max_fanout_per_pass)
{
    VectorWithMemoryTracking<size_t> result;
    if (fanout <= 1)
        return result;

    chassert(std::has_single_bit(fanout));
    const size_t total_bits = static_cast<size_t>(std::countr_zero(fanout));
    const size_t max_bits = static_cast<size_t>(std::countr_zero(std::bit_floor(std::max<size_t>(2, max_fanout_per_pass))));
    const size_t num_passes = (total_bits + max_bits - 1) / max_bits;
    const size_t bits_per_pass = (total_bits + num_passes - 1) / num_passes;
    for (size_t remaining = total_bits; remaining > 0;)
    {
        const size_t bits = std::min(bits_per_pass, remaining);
        result.push_back(bits);
        remaining -= bits;
    }
    return result;
}

namespace
{

const char * storeModeName(StoreMode mode)
{
    return mode == StoreMode::Combined ? "combined" : "direct";
}

}

StoreMode chooseStoreMode(size_t width, size_t pieces, size_t rows, bool allow_combining)
{
    if (!allow_combining || !COMBINING_ENABLED || !combiningSupportsWidth(width) || pieces < COMBINING_MIN_PIECES || pieces > COMBINING_MAX_PIECES)
        return StoreMode::Direct;
    return rows >= pieces * COMBINING_MIN_LINES_PER_PIECE * (LINE_BYTES / width) ? StoreMode::Combined : StoreMode::Direct;
}

StoreMode ScatterScratch::mode() const
{
    if (!hasStaging())
        return StoreMode::Direct;
    StoreMode result = StoreMode::Direct;
    memcpy(&result, spare(), sizeof(result));
    return result;
}

/// Layout of the bytes after the cursors, when there are staging lines: the mode in the first 8 bytes, the fill of each
/// piece, then the staging lines from the next multiple of 64 bytes.
UInt32 * ScatterScratch::fills()
{
    chassert(hasStaging());
    return reinterpret_cast<UInt32 *>(spare() + sizeof(char *));
}

char * ScatterScratch::lines()
{
    chassert(hasStaging());
    const uintptr_t end_of_fills = reinterpret_cast<uintptr_t>(fills() + cursors.size());
    return reinterpret_cast<char *>((end_of_fills + LINE_BYTES - 1) & ~static_cast<uintptr_t>(LINE_BYTES - 1));
}

void ScatterScratch::init(size_t fanout, StoreMode mode_)
{
    /// Allocated exactly: a `Direct` scratch takes no more than its cursors, and a `Combined` one no more than its
    /// cursors, fills and lines, plus up to 63 bytes to align the first line.
    size_t spare_bytes = 0;
    if (mode_ == StoreMode::Combined)
        spare_bytes = sizeof(char *) + fanout * sizeof(UInt32) + LINE_BYTES - 1 + fanout * LINE_BYTES;
    const size_t elements = fanout + (spare_bytes + sizeof(char *) - 1) / sizeof(char *);
    if (cursors.capacity() != elements)
    {
        PaddedPODArray<char *> exact;
        exact.reserve_exact(elements);
        cursors.swap(exact);
    }
    cursors.clear();
    cursors.resize_fill(fanout, nullptr);
    if (mode_ == StoreMode::Combined)
    {
        memset(spare(), 0, sizeof(char *) + fanout * sizeof(UInt32));
        memcpy(spare(), &mode_, sizeof(mode_));
    }
}

void ScatterScratch::setStoreMode(StoreMode mode_)
{
    if (!hasStaging())
    {
        if (mode_ == StoreMode::Combined)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot combine writes: the scratch was initialized without staging lines");
        return;
    }
    /// `drain` has stored the staged rows of the last column.
    chassert(std::all_of(fills(), fills() + cursors.size(), [](UInt32 filled) { return filled == 0; }));
    memcpy(spare(), &mode_, sizeof(mode_));
}

void ScatterScratch::seed(size_t piece, char * cursor)
{
    cursors[piece] = cursor;
    if (mode() == StoreMode::Combined)
        fills()[piece] = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cursor) & (LINE_BYTES - 1));
}

void ScatterScratch::drain()
{
    if (mode() == StoreMode::Combined)
    {
        UInt32 * const fill = fills();
        char * const staging = lines();
        for (size_t piece = 0; piece < cursors.size(); ++piece)
        {
            const UInt32 filled = fill[piece];
            if (!filled)
                continue;
            char * cursor = cursors[piece];
            const UInt32 start = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cursor) & (LINE_BYTES - 1));
            if (filled > start)
                memcpy(cursor, staging + piece * LINE_BYTES + start, filled - start);
            fill[piece] = 0;
        }
        /// Non-temporal stores are weakly ordered.
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }
    /// The cursors of this column must not reach the next one: a stale cursor of a piece that gets no rows there could
    /// fail the alignment check of `scatterPidChunk`.
    std::fill(cursors.begin(), cursors.end(), nullptr);
}

void scatterPidChunk(size_t width, const UInt16 * pids, const char * data, size_t n, StoreMode mode, ScatterScratch & scratch)
{
    if (mode != scratch.mode())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Cannot scatter a chunk in the {} store mode: the scratch is in the {} store mode",
            storeModeName(mode),
            storeModeName(scratch.mode()));

    if (mode == StoreMode::Direct)
    {
        scatterFixedRows(width, pids, data, n, scratch.cursors.data());
        return;
    }

    if (!combiningSupportsWidth(width))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Write combining does not support rows of {} bytes", width);
    /// A line of staged rows must fill to exactly 64 bytes, so every cursor must be a multiple of the row width.
    for (const char * cursor : scratch.cursors)
        if ((reinterpret_cast<uintptr_t>(cursor) & (width - 1)) != 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "A piece cursor is not a multiple of the row width {}", width);
    scatterFixedRowsCombined(width, pids, data, n, scratch);
}

void histogramPidChunk(const UInt16 * pids, size_t n, UInt64 * hist, UInt64 * lanes, size_t fanout)
{
    histogramImpl(pids, n, hist, lanes, fanout);
}

void reduceHistogramLanes(UInt64 * hist, const UInt64 * lanes, size_t fanout)
{
    reduceLanesImpl(hist, lanes, fanout);
}

std::pair<MutableColumnPtr, std::span<char>> allocateUninitializedFixed(const IColumn & sample, size_t rows)
{
    auto column = sample.cloneEmpty();
    const auto raw = growUninitialized(*column, rows);
    return {std::move(column), raw};
}

}

}
