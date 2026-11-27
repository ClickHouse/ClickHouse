#include <Interpreters/HashJoin/KeyGetter.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <base/unaligned.h>
#include <Common/SipHashStrings.h>

#include <algorithm>
#include <bit>
#include <cstring>
#include <typeinfo>


namespace DB
{

namespace
{

/// Without NEON or AVX2 the kernel hashes the rows one by one, so copying them first would gain nothing. On big-endian
/// platforms, `updateHashWithValue` swaps the bytes of a number to little-endian order before it hashes them.
#if (defined(__aarch64__) && defined(__ARM_NEON)) || defined(__AVX2__)
constexpr bool hashes_many_rows = std::endian::native == std::endian::little;
#else
constexpr bool hashes_many_rows = false;
#endif

/// The kernel hashes up to this many rows in one chunk.
constexpr size_t chunk_rows = 256;
/// The most bytes that the `Rows` layout copies for one chunk. A longer row is hashed by `build`, without a copy.
constexpr size_t max_chunk_bytes = 64 * 1024;

template <typename... Columns>
bool isOneOf(const IColumn & column)
{
    return ((typeid(column) == typeid(Columns)) || ...);
}

/// The columns whose `updateHashWithValue` hashes the bytes of the value as they are in the column. The types are
/// listed because a `ColumnConst` or a `ColumnUnique` reports the `isFixedAndContiguous` of its nested column.
bool isNumberColumn(const IColumn & column)
{
    return isOneOf<
        ColumnVector<UInt8>,
        ColumnVector<UInt16>,
        ColumnVector<UInt32>,
        ColumnVector<UInt64>,
        ColumnVector<UInt128>,
        ColumnVector<UInt256>,
        ColumnVector<Int8>,
        ColumnVector<Int16>,
        ColumnVector<Int32>,
        ColumnVector<Int64>,
        ColumnVector<Int128>,
        ColumnVector<Int256>,
        ColumnVector<BFloat16>,
        ColumnVector<Float32>,
        ColumnVector<Float64>,
        ColumnVector<UUID>,
        ColumnVector<IPv4>,
        ColumnVector<IPv6>,
        ColumnDecimal<Decimal32>,
        ColumnDecimal<Decimal64>,
        ColumnDecimal<Decimal128>,
        ColumnDecimal<Decimal256>,
        ColumnDecimal<DateTime64>,
        ColumnDecimal<Time64>>(column);
}

ALWAYS_INLINE void copyValue(UInt8 * __restrict dst, const UInt8 * __restrict src, size_t size)
{
    /// A copy of one of these sizes is a load and a store, not a call to `memcpy`.
    switch (size)
    {
        case 1: memcpy(dst, src, 1); return;
        case 2: memcpy(dst, src, 2); return;
        case 4: memcpy(dst, src, 4); return;
        case 8: memcpy(dst, src, 8); return;
        case 16: memcpy(dst, src, 16); return;
        default: memcpy(dst, src, size); return;
    }
}

template <size_t size, typename RowAt>
void copyValues(UInt8 * __restrict dst, size_t row_size, const UInt8 * __restrict src, RowAt row_at, size_t num_rows)
{
    for (size_t i = 0; i < num_rows; ++i)
        memcpy(dst + i * row_size, src + row_at(i) * size, size);
}

/// Copies the values of `size` bytes of the rows `row_at(0)`, ..., `row_at(num_rows - 1)` of `src` to the rows of
/// `row_size` bytes at `dst`.
template <typename RowAt>
void copyValues(UInt8 * __restrict dst, size_t row_size, const UInt8 * __restrict src, size_t size, RowAt row_at, size_t num_rows)
{
    switch (size)
    {
        case 1: copyValues<1>(dst, row_size, src, row_at, num_rows); return;
        case 2: copyValues<2>(dst, row_size, src, row_at, num_rows); return;
        case 4: copyValues<4>(dst, row_size, src, row_at, num_rows); return;
        case 8: copyValues<8>(dst, row_size, src, row_at, num_rows); return;
        case 16: copyValues<16>(dst, row_size, src, row_at, num_rows); return;
        case 32: copyValues<32>(dst, row_size, src, row_at, num_rows); return;
        default:
            for (size_t i = 0; i < num_rows; ++i)
                memcpy(dst + i * row_size, src + row_at(i) * size, size);
    }
}

}

bool HashedJoinKey::addParts(const IColumn & column, std::vector<Part> & parts)
{
    /// `ColumnTuple::updateHashWithValue` hashes its elements one after another.
    if (const auto * tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        for (const auto & element : tuple->getColumns())
            if (!addParts(*element, parts))
                return false;
        return true;
    }
    /// `ColumnNullable::updateHashWithValue` hashes the null flag, then the value if it is not NULL. A part has one
    /// flag, so the nested column must be one part without a flag of its own.
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        const size_t nested_part = parts.size();
        if (!addParts(nullable->getNestedColumn(), parts) || parts.size() != nested_part + 1 || parts.back().null_map)
            return false;
        parts.back().null_map = nullable->getNullMapRawDataOrZeros();
        return true;
    }
    if (const auto * string = typeid_cast<const ColumnString *>(&column))
    {
        parts.push_back({string->getChars().data(), 0, string->getOffsets().data()});
        return true;
    }
    if (const auto * fixed_string = typeid_cast<const ColumnFixedString *>(&column))
    {
        parts.push_back({fixed_string->getChars().data(), fixed_string->getN(), nullptr});
        return true;
    }
    if (isNumberColumn(column))
    {
        parts.push_back({reinterpret_cast<const UInt8 *>(column.getRawData().data()), column.sizeOfValueIfFixed(), nullptr});
        return true;
    }
    return false;
}

HashedJoinKey::HashedJoinKey(const ColumnRawPtrs & key_columns_)
    : key_columns(key_columns_)
{
    bool can_copy = hashes_many_rows;
    for (size_t i = 0; can_copy && i < key_columns.size(); ++i)
        can_copy = addParts(*key_columns[i], parts);

    /// An empty `Tuple` has no bytes to copy.
    if (!can_copy || parts.empty())
    {
        parts.clear();
        return;
    }

    bool all_fixed = true;
    for (const auto & part : parts)
    {
        if (part.offsets || part.null_map)
            all_fixed = false;
        else
            fixed_size += part.size;
    }

    if (!all_fixed)
        layout = Layout::Rows;
    else if (parts.size() == 1)
        layout = Layout::Fixed;
    else
        layout = Layout::FixedRows;
}

void HashedJoinKey::buildRange(size_t begin, size_t end, UInt128 * __restrict keys, const UInt8 * skip)
{
    if (layout == Layout::PerRow)
    {
        for (size_t row = begin; row < end; ++row)
            if (!skip[row])
                keys[row - begin] = build(row);
    }
    else
    {
        size_t rows[chunk_rows];
        for (size_t chunk_begin = begin; chunk_begin < end; chunk_begin += chunk_rows)
        {
            const size_t chunk_end = std::min(end, chunk_begin + chunk_rows);
            const size_t chunk_size = chunk_end - chunk_begin;
            UInt128 * chunk_keys = keys + (chunk_begin - begin);
            /// 16 bits, so that the counting loop adds 16-bit lanes.
            UInt16 num_skipped = 0;
            for (size_t row = chunk_begin; row < chunk_end; ++row)
                num_skipped += skip[row] != 0;

            /// The kernel hashes every row it is given. When few rows are skipped, hashing them too costs less than
            /// listing the other rows.
            if (num_skipped * 8 < chunk_size)
            {
                auto row_at = [&](size_t i) { return chunk_begin + i; };
                if (layout == Layout::Fixed)
                    sipHash128PaddedFixedStrings(
                        reinterpret_cast<const char *>(parts[0].data + chunk_begin * fixed_size), fixed_size, chunk_size, chunk_keys);
                else if (layout == Layout::FixedRows)
                    buildFixedRows(row_at, chunk_size, chunk_keys);
                else
                    buildRows(row_at, chunk_size, chunk_keys);
                continue;
            }

            size_t num_rows = 0;
            for (size_t row = chunk_begin; row < chunk_end; ++row)
            {
                rows[num_rows] = row;
                num_rows += !skip[row];
            }
            auto row_at = [&](size_t i) { return rows[i]; };
            if (layout == Layout::Rows)
                buildRows(row_at, num_rows, chunk_keys);
            else
                buildFixedRows(row_at, num_rows, chunk_keys);

            /// Moves the key of `rows[i]` from `chunk_keys[i]` to its row. A key moves to the same or a later position, so
            /// the keys are moved from the last one on.
            for (size_t i = num_rows; i > 0; --i)
                chunk_keys[rows[i - 1] - chunk_begin] = chunk_keys[i - 1];
        }
    }

#if defined(DEBUG_OR_SANITIZER_BUILD)
    for (size_t row = begin; row < end; ++row)
        chassert(skip[row] || keys[row - begin] == build(row));
#endif
}

void HashedJoinKey::markRepeatedRows(size_t begin, size_t end, UInt8 * skip) const
{
    chassert(hashesRowsInBatches());
    if (end - begin < 2)
        return;

    /// `repeated[i]`: row `begin + i` has the bytes that `hash128` hashes for row `begin + i - 1`. Each part is compared for
    /// all the rows, so that the loops over fixed-size values vectorize.
    PaddedPODArray<UInt8> repeated;
    repeated.reserve_exact(end - begin);
    repeated.resize_fill(end - begin, 1);
    repeated[0] = 0;
    UInt8 * __restrict same = repeated.data() - begin;

    auto compare_values = [&]<typename T>(const UInt8 * data, UInt8 * __restrict result)
    {
        for (size_t row = begin + 1; row < end; ++row)
            result[row] &= unalignedLoad<T>(data + row * sizeof(T)) == unalignedLoad<T>(data + (row - 1) * sizeof(T));
    };

    for (const auto & part : parts)
    {
        /// The value of a NULL is not hashed, so only the null flags are compared for it.
        PaddedPODArray<UInt8> values_same;
        UInt8 * __restrict compared = same;
        if (part.null_map)
        {
            values_same.reserve_exact(end - begin);
            values_same.resize_fill(end - begin, 1);
            compared = values_same.data() - begin;
        }

        if (part.offsets)
        {
            for (size_t row = begin + 1; row < end; ++row)
            {
                const auto start = part.offsets[static_cast<ssize_t>(row) - 1];
                const auto previous_start = part.offsets[static_cast<ssize_t>(row) - 2];
                const auto size = part.offsets[row] - start;
                if (compared[row] && size != start - previous_start)
                    compared[row] = 0;
                else if (compared[row] && size)
                    compared[row] = memcmp(part.data + start, part.data + previous_start, size) == 0;
            }
        }
        else
        {
            switch (part.size)
            {
                case sizeof(UInt8): compare_values.template operator()<UInt8>(part.data, compared); break;
                case sizeof(UInt16): compare_values.template operator()<UInt16>(part.data, compared); break;
                case sizeof(UInt32): compare_values.template operator()<UInt32>(part.data, compared); break;
                case sizeof(UInt64): compare_values.template operator()<UInt64>(part.data, compared); break;
                case sizeof(UInt128): compare_values.template operator()<UInt128>(part.data, compared); break;
                default:
                    for (size_t row = begin + 1; row < end; ++row)
                        compared[row] &= memcmp(part.data + row * part.size, part.data + (row - 1) * part.size, part.size) == 0;
            }
        }

        if (part.null_map)
        {
            const UInt8 * __restrict nulls = part.null_map;
            for (size_t row = begin + 1; row < end; ++row)
                same[row] &= (nulls[row] == nulls[row - 1]) & (nulls[row] | compared[row]);
        }
    }

    /// A row that repeats the previous one has its hash, unless the previous one is skipped by the caller: then the row is
    /// hashed, which costs only the hashing. From the last row down, so that `skip[row - 1]` is the caller's.
    for (size_t row = end - 1; row > begin; --row)
        skip[row] |= same[row] & !skip[row - 1];
}

template <typename RowAt>
void HashedJoinKey::buildFixedRows(RowAt row_at, size_t num_rows, UInt128 * __restrict keys)
{
    buffer.resize(num_rows * fixed_size);
    size_t part_offset = 0;
    for (const auto & part : parts)
    {
        copyValues(buffer.data() + part_offset, fixed_size, part.data, part.size, row_at, num_rows);
        part_offset += part.size;
    }
    sipHash128PaddedFixedStrings(reinterpret_cast<const char *>(buffer.data()), fixed_size, num_rows, keys);
}

template <typename RowAt>
void HashedJoinKey::buildRows(RowAt row_at, size_t num_rows, UInt128 * __restrict keys)
{
    /// `fixed_size` has the fixed-width parts that are not `Nullable`.
    auto row_size = [&](size_t row)
    {
        size_t size = fixed_size;
        for (const auto & part : parts)
        {
            if (part.null_map)
            {
                size += 1;
                if (part.null_map[row])
                    continue;
            }
            if (part.offsets)
                size += sizeof(UInt64) + (part.offsets[row] - part.offsets[ssize_t(row) - 1]) + 1;
            else if (part.null_map)
                size += part.size;
        }
        return size;
    };

    auto write_row = [&](size_t row, UInt8 * pos)
    {
        for (const auto & part : parts)
        {
            if (part.null_map)
            {
                *pos++ = part.null_map[row];
                if (part.null_map[row])
                    continue;
            }
            if (part.offsets)
            {
                /// As `ColumnString::updateHashWithValue`: the size plus one, the chars and a zero byte.
                const size_t size = part.offsets[row] - part.offsets[ssize_t(row) - 1];
                unalignedStore<UInt64>(pos, size + 1);
                memcpy(pos + sizeof(UInt64), part.data + part.offsets[ssize_t(row) - 1], size);
                pos[sizeof(UInt64) + size] = 0;
                pos += sizeof(UInt64) + size + 1;
            }
            else
            {
                copyValue(pos, part.data + row * part.size, part.size);
                pos += part.size;
            }
        }
    };

    /// The offsets of the rows of a chunk in `buffer`, as in a `ColumnString`: row `i` ends at `offsets[i]`, and
    /// `offsets[-1]` is 0.
    UInt64 offsets_with_zero[chunk_rows + 1];
    offsets_with_zero[0] = 0;
    UInt64 * offsets = offsets_with_zero + 1;

    for (size_t first = 0; first < num_rows;)
    {
        size_t count = 0;
        size_t bytes = 0;
        for (; first + count < num_rows; ++count)
        {
            const size_t size = row_size(row_at(first + count));
            if (bytes + size > max_chunk_bytes)
                break;
            bytes += size;
            offsets[count] = bytes;
        }

        if (count == 0)
        {
            keys[first] = build(row_at(first));
            ++first;
            continue;
        }

        buffer.resize(bytes);
        for (size_t i = 0; i < count; ++i)
            write_row(row_at(first + i), buffer.data() + offsets[ssize_t(i) - 1]);

        sipHash128PaddedStrings(reinterpret_cast<const char *>(buffer.data()), offsets, count, keys + first);
        first += count;
    }
}

}
