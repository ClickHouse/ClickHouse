#include <Functions/GatherUtils/Algorithms.h>

#include <Columns/ColumnsCommon.h>

#include <bit>


namespace DB::GatherUtils
{

namespace
{

/// The rows of a run of equal conditions are consecutive both in the result and in the column that the run takes,
/// so a run can be copied with one copy of its bytes instead of one copy per row. Conditions often select long runs,
/// as in `CASE WHEN c THEN s ELSE '' END`. Alternating or random conditions give runs of one or two rows, for which
/// copying run by run costs more than copying row by row. So the rows are processed 64 at a time: a group with few
/// runs is copied run by run, any other group row by row. A group that alternates (almost) every row chooses the
/// source of a row with a branch, which is predictable for it; other groups choose it without a branch.
template <typename SourceA, typename SourceB>
void conditionalStrings(const SourceA & src_a, const SourceB & src_b, StringSink & sink, const PaddedPODArray<UInt8> & condition)
{
    static constexpr bool a_is_const = std::is_same_v<SourceA, ConstSource<StringSource>>;
    static constexpr bool b_is_const = std::is_same_v<SourceB, ConstSource<StringSource>>;
    static_assert(!(a_is_const && b_is_const));

    /// A group of 64 rows is copied run by run if it has at most this many runs.
    static constexpr size_t max_runs_to_copy_by_runs = 16;
    /// A group with at least this many runs is copied row by row with a branch on the condition.
    static constexpr size_t min_runs_to_copy_rows_with_branch = 56;
    /// Runs of more bytes than this are copied with `memcpy`, shorter ones with the inlined small copy.
    static constexpr size_t max_bytes_for_small_copy = 256;

    const size_t rows = condition.size();
    const UInt8 * cond = condition.data();

    auto & res_offsets = sink.offsets;
    auto & res_chars = sink.elements;

    /// A constant is the only row of its source's column, so it is read from the padded chars of that column, like a
    /// row of a column.
    const UInt8 * a_chars = src_a.elements.data();
    const UInt8 * b_chars = src_b.elements.data();
    const ColumnString::Offset * a_offsets = src_a.offsets.data();
    const ColumnString::Offset * b_offsets = src_b.offsets.data();
    const size_t a_value_size = a_is_const ? a_offsets[0] : 0;
    const size_t b_value_size = b_is_const ? b_offsets[0] : 0;

    /// As the generic `conditional` does, reserve the size of the larger source and grow if the result needs more.
    /// At least one byte, so that the copies below always write into allocated memory.
    res_chars.reserve_exact(std::max<size_t>({1, src_a.getSizeForReserve(), src_b.getSizeForReserve()}));
    UInt8 * res_data = res_chars.data();
    size_t res_capacity = res_chars.capacity();
    size_t res_size = 0;

    /// The copies may write up to 15 bytes past the copied ones, into the padding of the result or into bytes that
    /// later copies overwrite.
    auto reserve = [&](size_t bytes)
    {
        if (res_size + bytes > res_capacity) [[unlikely]]
        {
            res_chars.resize_assume_reserved(res_size);
            res_chars.reserve(res_size + bytes);
            res_data = res_chars.data();
            res_capacity = res_chars.capacity();
        }
    };

    auto copy_row = [&](const UInt8 * src, size_t size, size_t row)
    {
        reserve(size);
        /// Copy at least one byte, so that the copy makes one iteration for both empty and short strings.
        memcpySmallAllowReadWriteOverflow15(res_data + res_size, src, std::max<size_t>(size, 1));
        res_size += size;
        res_offsets[row] = res_size;
    };

    auto copy_rows = [&]<bool with_branch>(size_t begin, size_t end)
    {
        /// The offsets of the previous row are kept rather than read again.
        ColumnString::Offset a_prev = 0;
        ColumnString::Offset b_prev = 0;
        if constexpr (!a_is_const)
            a_prev = a_offsets[begin - 1];
        if constexpr (!b_is_const)
            b_prev = b_offsets[begin - 1];

        for (size_t row = begin; row < end; ++row)
        {
            const UInt8 * a_src = a_chars;
            size_t a_size = a_value_size;
            if constexpr (!a_is_const)
            {
                a_src += a_prev;
                a_size = a_offsets[row] - a_prev;
                a_prev = a_offsets[row];
            }
            const UInt8 * b_src = b_chars;
            size_t b_size = b_value_size;
            if constexpr (!b_is_const)
            {
                b_src += b_prev;
                b_size = b_offsets[row] - b_prev;
                b_prev = b_offsets[row];
            }

            const bool take_a = cond[row] != 0;
            if constexpr (with_branch)
            {
                if (take_a)
                    copy_row(a_src, a_size, row);
                else
                    copy_row(b_src, b_size, row);
            }
            else
            {
                copy_row(
                    __builtin_unpredictable(take_a) ? a_src : b_src,
                    __builtin_unpredictable(take_a) ? a_size : b_size,
                    row);
            }
        }
    };

    auto copy_run = [&]<bool is_const>(const ColumnString::Offset * offsets, const UInt8 * chars, size_t const_size, size_t begin, size_t end)
    {
        if constexpr (is_const)
        {
            if (const_size == 0)
            {
                std::fill(res_offsets.begin() + begin, res_offsets.begin() + end, res_size);
                return;
            }
            reserve((end - begin) * const_size);
            for (size_t row = begin; row < end; ++row)
            {
                memcpySmallAllowReadWriteOverflow15(res_data + res_size, chars, const_size);
                res_size += const_size;
                res_offsets[row] = res_size;
            }
        }
        else
        {
            const size_t src_begin = offsets[begin - 1];
            const size_t bytes = offsets[end - 1] - src_begin;
            reserve(bytes);
            if (bytes <= max_bytes_for_small_copy)
                memcpySmallAllowReadWriteOverflow15(res_data + res_size, chars + src_begin, bytes);
            else
                memcpy(res_data + res_size, chars + src_begin, bytes);

            /// The offsets of the run are those of the column, shifted (modulo 2^64) to the position of the run.
            const ColumnString::Offset shift = res_size - src_begin;
            for (size_t row = begin; row < end; ++row)
                res_offsets[row] = offsets[row] + shift;
            res_size += bytes;
        }
    };

    for (size_t group = 0; group < rows; group += 64)
    {
        const size_t group_rows = std::min<size_t>(64, rows - group);

        /// Bit i is set if row `group + i` takes `src_a`.
        const UInt64 take_a = [&]
        {
            if (group_rows == 64)
                return bytes64MaskToBits64Mask(cond + group);
            UInt8 tail[64] = {};
            memcpy(tail, cond + group, group_rows);
            return bytes64MaskToBits64Mask(tail);
        }();

        /// Bit i is set if a run starts at row `group + i`.
        const UInt64 group_mask = group_rows == 64 ? ~0ULL : (1ULL << group_rows) - 1;
        UInt64 run_starts = ((take_a ^ (take_a << 1)) | 1) & group_mask;

        const size_t runs = std::popcount(run_starts);
        if (runs >= min_runs_to_copy_rows_with_branch)
        {
            copy_rows.template operator()<true>(group, group + group_rows);
            continue;
        }

        if (runs > max_runs_to_copy_by_runs)
        {
            copy_rows.template operator()<false>(group, group + group_rows);
            continue;
        }

        while (run_starts)
        {
            const size_t begin = std::countr_zero(run_starts);
            run_starts &= run_starts - 1;
            const size_t end = run_starts ? std::countr_zero(run_starts) : group_rows;
            if ((take_a >> begin) & 1)
                copy_run.template operator()<a_is_const>(a_offsets, a_chars, a_value_size, group + begin, group + end);
            else
                copy_run.template operator()<b_is_const>(b_offsets, b_chars, b_value_size, group + begin, group + end);
        }
    }

    res_chars.resize_assume_reserved(res_size);
    sink.row_num = rows;
    sink.current_offset = res_size;
}

}

void conditional(StringSource && src_a, StringSource && src_b, StringSink & sink, const PaddedPODArray<UInt8> & condition)
{
    conditionalStrings(src_a, src_b, sink, condition);
}

void conditional(StringSource && src_a, ConstSource<StringSource> && src_b, StringSink & sink, const PaddedPODArray<UInt8> & condition)
{
    conditionalStrings(src_a, src_b, sink, condition);
}

void conditional(ConstSource<StringSource> && src_a, StringSource && src_b, StringSink & sink, const PaddedPODArray<UInt8> & condition)
{
    conditionalStrings(src_a, src_b, sink, condition);
}

}
