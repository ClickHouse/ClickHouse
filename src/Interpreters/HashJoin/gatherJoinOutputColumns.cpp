#include <Interpreters/HashJoin/gatherJoinOutputColumns.h>

#include <Common/ProfileEvents.h>

#include <cstring>

namespace ProfileEvents
{
    extern const Event HashJoinDirectGatheredValues;
}

namespace DB
{

namespace
{

/// `STRIDE` is 0 when the width is only known at run time, which is what covers `FixedString(n)` for
/// an arbitrary `n`; a compile-time width turns the copy into a single load and store.
template <bool from_row_list, size_t STRIDE>
void gatherFixedStride(
    IColumn & dst,
    const DirectGatherColumn & src,
    const UInt64 * row_refs_begin,
    const UInt64 * row_refs_end,
    size_t rows_to_add)
{
    const size_t stride = STRIDE ? STRIDE : src.stride;
    const std::span<char> out_span = dst.insertRawUninitialized(rows_to_add);
    chassert(out_span.size() == rows_to_add * stride);

    const void * const * sources = src.data_by_block;
    char * out = out_span.data();

    auto copy_ref = [&](UInt64 ref_word)
    {
        const char * from = static_cast<const char *>(sources[refWordBlockNo(ref_word)])
            + static_cast<size_t>(refWordRowNo(ref_word)) * stride;
        memcpy(out, from, stride);
        out += stride;
    };

    /// At a few nanoseconds of loop body per row, 32 rows of lead cover one source row's memory latency.
    static constexpr size_t look_ahead = 32;
    const size_t num_refs = row_refs_end - row_refs_begin;
    for (size_t i = 0; i < num_refs; ++i)
    {
        if (i + look_ahead < num_refs)
        {
            /// Only an inline word carries a (block_no, row_number) address.
            const UInt64 ahead = row_refs_begin[i + look_ahead];
            if (refWordIsInline(ahead))
                __builtin_prefetch(
                    static_cast<const char *>(sources[refWordBlockNo(ahead)])
                    + static_cast<size_t>(refWordRowNo(ahead)) * stride);
        }

        const UInt64 word = row_refs_begin[i];
        if (!word)
        {
            memset(out, 0, stride);
            out += stride;
            continue;
        }

        if constexpr (from_row_list)
        {
            for (const UInt64 ref_word : refsOf(word))
                copy_ref(ref_word);
        }
        else
        {
            chassert(refWordIsInline(word));
            copy_ref(word);
        }
    }

    chassert(out == out_span.data() + out_span.size());
}

}

template <bool from_row_list>
void gatherColumnDirect(
    IColumn & dst,
    const DirectGatherColumn & src,
    const UInt64 * row_refs_begin,
    const UInt64 * row_refs_end,
    size_t rows_to_add)
{
    switch (src.stride)
    {
#define M(STRIDE) \
    case (STRIDE): \
        gatherFixedStride<from_row_list, (STRIDE)>(dst, src, row_refs_begin, row_refs_end, rows_to_add); \
        break;
        M(1)
        M(2)
        M(4)
        M(8)
        M(16)
        M(32)
#undef M
        default:
            gatherFixedStride<from_row_list, 0>(dst, src, row_refs_begin, row_refs_end, rows_to_add);
            break;
    }

    ProfileEvents::increment(ProfileEvents::HashJoinDirectGatheredValues, rows_to_add);
}

template void gatherColumnDirect<false>(IColumn &, const DirectGatherColumn &, const UInt64 *, const UInt64 *, size_t);
template void gatherColumnDirect<true>(IColumn &, const DirectGatherColumn &, const UInt64 *, const UInt64 *, size_t);

bool directGatherAdmits(TypeIndex type_id)
{
    switch (type_id)
    {
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
        case TypeIndex::UInt256:
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        case TypeIndex::BFloat16:
        case TypeIndex::Float32:
        case TypeIndex::Float64:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
        case TypeIndex::Time:
        case TypeIndex::Time64:
        case TypeIndex::Interval:
        case TypeIndex::IPv4:
        case TypeIndex::IPv6:
        case TypeIndex::UUID:
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
        case TypeIndex::FixedString:
            return true;
        default:
            return false;
    }
}

}
