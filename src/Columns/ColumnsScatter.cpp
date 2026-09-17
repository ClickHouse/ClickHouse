#include <Columns/ColumnsScatter.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Common/Arena.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/memcpySmall.h>

#include <Core/TypeId.h>

#include <algorithm>
#include <array>
#include <bit>
#include <limits>
#include <string_view>
#include <utility>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

}

namespace DB::ColumnsScatter
{

namespace
{

using NtLine = char __attribute__((vector_size(LINE_BYTES)));

template <typename Pid>
struct RouteFromPids
{
    const Pid * pids;
    ALWAYS_INLINE UInt32 partition(size_t i) const { return pids[i]; }
};

template <size_t width, typename Route>
void scatterDirect(Route route, const char * data, size_t n, char ** cursors)
{
    for (size_t i = 0; i < n; ++i)
    {
        const UInt32 p = route.partition(i);
        char * dst = cursors[p];
        __builtin_memcpy_inline(dst, data + i * width, width);
        cursors[p] = dst + width;
    }
}

/// Runtime-width copy: 16-byte chunks, then an overlapped 16-byte tail. A runtime-size `memcpy`
/// would lower to a libc call per row. The barrier stops clang's loop-idiom pass from recreating
/// that call. The tail rewrites bytes of the same row with the same values and never writes past
/// dst + w, so the exact-sized destinations stay intact.
ALWAYS_INLINE void copyRowExact(char * __restrict dst, const char * __restrict src, size_t w)
{
    if (w >= 16)
    {
        /// 32-byte stride so the backend still pairs the loads and stores (ldp/stp on AArch64)
        /// while the per-chunk barrier holds.
        size_t i = 0;
        for (; i + 32 <= w; i += 32)
        {
            __builtin_memcpy_inline(dst + i, src + i, 32);
            __asm__ __volatile__("" : : : "memory");
        }
        if (i + 16 <= w)
        {
            __builtin_memcpy_inline(dst + i, src + i, 16);
            __asm__ __volatile__("" : : : "memory");
            i += 16;
        }
        if (i != w)
            __builtin_memcpy_inline(dst + w - 16, src + w - 16, 16);
    }
    else if (w >= 8)
    {
        __builtin_memcpy_inline(dst, src, 8);
        __builtin_memcpy_inline(dst + w - 8, src + w - 8, 8);
    }
    else if (w >= 4)
    {
        __builtin_memcpy_inline(dst, src, 4);
        __builtin_memcpy_inline(dst + w - 4, src + w - 4, 4);
    }
    else if (w >= 2)
    {
        __builtin_memcpy_inline(dst, src, 2);
        __builtin_memcpy_inline(dst + w - 2, src + w - 2, 2);
    }
    else if (w == 1)
    {
        dst[0] = src[0];
    }
}

template <typename Route>
void scatterDirectGeneric(Route route, const char * data, size_t n, size_t w, char ** cursors)
{
    for (size_t i = 0; i < n; ++i)
    {
        const UInt32 p = route.partition(i);
        char * dst = cursors[p];
        copyRowExact(dst, data + i * w, w);
        cursors[p] = dst + w;
    }
}

template <size_t width, typename Route>
void scatterSwwc(Route route, const char * data, size_t n, ScatterScratch & scratch)
{
    /// Hoisted by hand: the non-temporal store through `char *` defeats TBAA, so the compiler
    /// otherwise reloads the cursor and fill bases on every row.
    char * const staging = scratch.staging;
    char ** const cursors = scratch.cursors.data();
    UInt32 * const fill = scratch.fill.data();

    for (size_t i = 0; i < n; ++i)
    {
        const UInt32 p = route.partition(i);
        char * line = staging + static_cast<size_t>(p) * LINE_BYTES;
        UInt32 f = fill[p];
        __builtin_memcpy_inline(line + f, data + i * width, width);
        f += width;
        if (f == LINE_BYTES)
        {
            char * cur = cursors[p];
            const UInt32 m = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cur) & (LINE_BYTES - 1));
            if (m) /// first flush of a misaligned stream: emit the partial head line with regular stores
            {
                __builtin_memcpy(cur, line + m, LINE_BYTES - m);
                cursors[p] = cur + (LINE_BYTES - m);
            }
            else
            {
                __builtin_nontemporal_store(*reinterpret_cast<const NtLine *>(line), reinterpret_cast<NtLine *>(cur));
                cursors[p] = cur + LINE_BYTES;
            }
            f = 0;
        }
        fill[p] = f;
    }
}

template <size_t width, typename Route>
ALWAYS_INLINE void scatterOne(Route route, const char * data, size_t n, bool use_swwc, ScatterScratch & scratch)
{
    if (use_swwc)
        scatterSwwc<width>(route, data, n, scratch);
    else
        scatterDirect<width>(route, data, n, scratch.cursors.data());
}

template <typename Pid>
void scatterPidChunkImpl(size_t w, const Pid * pids, const char * data, size_t n, bool use_swwc, ScatterScratch & scratch)
{
    RouteFromPids<Pid> route{pids};
    switch (w)
    {
        case 1: scatterOne<1>(route, data, n, use_swwc, scratch); break;
        case 2: scatterOne<2>(route, data, n, use_swwc, scratch); break;
        case 4: scatterOne<4>(route, data, n, use_swwc, scratch); break;
        case 8: scatterOne<8>(route, data, n, use_swwc, scratch); break;
        case 16: scatterOne<16>(route, data, n, use_swwc, scratch); break;
        /// Width 32 fails `widthSupportsSwwc`: a 16-byte alignment cannot keep a 32-byte staging
        /// stride exact. It goes direct, with a compile-time width that spares `copyRowExact`'s branches.
        case 32: scatterDirect<32>(route, data, n, scratch.cursors.data()); break;
        default: scatterDirectGeneric(route, data, n, w, scratch.cursors.data()); break;
    }
}

/// `lanes` breaks the load-increment-store dependency chain: four rows in flight increment four independent counters.
template <typename Pid, typename Counter>
void histogramPidChunkImpl(const Pid * pids, size_t n, Counter * hist, Counter * lanes, size_t fanout)
{
    if (!lanes)
    {
        for (size_t i = 0; i < n; ++i)
            ++hist[pids[i]];
        return;
    }
    size_t i = 0;
    for (; i + 4 <= n; i += 4)
    {
        ++lanes[0 * fanout + pids[i + 0]];
        ++lanes[1 * fanout + pids[i + 1]];
        ++lanes[2 * fanout + pids[i + 2]];
        ++lanes[3 * fanout + pids[i + 3]];
    }
    for (; i < n; ++i)
        ++lanes[(i & 3) * fanout + pids[i]];
}

template <typename Counter>
void reduceHistogramLanesImpl(Counter * hist, const Counter * lanes, size_t fanout)
{
    for (size_t p = 0; p < fanout; ++p)
        hist[p] += lanes[0 * fanout + p] + lanes[1 * fanout + p] + lanes[2 * fanout + p] + lanes[3 * fanout + p];
}


thread_local DispatchTrace * dispatch_trace = nullptr;

ALWAYS_INLINE void traceDispatch(TypeIndex type, ScatterKernelId kernel)
{
    if (dispatch_trace) [[unlikely]]
        dispatch_trace->entries.push_back({type, kernel});
}

/** State for the String kernels. Each shard has two output streams that advance together.
  * Chars have a data-dependent per-row length. Offsets are running per-shard totals rebased to 0,
  * not copies of the source offsets. `scatterString` sizes the chars streams with
  * `stringBytesPerShardImpl`, then allocates, seeds, and scatters chunk by chunk.
  */
struct StringScatterState
{
    /// One 32-byte record per shard, so a row touches one cache line of cursor state instead of one
    /// line in each of three arrays. At fanout 8192 the cursor state alone is 256 KiB. The lines
    /// touched per row drive L2 traffic.
    struct ShardCursor
    {
        char * chars = nullptr;
        UInt64 * offsets = nullptr;
        /// Next row's destination offset after adding its length; 0 for a fresh destination.
        UInt64 rebased = 0;
        UInt64 padding = 0;
    };

    size_t fanout = 0;
    PaddedPODArray<ShardCursor> cursors;

    void init(size_t fanout_)
    {
        fanout = fanout_;
        cursors.resize(fanout);
    }

    void seed(size_t p, char * chars_cursor, UInt64 * offsets_cursor, UInt64 rebased_start)
    {
        cursors[p] = {chars_cursor, offsets_cursor, rebased_start, 0};
    }
};

/// The chars copy and the rebased offset store, fused per row. The copy follows the
/// `memcpySmallAllowReadWriteOverflow15` contract: a row write may touch up to 15 bytes past the
/// row's end. Every shard's chars destination must be its own overflow-tolerant allocation; a
/// `ColumnString` is one. Carving the shards out of a single shared buffer is not supported.
/// A row written to shard p would clobber the head of shard p + 1.
template <typename Pid>
void scatterStringChunkImpl(const char * chars, const UInt64 * offsets, const Pid * pids, size_t n, StringScatterState & state)
{
    StringScatterState::ShardCursor * const cursors = state.cursors.data();

    UInt64 prev = 0;
    for (size_t i = 0; i < n; ++i)
    {
        const size_t p = pids[i];
        const UInt64 end = offsets[i];
        const UInt64 len = end - prev;
        StringScatterState::ShardCursor & cursor = cursors[p];
        memcpySmallAllowReadWriteOverflow15(cursor.chars, chars + prev, len);
        cursor.chars += len;
        const UInt64 total = cursor.rebased + len;
        cursor.rebased = total;
        *cursor.offsets++ = total;
        prev = end;
    }
}

/// `offsets` is in `ColumnString` form: offsets[i] ends row i, so row i spans [offsets[i - 1], offsets[i])
/// with offsets[-1] taken as 0. `bytes_per_shard` is written every iteration; clang-tidy misreads
/// the indexed write as const-able.
template <typename Pid>
void stringBytesPerShardImpl(
    const UInt64 * offsets, const Pid * pids, size_t n, UInt64 * bytes_per_shard) /// NOLINT(readability-non-const-parameter)
{
    UInt64 prev = 0;
    for (size_t i = 0; i < n; ++i)
    {
        const UInt64 end = offsets[i];
        bytes_per_shard[pids[i]] += end - prev;
        prev = end;
    }
}


template <typename Pid>
using SourcePids = std::span<const std::span<const Pid>>;

template <typename Pid>
using ScatterKernel = MutableColumns (*)(std::span<const IColumn * const>, SourcePids<Pid>, std::span<const UInt32>);

/// Untraced on purpose: the trace records one top-level kernel per call, and this is what the
/// composite kernels recurse through. Defined after the dispatch table.
template <typename Pid>
MutableColumns dispatchToKernel(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard);

template <typename Pid>
MutableColumns scatterFallback(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard);

/// Every fixed-and-contiguous type with `insertRawUninitialized` support. The body stays runtime-width;
/// the compile-time kernel is picked per chunk inside `scatterPidChunkImpl`.
template <typename Pid>
MutableColumns scatterFixedWidth(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard)
{
    const IColumn & sample = *sources[0];
    const size_t width = sample.sizeOfValueIfFixed();
    const size_t num_shards = rows_per_shard.size();
    const bool use_swwc = num_shards >= SWWC_MIN_FANOUT && widthSupportsSwwc(width);

    /// The entry's `getDataType` equality cannot see FixedString widths; a mismatch would stride
    /// the source wrongly and corrupt every shard silently.
    for (size_t b = 1; b < sources.size(); ++b)
        if (sources[b]->sizeOfValueIfFixed() != width)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Source column {} has value width {} but source 0 has width {}",
                b,
                sources[b]->sizeOfValueIfFixed(),
                width);

    MutableColumns result(num_shards);
    ScatterScratch scratch;
    scratch.init(num_shards, use_swwc);
    for (size_t s = 0; s < num_shards; ++s)
    {
        auto [column, raw] = allocateUninitializedFixed(sample, rows_per_shard[s]);
        scratch.seed(s, raw.data());
        result[s] = std::move(column);
    }

    for (size_t b = 0; b < sources.size(); ++b)
        /// Length is `rows * width`, not `getRawData().size()`: the checker cannot correlate the
        /// pointer with a size call, but the two are equal by contract.
        scatterPidChunkImpl(width, pids[b].data(), sources[b]->getRawData().data(), sources[b]->size(), use_swwc, scratch); /// NOLINT(bugprone-suspicious-stringview-data-usage)

    scratch.drain();
    return result;
}

/// One byte-histogram pass over (offsets, pids) sizes the chars streams, so both destinations can be
/// `resize_exact`ed and left for the scatter writes to first-touch.
template <typename Pid>
MutableColumns scatterString(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();

    PaddedPODArray<UInt64> bytes_per_shard;
    bytes_per_shard.resize_fill(num_shards, 0);
    for (size_t b = 0; b < sources.size(); ++b)
    {
        const auto & source = assert_cast<const ColumnString &>(*sources[b]);
        stringBytesPerShardImpl(source.getOffsets().data(), pids[b].data(), source.size(), bytes_per_shard.data());
    }

    MutableColumns result(num_shards);
    StringScatterState state;
    state.init(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
    {
        auto column = ColumnString::create();
        column->getChars().resize_exact(bytes_per_shard[s]);
        column->getOffsets().resize_exact(rows_per_shard[s]);
        state.seed(s, reinterpret_cast<char *>(column->getChars().data()), column->getOffsets().data(), 0);
        result[s] = std::move(column);
    }

    for (size_t b = 0; b < sources.size(); ++b)
    {
        const auto & source = assert_cast<const ColumnString &>(*sources[b]);
        scatterStringChunkImpl(
            reinterpret_cast<const char *>(source.getChars().data()),
            source.getOffsets().data(),
            pids[b].data(),
            source.size(),
            state);
    }
    return result;
}

/// The null map is just a width-1 fixed column; the nested column recurses with the same pids.
template <typename Pid>
MutableColumns scatterNullable(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();

    ColumnRawPtrs null_maps;
    ColumnRawPtrs nested;
    null_maps.reserve(sources.size());
    nested.reserve(sources.size());
    for (const IColumn * source : sources)
    {
        const auto & nullable = assert_cast<const ColumnNullable &>(*source);
        null_maps.push_back(&nullable.getNullMapColumn());
        nested.push_back(&nullable.getNestedColumn());
    }

    auto null_map_shards = scatterFixedWidth<Pid>({null_maps.data(), null_maps.size()}, pids, rows_per_shard);
    auto nested_shards = dispatchToKernel<Pid>({nested.data(), nested.size()}, pids, rows_per_shard);

    MutableColumns result(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        result[s] = ColumnNullable::create(std::move(nested_shards[s]), std::move(null_map_shards[s]));
    return result;
}

template <typename Pid>
MutableColumns scatterTuple(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();
    const auto & first = assert_cast<const ColumnTuple &>(*sources[0]);
    const size_t num_elements = first.tupleSize();

    /// The entry's `getDataType` equality cannot see tuple arity; a mismatch would index elements out of bounds.
    for (size_t b = 1; b < sources.size(); ++b)
        if (assert_cast<const ColumnTuple &>(*sources[b]).tupleSize() != num_elements)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Source column {} is a tuple of {} elements but source 0 has {} elements",
                b,
                assert_cast<const ColumnTuple &>(*sources[b]).tupleSize(),
                num_elements);

    /// An element-less tuple carries nothing but its row count.
    if (num_elements == 0)
    {
        MutableColumns result(num_shards);
        for (size_t s = 0; s < num_shards; ++s)
            result[s] = ColumnTuple::create(rows_per_shard[s]);
        return result;
    }

    std::vector<MutableColumns> element_shards(num_elements); /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    ColumnRawPtrs element_sources;
    for (size_t e = 0; e < num_elements; ++e)
    {
        element_sources.clear();
        for (const IColumn * source : sources)
            element_sources.push_back(&assert_cast<const ColumnTuple &>(*source).getColumn(e));
        element_shards[e] = dispatchToKernel<Pid>({element_sources.data(), element_sources.size()}, pids, rows_per_shard);
    }

    MutableColumns result(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
    {
        MutableColumns elements(num_elements);
        for (size_t e = 0; e < num_elements; ++e)
            elements[e] = std::move(element_shards[e][s]);
        result[s] = ColumnTuple::create(std::move(elements));
    }
    return result;
}

/// Destination offsets are rebased per-shard element totals, and the nested column is scattered with
/// each row's pid replicated over that row's elements.
template <typename Pid>
MutableColumns scatterArray(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();

    /// Nested dispatch sizes destinations from UInt32 counts, so wider batches fall back. The check
    /// runs before pid expansion. Expansion would otherwise materialize gigabytes and discard them.
    size_t total_elements = 0;
    for (const IColumn * source : sources)
    {
        const auto & offsets = assert_cast<const ColumnArray &>(*source).getOffsets();
        total_elements += offsets.empty() ? 0 : offsets.back();
    }
    if (total_elements > std::numeric_limits<UInt32>::max())
        return scatterFallback<Pid>(sources, pids, rows_per_shard);

    PaddedPODArray<UInt64> elements_per_shard;
    elements_per_shard.resize_fill(num_shards, 0);
    std::vector<PaddedPODArray<Pid>> element_pids(sources.size()); /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    ColumnRawPtrs nested;
    nested.reserve(sources.size());
    for (size_t b = 0; b < sources.size(); ++b)
    {
        const auto & array = assert_cast<const ColumnArray &>(*sources[b]);
        const auto & offsets = array.getOffsets();
        const size_t n = array.size();
        element_pids[b].resize(offsets.empty() ? 0 : offsets[n - 1]);
        UInt64 prev = 0;
        for (size_t i = 0; i < n; ++i)
        {
            const UInt64 end = offsets[i];
            const Pid p = pids[b][i];
            elements_per_shard[p] += end - prev;
            for (UInt64 j = prev; j < end; ++j)
                element_pids[b][j] = p;
            prev = end;
        }
        nested.push_back(&array.getData());
    }

    PaddedPODArray<UInt32> nested_rows_per_shard;
    nested_rows_per_shard.resize(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        nested_rows_per_shard[s] = static_cast<UInt32>(elements_per_shard[s]);

    std::vector<std::span<const Pid>> element_pid_spans; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    element_pid_spans.reserve(sources.size());
    for (const auto & span : element_pids)
        element_pid_spans.emplace_back(span.data(), span.size());
    auto nested_shards = dispatchToKernel<Pid>(
        {nested.data(), nested.size()},
        {element_pid_spans.data(), element_pid_spans.size()},
        {nested_rows_per_shard.data(), num_shards});

    MutableColumns offsets_shards(num_shards);
    /// `ShardCursor` is reused only for its {offsets cursor, rebased total} pair; there is no chars stream to seed.
    StringScatterState offsets_state;
    offsets_state.init(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
    {
        auto offsets_column = ColumnArray::ColumnOffsets::create();
        offsets_column->getData().resize_exact(rows_per_shard[s]);
        offsets_state.seed(s, nullptr, offsets_column->getData().data(), 0);
        offsets_shards[s] = std::move(offsets_column);
    }
    for (size_t b = 0; b < sources.size(); ++b)
    {
        const auto & array = assert_cast<const ColumnArray &>(*sources[b]);
        const auto & offsets = array.getOffsets();
        StringScatterState::ShardCursor * const cursors = offsets_state.cursors.data();
        UInt64 prev = 0;
        for (size_t i = 0; i < array.size(); ++i)
        {
            const UInt64 end = offsets[i];
            const size_t p = pids[b][i];
            StringScatterState::ShardCursor & cursor = cursors[p];
            const UInt64 total = cursor.rebased + (end - prev);
            cursor.rebased = total;
            *cursor.offsets++ = total;
            prev = end;
        }
    }

    MutableColumns result(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        result[s] = ColumnArray::create(std::move(nested_shards[s]), std::move(offsets_shards[s]));
    return result;
}

/// A Map is its nested `Array(Tuple(key, value))`.
template <typename Pid>
MutableColumns scatterMap(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard)
{
    ColumnRawPtrs nested;
    nested.reserve(sources.size());
    for (const IColumn * source : sources)
        nested.push_back(&assert_cast<const ColumnMap &>(*source).getNestedColumn());
    auto nested_shards = scatterArray<Pid>({nested.data(), nested.size()}, pids, rows_per_shard);

    /// Statistics is only a serialization sizing hint, and merged shards have no exact one anyway,
    /// so the first source's is good enough - the same choice `ColumnMap::scatter` makes.
    const auto & statistics = assert_cast<const ColumnMap &>(*sources[0]).getStatistics();
    MutableColumns result(nested_shards.size());
    for (size_t s = 0; s < nested_shards.size(); ++s)
        result[s] = ColumnMap::create(std::move(nested_shards[s]), statistics);
    return result;
}

/// Stays LowCardinality: the index stream takes the fixed-width kernel and every shard shares one
/// dictionary, as `ColumnLowCardinality::scatter` does. Several sources generally mean several
/// dictionaries. Merging them is the fallback's body, so that case is delegated.
template <typename Pid>
MutableColumns scatterLowCardinality(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard)
{
    if (sources.size() > 1)
        return scatterFallback<Pid>(sources, pids, rows_per_shard);

    const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(*sources[0]);
    const IColumn * indexes = &low_cardinality.getIndexes();
    auto index_shards = scatterFixedWidth<Pid>({&indexes, 1}, pids, rows_per_shard);

    ColumnPtr shared_dictionary = IColumn::mutate(low_cardinality.getDictionaryPtr());
    MutableColumns result(rows_per_shard.size());
    for (size_t s = 0; s < result.size(); ++s)
        result[s] = ColumnLowCardinality::create(shared_dictionary->assumeMutable(), std::move(index_shards[s]), /*is_shared*/ true);
    return result;
}

/// `IColumn::scatter` per source, merged with `insertRangeFrom`, so each type keeps its own scatter
/// semantics - notably `ColumnAggregateFunction` results still view the source arena. Sources must
/// already be normalized.
template <typename Pid>
MutableColumns scatterFallback(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();

    IColumn::Selector selector;
    if (sources.size() == 1)
    {
        selector.resize_exact(pids[0].size());
        for (size_t j = 0; j < pids[0].size(); ++j)
            selector[j] = pids[0][j];
        auto parts = sources[0]->scatter(num_shards, selector);
        MutableColumns result(num_shards);
        for (size_t s = 0; s < num_shards; ++s)
            result[s] = std::move(parts[s]);
        return result;
    }

    MutableColumns result(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
    {
        result[s] = sources[0]->cloneEmpty();
        if (rows_per_shard[s])
            result[s]->reserve(rows_per_shard[s]);
    }
    for (size_t b = 0; b < sources.size(); ++b)
    {
        selector.resize_exact(pids[b].size());
        for (size_t j = 0; j < pids[b].size(); ++j)
            selector[j] = pids[b][j];
        auto parts = sources[b]->scatter(num_shards, selector);
        for (size_t s = 0; s < num_shards; ++s)
            if (parts[s]->size())
                result[s]->insertRangeFrom(*parts[s], 0, parts[s]->size());
    }
    return result;
}

constexpr size_t SCATTER_TABLE_SIZE = static_cast<size_t>(std::numeric_limits<std::underlying_type_t<TypeIndex>>::max()) + 1;

constexpr std::array<TypeIndex, 25> FIXED_WIDTH_TYPES = {
    TypeIndex::UInt8,     TypeIndex::UInt16,   TypeIndex::UInt32,   TypeIndex::UInt64,     TypeIndex::UInt128, TypeIndex::UInt256,
    TypeIndex::Int8,      TypeIndex::Int16,    TypeIndex::Int32,    TypeIndex::Int64,      TypeIndex::Int128,  TypeIndex::Int256,
    TypeIndex::BFloat16,  TypeIndex::Float32,  TypeIndex::Float64,  TypeIndex::UUID,       TypeIndex::IPv4,    TypeIndex::IPv6,
    TypeIndex::Decimal32, TypeIndex::Decimal64, TypeIndex::Decimal128, TypeIndex::Decimal256, TypeIndex::DateTime64, TypeIndex::Time64,
    TypeIndex::FixedString};

/// TypeIndex to kernel, sized by the underlying type so indexing needs no bounds check. Types
/// without a dedicated kernel take the fallback. The function-pointer table is derived from this
/// one. The traced kernel therefore equals the executed kernel by construction, and a new type
/// family is registered in one place.
constexpr std::array<ScatterKernelId, SCATTER_TABLE_SIZE> buildKernelIdTable()
{
    std::array<ScatterKernelId, SCATTER_TABLE_SIZE> table{};
    table.fill(ScatterKernelId::Fallback);
    for (TypeIndex type : FIXED_WIDTH_TYPES)
        table[static_cast<size_t>(type)] = ScatterKernelId::FixedWidth;
    table[static_cast<size_t>(TypeIndex::String)] = ScatterKernelId::String;
    table[static_cast<size_t>(TypeIndex::Nullable)] = ScatterKernelId::Nullable;
    table[static_cast<size_t>(TypeIndex::Tuple)] = ScatterKernelId::Tuple;
    table[static_cast<size_t>(TypeIndex::Array)] = ScatterKernelId::Array;
    table[static_cast<size_t>(TypeIndex::Map)] = ScatterKernelId::Map;
    table[static_cast<size_t>(TypeIndex::LowCardinality)] = ScatterKernelId::LowCardinality;
    return table;
}

constexpr auto KERNEL_ID_TABLE = buildKernelIdTable();

template <typename Pid>
constexpr ScatterKernel<Pid> kernelForId(ScatterKernelId id)
{
    switch (id)
    {
        case ScatterKernelId::FixedWidth:
            return &scatterFixedWidth<Pid>;
        case ScatterKernelId::String:
            return &scatterString<Pid>;
        case ScatterKernelId::Nullable:
            return &scatterNullable<Pid>;
        case ScatterKernelId::Tuple:
            return &scatterTuple<Pid>;
        case ScatterKernelId::Array:
            return &scatterArray<Pid>;
        case ScatterKernelId::Map:
            return &scatterMap<Pid>;
        case ScatterKernelId::LowCardinality:
            return &scatterLowCardinality<Pid>;
        case ScatterKernelId::ConstCompact: /// not a dispatch-table kernel: handled before dispatch
        case ScatterKernelId::Fallback:
            return &scatterFallback<Pid>;
    }
    UNREACHABLE();
}

template <typename Pid>
constexpr std::array<ScatterKernel<Pid>, SCATTER_TABLE_SIZE> buildScatterTable()
{
    std::array<ScatterKernel<Pid>, SCATTER_TABLE_SIZE> table{};
    for (size_t i = 0; i < SCATTER_TABLE_SIZE; ++i)
        table[i] = kernelForId<Pid>(KERNEL_ID_TABLE[i]);
    return table;
}

template <typename Pid>
MutableColumns dispatchToKernel(std::span<const IColumn * const> sources, SourcePids<Pid> pids, std::span<const UInt32> rows_per_shard)
{
    static constexpr auto table = buildScatterTable<Pid>();
    return table[static_cast<size_t>(sources[0]->getDataType())](sources, pids, rows_per_shard);
}

/// An all-const batch of byte-identical values needs only `cloneResized` per shard. Equality has to
/// be byte-exact rather than `compareAt`. A physical split must preserve +0.0 vs -0.0 and NaN
/// payloads. Empty when the values differ, or cannot be serialized and there is more than one source.
MutableColumns tryScatterAllConst(std::span<const IColumn * const> sources, std::span<const UInt32> rows_per_shard)
{
    const auto & first = assert_cast<const ColumnConst &>(*sources[0]);
    if (sources.size() > 1)
    {
        /// `ColumnFunction` cannot be serialized, so its values cannot be compared byte-exactly.
        if (first.getDataColumn().getDataType() == TypeIndex::Function)
            return {};

        Arena arena;
        const char * ref_begin = nullptr;
        const std::string_view ref = first.getDataColumn().serializeValueIntoArena(0, arena, ref_begin, nullptr);
        for (size_t b = 1; b < sources.size(); ++b)
        {
            const auto & other = assert_cast<const ColumnConst &>(*sources[b]);
            const char * begin = nullptr;
            const std::string_view serialized = other.getDataColumn().serializeValueIntoArena(0, arena, begin, nullptr);
            if (serialized != ref)
                return {};
        }
    }

    MutableColumns result(rows_per_shard.size());
    for (size_t s = 0; s < rows_per_shard.size(); ++s)
        result[s] = first.cloneResized(rows_per_shard[s]);
    return result;
}

template <typename Pid>
void countRowsPerShardImpl(SourcePids<Pid> pids_per_source, std::span<UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();
    const bool interleave = num_shards <= HIST_INTERLEAVE_MAX_FANOUT;
    PaddedPODArray<UInt32> lanes;
    if (interleave)
        lanes.resize_fill(4 * num_shards, 0);
    for (const auto & pids : pids_per_source)
        histogramPidChunkImpl(pids.data(), pids.size(), rows_per_shard.data(), interleave ? lanes.data() : nullptr, num_shards);
    if (interleave)
        reduceHistogramLanesImpl(rows_per_shard.data(), lanes.data(), num_shards);
}

template <typename Pid>
MutableColumns scatterImpl(
    std::span<const IColumn * const> sources, SourcePids<Pid> pids_per_source, size_t num_shards, std::span<const UInt32> rows_per_shard)
{
    if (sources.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot scatter an empty batch of source columns");
    if (sources.size() != pids_per_source.size())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Number of source columns ({}) does not match number of pid spans ({})",
            sources.size(),
            pids_per_source.size());
    if (num_shards == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot scatter into zero shards");
    if (!rows_per_shard.empty() && rows_per_shard.size() != num_shards)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Size of rows_per_shard ({}) does not match num_shards ({})",
            rows_per_shard.size(),
            num_shards);

    size_t total_rows = 0;
    for (size_t b = 0; b < sources.size(); ++b)
    {
        if (sources[b]->size() != pids_per_source[b].size())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Source column {} has {} rows but {} partition ids",
                b,
                sources[b]->size(),
                pids_per_source[b].size());
        /// One virtual call per chunk buys the same-concrete-type contract the raw-byte kernels
        /// would otherwise violate silently.
        if (sources[b]->getDataType() != sources[0]->getDataType())
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Source column {} has type {} but source 0 has type {}",
                b,
                sources[b]->getName(),
                sources[0]->getName());
        total_rows += pids_per_source[b].size();
#ifdef DEBUG_OR_SANITIZER_BUILD
        for (const Pid pid : pids_per_source[b])
            chassert(static_cast<size_t>(pid) < num_shards);
#endif
    }

#ifdef DEBUG_OR_SANITIZER_BUILD
    /// These counts size destinations the kernels then write through, so an undercount is a heap
    /// overflow in release. Recounted after the pid-range asserts above, so the recount cannot go out of bounds.
    if (!rows_per_shard.empty())
    {
        PaddedPODArray<UInt32> recounted;
        recounted.resize_fill(num_shards, 0);
        countRowsPerShardImpl<Pid>(pids_per_source, {recounted.data(), num_shards});
        for (size_t s = 0; s < num_shards; ++s)
            chassert(recounted[s] == rows_per_shard[s]);
    }
#endif

    /// The fast kernels and the compact-const path size destinations from UInt32 counts.
    const bool fits_32 = total_rows <= std::numeric_limits<UInt32>::max();

    PaddedPODArray<UInt32> counted;
    auto ensure_counts = [&]() -> std::span<const UInt32>
    {
        if (!rows_per_shard.empty())
            return rows_per_shard;
        if (counted.empty())
        {
            counted.resize_fill(num_shards, 0);
            countRowsPerShardImpl<Pid>(pids_per_source, {counted.data(), num_shards});
        }
        return {counted.data(), num_shards};
    };

    if (fits_32)
    {
        bool all_const = true;
        for (const IColumn * source : sources)
            all_const &= source->isConst();
        if (all_const)
        {
            auto compact = tryScatterAllConst(sources, ensure_counts());
            if (!compact.empty())
            {
                traceDispatch(sources[0]->getDataType(), ScatterKernelId::ConstCompact);
                return compact;
            }
        }
    }

    /// Every kernel below assumes wrapper-free input at every nesting level; LowCardinality is kept.
    Columns normalized_holder;
    ColumnRawPtrs normalized_sources;
    normalized_holder.reserve(sources.size());
    normalized_sources.reserve(sources.size());
    for (const IColumn * source : sources)
    {
        normalized_holder.push_back(source->convertToFullIfWrapped());
        normalized_sources.push_back(normalized_holder.back().get());
    }
    sources = {normalized_sources.data(), normalized_sources.size()};

    const TypeIndex type = sources[0]->getDataType();
    const ScatterKernelId kernel_id = KERNEL_ID_TABLE[static_cast<size_t>(type)];

#ifdef DEBUG_OR_SANITIZER_BUILD
    /// Release builds get only TypeIndex plus the width and arity guards inside the kernels. Every
    /// type with a dedicated kernel implements `structureEquals`; the fallback types need not.
    if (kernel_id != ScatterKernelId::Fallback)
        for (size_t b = 1; b < sources.size(); ++b)
            chassert(sources[b]->structureEquals(*sources[0]));
#endif

    if (!fits_32)
    {
        /// `IColumn::scatter` sizes destinations itself from 64-bit counts, so UInt32 counting is
        /// skipped; zero counts only cost the reserve.
        traceDispatch(type, ScatterKernelId::Fallback);
        PaddedPODArray<UInt32> zero_counts;
        zero_counts.resize_fill(num_shards, 0);
        return scatterFallback<Pid>(sources, pids_per_source, std::span<const UInt32>(zero_counts.data(), num_shards));
    }

    traceDispatch(type, kernel_id);
    return dispatchToKernel<Pid>(sources, pids_per_source, ensure_counts());
}

}


void scatterPidChunk(size_t width, const UInt16 * pids, const char * data, size_t n, bool use_swwc, ScatterScratch & scratch)
{
    scatterPidChunkImpl(width, pids, data, n, use_swwc, scratch);
}

void histogramPidChunk(const UInt16 * pids, size_t n, UInt64 * hist, UInt64 * lanes, size_t fanout)
{
    histogramPidChunkImpl(pids, n, hist, lanes, fanout);
}

void reduceHistogramLanes(UInt64 * hist, const UInt64 * lanes, size_t fanout)
{
    reduceHistogramLanesImpl(hist, lanes, fanout);
}

std::vector<size_t> computePassBits(size_t fanout, size_t max_fanout_per_pass) /// STYLE_CHECK_ALLOW_STD_CONTAINERS
{
    chassert(std::has_single_bit(fanout));
    const size_t total_bits = static_cast<size_t>(std::countr_zero(fanout));
    const size_t pass_bits = std::max<size_t>(
        1, static_cast<size_t>(std::countr_zero(std::bit_floor(std::max<size_t>(2, max_fanout_per_pass)))));
    const size_t num_passes = (total_bits + pass_bits - 1) / pass_bits;
    const size_t per_pass = num_passes ? (total_bits + num_passes - 1) / num_passes : 0;

    std::vector<size_t> result; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    size_t remaining = total_bits;
    while (remaining > 0)
    {
        const size_t bits = std::min(per_pass, remaining);
        result.push_back(bits);
        remaining -= bits;
    }
    return result;
}

std::pair<MutableColumnPtr, std::span<char>> allocateUninitializedFixed(const IColumn & sample, size_t rows)
{
    auto column = sample.cloneEmpty();
    /// `insertRawUninitialized` grows power-of-two because it serves append loops; reserving first
    /// makes this allocation exact, as the contract promises.
    column->reserve(rows);
    auto raw = column->insertRawUninitialized(rows);
    chassert(raw.size() == rows * sample.sizeOfValueIfFixed());
    return {std::move(column), raw};
}

ScatterKernelId plannedKernel(const IColumn & column)
{
    return KERNEL_ID_TABLE[static_cast<size_t>(column.getDataType())];
}

DispatchTrace * exchangeDispatchTrace(DispatchTrace * trace)
{
    return std::exchange(dispatch_trace, trace);
}

void countRowsPerShard(std::span<const std::span<const UInt16>> pids_per_source, std::span<UInt32> rows_per_shard)
{
    countRowsPerShardImpl(pids_per_source, rows_per_shard);
}

MutableColumns scatter(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt16>> pids_per_source,
    size_t num_shards,
    std::span<const UInt32> rows_per_shard)
{
    return scatterImpl(source_columns, pids_per_source, num_shards, rows_per_shard);
}

}
