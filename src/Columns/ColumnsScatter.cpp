#include <Columns/ColumnsScatter.h>

#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <base/types.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>
#include <Common/memcpySmall.h>

#include <absl/container/inlined_vector.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <limits>
#include <span>
#include <type_traits>

namespace DB::ColumnsScatter
{

namespace
{

/// Inline capacity of the per-partition scratch arrays (write-pointer caches,
/// per-shard byte cursors, rows_per_shard scratch). Each kernel stores these in an
/// `absl::InlinedVector<_, SCATTER_INLINE_SHARDS>`: for `num_shards <=
/// SCATTER_INLINE_SHARDS` the storage lives entirely on the stack — same hot
/// path as a fixed C array, no heap allocation, no per-row branching — and only
/// the rare `num_shards > SCATTER_INLINE_SHARDS` case spills to the heap.
///
/// 256 covers the typical sharded-aggregation range (`num_shards == max_threads`);
/// larger shard counts remain correct via the heap spill, so there is no hard
/// upper bound on `num_shards`.
constexpr size_t SCATTER_INLINE_SHARDS = 256;

/// Inline capacity for the per-flush list of source columns (one per pending chunk),
/// used only on the rare path that materializes transparent wrappers before dispatch.
constexpr size_t SCATTER_INLINE_SOURCES = 64;

/// Type lists driving the O(1) dispatch table below. Each entry is a `TypeIndex`
/// enumerator whose physical column maps to `scatterFixed`/`scatterDecimal`.
/// Adding a fast-path numeric/decimal type is a one-token edit here.
#define SCATTER_FIXED_TYPES(M) \
    M(UInt8) \
    M(UInt16) \
    M(UInt32) \
    M(UInt64) \
    M(UInt128) M(UInt256) M(Int8) M(Int16) M(Int32) M(Int64) M(Int128) M(Int256) M(BFloat16) M(Float32) M(Float64) M(UUID) M(IPv4) M(IPv6)

#define SCATTER_DECIMAL_TYPES(M) M(Decimal32) M(Decimal64) M(Decimal128) M(Decimal256) M(DateTime64) M(Time64)

// ── Fixed-width scatter ───────────────────────────────────────────────────────
//
// One call handles *all* source columns for one column-position. The write
// pointers are set up once before the outer B-loop and persist across chunks
// — this is the key cross-chunk amortisation: no per-chunk pointer refresh.
//
// Why T ** wp?
//   1. It IS the per-partition running write cursor; without it the inner loop
//      pays two extra indirections per row.
//   2. Typed T * makes ++ advance by sizeof(T) in one instruction (no cast).
//   3. Known-non-aliasing with the inputs lets the compiler keep hot wp[p]
//      slots in registers when pids[j] clusters on one partition.

template <typename T>
[[gnu::hot]] MutableColumns scatterFixed(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();

    /// Write-pointer cache: inline (stack) for num_shards <= SCATTER_INLINE_SHARDS,
    /// heap-backed beyond. The hot loop indexes the raw `wp` data pointer, so its
    /// codegen is identical to the previous fixed C array.
    absl::InlinedVector<T *, SCATTER_INLINE_SHARDS> wp_storage(num_shards);
    T ** wp = wp_storage.data();

    // Allocate destinations with their final size already committed, and
    // initialise write pointers in the same pass — no separate commit loop needed.
    MutableColumns dst(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
    {
        auto col = ColumnVector<T>::create();
        col->getData().reserve_exact(rows_per_shard[s]);
        col->getData().resize_assume_reserved(rows_per_shard[s]);
        wp[s] = col->getData().data();
        dst[s] = std::move(col);
    }

    // Single pass: hot loop — write pointers persist across all source columns.
    for (size_t b = 0; b < source_columns.size(); ++b)
    {
        const T * src = assert_cast<const ColumnVector<T> &>(*source_columns[b]).getData().data();
        const auto & pids = pids_per_source[b];
        for (size_t j = 0; j < pids.size(); ++j)
            *wp[pids[j]]++ = src[j]; // 3-insn inner loop
    }

    return dst;
}

// ── Decimal scatter ───────────────────────────────────────────────────────────
// Decimal<T> wraps a NativeType with identical memory layout; reinterpret and
// reuse the fixed-width path.

template <typename DecimalT>
[[gnu::hot]] MutableColumns scatterDecimal(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard)
{
    using NativeT = DecimalT::NativeType;

    const size_t num_shards = rows_per_shard.size();

    // Scale is taken from the first source column (all sources must share it).
    const auto & probe_col = assert_cast<const ColumnDecimal<DecimalT> &>(*source_columns[0]);
    const UInt32 scale = probe_col.getScale();

#ifdef DEBUG_OR_SANITIZER_BUILD
    for (size_t b = 1; b < source_columns.size(); ++b)
        chassert(assert_cast<const ColumnDecimal<DecimalT> &>(*source_columns[b]).getScale() == scale);
#endif

    absl::InlinedVector<NativeT *, SCATTER_INLINE_SHARDS> wp_storage(num_shards);
    NativeT ** wp = wp_storage.data();

    MutableColumns dst(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
    {
        auto col = ColumnDecimal<DecimalT>::create(0, scale);
        col->getData().reserve_exact(rows_per_shard[s]);
        col->getData().resize_assume_reserved(rows_per_shard[s]);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        wp[s] = reinterpret_cast<NativeT *>(col->getData().data());
        dst[s] = std::move(col);
    }

    for (size_t b = 0; b < source_columns.size(); ++b)
    {
        const auto & col = assert_cast<const ColumnDecimal<DecimalT> &>(*source_columns[b]);
        // NOLINTNEXTLINE(cppcoreguidelines-pro-type-reinterpret-cast)
        const NativeT * src = reinterpret_cast<const NativeT *>(col.getData().data());
        const auto & pids = pids_per_source[b];
        for (size_t j = 0; j < pids.size(); ++j)
            *wp[pids[j]]++ = src[j];
    }

    return dst;
}

// ── FixedString scatter ───────────────────────────────────────────────────────
// Runtime element size n = getN(); SWWC not supported (slot count not compile-time).

[[gnu::hot]] MutableColumns scatterFixedString(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard)
{
    const size_t n = assert_cast<const ColumnFixedString &>(*source_columns[0]).getN();
    const size_t num_shards = rows_per_shard.size();

    absl::InlinedVector<UInt8 *, SCATTER_INLINE_SHARDS> wp_storage(num_shards);
    UInt8 ** wp = wp_storage.data();

    MutableColumns dst(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
    {
        auto col = ColumnFixedString::create(n);
        col->getChars().reserve_exact(rows_per_shard[s] * n);
        col->getChars().resize_assume_reserved(rows_per_shard[s] * n);
        wp[s] = col->getChars().data();
        dst[s] = std::move(col);
    }

    /// `n` is typically small (1-50 B for UUIDs, IPs, fixed-length codes).
    /// `memcpySmallAllowReadWriteOverflow15` inlines a tight SSE loop that
    /// outperforms libc memcpy on these sizes. Safe because:
    ///   - ColumnFixedString::getChars() is PaddedPODArray with 63-byte right pad.
    ///   - Destination chars were just reserve_exact'd; same right pad on the
    ///     fresh allocation. Writing 15 bytes past the last `n` of the last
    ///     row of the last shard lands inside the pad.
    for (size_t b = 0; b < source_columns.size(); ++b)
    {
        const auto & col = assert_cast<const ColumnFixedString &>(*source_columns[b]);
        const UInt8 * src = col.getChars().data();
        const auto & pids = pids_per_source[b];
        for (size_t j = 0; j < pids.size(); ++j)
        {
            const UInt32 s = pids[j];
            memcpySmallAllowReadWriteOverflow15(wp[s], src + j * n, n);
            wp[s] += n;
        }
    }

    return dst;
}

// ── String scatter ────────────────────────────────────────────────────────────
// Standalone fused kernel: chars + offsets + running byte cursor cannot be
// factored into scatterFixed<UInt64> — each emitted offset is a per-partition
// running cursor, not a copy of a source value.
//
// Row counts are supplied via `rows_per_shard` (pre-computed by the caller).
// A single pass here computes only the per-shard BYTE counts, then scatters.

[[gnu::hot]] MutableColumns scatterString(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();

    // Single pass over source data: compute per-shard byte counts.
    // Row counts come from the pre-computed rows_per_shard — no redundant pids scan.
    PODArray<UInt64> per_shard_bytes(num_shards, 0);
    for (size_t b = 0; b < source_columns.size(); ++b)
    {
        const auto & off = assert_cast<const ColumnString &>(*source_columns[b]).getOffsets();
        const auto & pids = pids_per_source[b];
        UInt64 prev = 0;
        for (size_t j = 0; j < pids.size(); ++j)
        {
            const UInt64 end = off[j];
            per_shard_bytes[pids[j]] += end - prev;
            prev = end;
        }
    }

    /// Triple per-partition scratch arrays: offsets writer, chars writer,
    /// running absolute byte cursor. Inline (stack) for num_shards <=
    /// SCATTER_INLINE_SHARDS, heap-backed beyond. The hot loop indexes the raw
    /// data pointers, so codegen matches the previous fixed C arrays.
    /// UInt8 = char8_t (ClickHouse convention).
    absl::InlinedVector<UInt64 *, SCATTER_INLINE_SHARDS> off_ptrs_storage(num_shards);
    absl::InlinedVector<UInt8 *, SCATTER_INLINE_SHARDS> char_ptrs_storage(num_shards);
    absl::InlinedVector<UInt64, SCATTER_INLINE_SHARDS> abs_byte_storage(num_shards);
    UInt64 ** off_ptrs = off_ptrs_storage.data();
    UInt8 ** char_ptrs = char_ptrs_storage.data();
    UInt64 * abs_byte = abs_byte_storage.data();

    // Allocate destinations with final sizes already committed, and initialise
    // write pointers in the same pass — no separate commit loop needed.
    MutableColumns dst(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
    {
        auto col = ColumnString::create();
        col->getChars().reserve_exact(per_shard_bytes[s]);
        col->getChars().resize_assume_reserved(per_shard_bytes[s]);
        col->getOffsets().reserve_exact(rows_per_shard[s]);
        col->getOffsets().resize_assume_reserved(rows_per_shard[s]);
        char_ptrs[s] = col->getChars().data();
        off_ptrs[s] = col->getOffsets().data();
        abs_byte[s] = 0;
        dst[s] = std::move(col);
    }

    /// Scatter pass: fused hot loop — write pointers persist across source columns.
    /// String lengths are typically small (≤ 50 B for most workloads).
    /// `memcpySmallAllowReadWriteOverflow15` inlines a tight SSE loop that
    /// outperforms libc memcpy on these sizes. Safe because:
    ///   - ColumnString::getChars() is PaddedPODArray with 63-byte right pad.
    ///   - Destination chars were just reserve_exact'd; the 15-byte tail write
    ///     past the last string of the last shard lands inside its pad.
    for (size_t b = 0; b < source_columns.size(); ++b)
    {
        const auto & col = assert_cast<const ColumnString &>(*source_columns[b]);
        const auto & off = col.getOffsets();
        const UInt8 * src_chars = col.getChars().data();
        const auto & pids = pids_per_source[b];

        UInt64 prev = 0;
        for (size_t j = 0; j < pids.size(); ++j)
        {
            const UInt64 end = off[j];
            const size_t len = static_cast<size_t>(end - prev);
            const UInt32 s = pids[j];
            if (len)
                memcpySmallAllowReadWriteOverflow15(char_ptrs[s], src_chars + prev, len);
            char_ptrs[s] += len;
            abs_byte[s] += len;
            *off_ptrs[s]++ = abs_byte[s];
            prev = end;
        }
    }

    return dst;
}

// ── Fallback ──────────────────────────────────────────────────────────────────
// Per-source-column, per-shard: delegate to legacy IColumn::scatter().
// Correct but allocating; used only for column types without a fast path.
// Does not use rows_per_shard (IColumn::scatter computes counts internally).

[[nodiscard]] MutableColumns scatterFallback(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();

    // Create empty destinations from the first source column.
    MutableColumns dst(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        dst[s] = source_columns[0]->cloneEmpty();

    // Hoist the selector; resize it per source column instead of reallocating.
    IColumn::Selector sel;

    for (size_t b = 0; b < source_columns.size(); ++b)
    {
        const auto & pids = pids_per_source[b];
        const size_t n_rows = pids.size();

        // Widen UInt32 pids to UInt64 selector for legacy API.
        sel.resize(n_rows);
        for (size_t j = 0; j < n_rows; ++j)
            sel[j] = pids[j];

        auto parts = source_columns[b]->scatter(num_shards, sel);
        for (size_t s = 0; s < num_shards; ++s)
            dst[s]->insertRangeFrom(*parts[s], 0, parts[s]->size());
    }

    return dst;
}

// ── Forward declarations for composite types ──────────────────────────────────

[[nodiscard]] MutableColumns scatterNullable(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard);

[[nodiscard]] MutableColumns scatterTuple(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard);

// ── O(1) dispatch table ───────────────────────────────────────────────────────
// All kernels share one signature so they can sit in a single function-pointer
// table indexed by TypeIndex. Dispatch is one getDataType() virtual call, one
// table load, one indirect call — independent of the number of supported types.

using ScatterKernel
    = MutableColumns (*)(std::span<const IColumn * const>, std::span<const std::span<const UInt32>>, std::span<const UInt32>);

/// TypeIndex's underlying type spans the whole index domain, so a table of this
/// size makes `table[static_cast<size_t>(idx)]` unconditionally in-bounds — no
/// literal `256` and no range check needed.
constexpr size_t SCATTER_TABLE_SIZE = static_cast<size_t>(std::numeric_limits<std::underlying_type_t<TypeIndex>>::max()) + 1;

constexpr std::array<ScatterKernel, SCATTER_TABLE_SIZE> buildScatterTable()
{
    std::array<ScatterKernel, SCATTER_TABLE_SIZE> table{};

    /// Every unmapped slot lands on the allocating legacy path.
    table.fill(&scatterFallback);

#define M(TYPE) table[static_cast<size_t>(TypeIndex::TYPE)] = &scatterFixed<TYPE>;
    SCATTER_FIXED_TYPES(M)
#undef M

#define M(TYPE) table[static_cast<size_t>(TypeIndex::TYPE)] = &scatterDecimal<TYPE>;
    SCATTER_DECIMAL_TYPES(M)
#undef M

    table[static_cast<size_t>(TypeIndex::FixedString)] = &scatterFixedString;
    table[static_cast<size_t>(TypeIndex::String)] = &scatterString;
    table[static_cast<size_t>(TypeIndex::Nullable)] = &scatterNullable;
    table[static_cast<size_t>(TypeIndex::Tuple)] = &scatterTuple;

    return table;
}

#undef SCATTER_FIXED_TYPES
#undef SCATTER_DECIMAL_TYPES

// ── Internal dispatch: row counts already computed ────────────────────────────
// All typed kernels receive rows_per_shard — no redundant pids re-scan.
// Composite types (Nullable, Tuple) forward rows_per_shard into their recursive
// scatter calls so row counts are never recomputed for nested types either.

[[nodiscard]] MutableColumns scatterDispatch(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard)
{
    const IColumn & probe = *source_columns[0];

    /// Transparent wrappers report their *nested* type from getDataType(), so a
    /// raw table lookup would mis-route them into a fast-path kernel that
    /// assert_casts onto the wrong concrete column. Route them to the fallback
    /// before indexing the table. All other unsupported types report their own
    /// distinct index and land on the fallback default slot.
    if (probe.isConst() || probe.isSparse() || probe.isReplicated())
        return scatterFallback(source_columns, pids_per_source, rows_per_shard);

    static constexpr auto table = buildScatterTable();
    return table[static_cast<size_t>(probe.getDataType())](source_columns, pids_per_source, rows_per_shard);
}

/// Materialize only the *transparent* wrappers the dispatch needs to erase —
/// `ColumnConst`/`ColumnReplicated`/`ColumnSparse` — while deliberately preserving
/// `ColumnLowCardinality`. `IColumn::convertToFullIfNeeded` would additionally strip
/// `LowCardinality` (and recurse into subcolumns doing the same), turning e.g. a
/// `ColumnConst(ColumnLowCardinality(String))` under a `LowCardinality(String)` header
/// into a plain `ColumnString`; the scattered chunk would then carry a physical column
/// whose type no longer matches the transform's port/header contract. A full
/// `ColumnLowCardinality` is handled correctly downstream: it reports
/// `TypeIndex::LowCardinality`, which is an unmapped dispatch slot, so `scatterDispatch`
/// routes it to `scatterFallback`, which clones an empty `ColumnLowCardinality` and
/// scatters through `IColumn::scatter`, preserving the type. Returns `getPtr()` (no copy)
/// when nothing needs materializing.
ColumnPtr materializeTransparentWrappers(const IColumn & col)
{
    return col.convertToFullColumnIfConst()
        ->convertToFullColumnIfReplicated()
        ->convertToFullColumnIfSparse();
}

} // anonymous namespace

// ── Public entry point ────────────────────────────────────────────────────────

void countRowsPerShard(std::span<const std::span<const UInt32>> pids_per_source, std::span<UInt32> rows_per_shard)
{
    for (const auto & pids : pids_per_source)
        for (UInt32 p : pids)
            rows_per_shard[p]++;
}

MutableColumns scatter(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    size_t num_shards,
    std::span<const UInt32> rows_per_shard)
{
    chassert(!source_columns.empty());
    chassert(source_columns.size() == pids_per_source.size());
    chassert(num_shards > 0);

#ifdef DEBUG_OR_SANITIZER_BUILD
    const IColumn & probe = *source_columns[0];
    /// All source columns must share the same logical type. Concrete representation
    /// may differ across chunks (full vs ColumnConst/ColumnSparse/...) — that is
    /// normalized away below before dispatch.
    for (size_t b = 1; b < source_columns.size(); ++b)
        chassert(probe.getDataType() == source_columns[b]->getDataType());
#endif

    /// Source columns at one position are only guaranteed to share the same logical
    /// type, not the same concrete representation: a position can mix a materialized
    /// column with a `ColumnConst`/`ColumnSparse`/`ColumnReplicated` (e.g. after
    /// constant folding or `UNION`). The typed kernels `assert_cast` every source to
    /// the probe's concrete class and `getDataType()` is representation-blind, so a
    /// mixed batch would mis-cast or materialize wrong values. Materialize any
    /// transparent wrappers up front so the dispatch below sees a single representation.
    /// `materializeTransparentWrappers` strips only `Const`/`Replicated`/`Sparse`
    /// (preserving `LowCardinality`, whose type must not change) and returns `getPtr()`
    /// (no copy) for already-full columns, so this only costs anything on the rare
    /// mixed/wrapped batch.
    absl::InlinedVector<ColumnPtr, SCATTER_INLINE_SOURCES> materialized;
    absl::InlinedVector<const IColumn *, SCATTER_INLINE_SOURCES> full_ptrs;
    const auto any_wrapper = std::any_of(
        source_columns.begin(), source_columns.end(),
        [](const IColumn * col)
        {
            return col->isConst() || col->isSparse() || col->isReplicated();
    });

    if (any_wrapper)
    {
        const size_t num_sources = source_columns.size();
        materialized.reserve(num_sources);
        full_ptrs.reserve(num_sources);
        for (const IColumn * col : source_columns)
        {
            materialized.push_back(materializeTransparentWrappers(*col));
            full_ptrs.push_back(materialized.back().get());
        }
        source_columns = std::span<const IColumn * const>(full_ptrs.data(), num_sources);
    }

    /// Guard against UInt32 per-shard row-count overflow. The fast kernels reserve and
    /// size their destinations from UInt32 counts, but the write pointers still emit
    /// every row; if a single flush routed more than UINT32_MAX rows to one shard the
    /// count would wrap and the kernels would write out of bounds. No shard can hold
    /// more rows than the whole batch, so when the batch exceeds UINT32_MAX rows fall
    /// back to the size_t-safe per-source path (its destinations grow dynamically).
    {
        const auto total_rows = std::accumulate(
            pids_per_source.begin(), pids_per_source.end(), size_t{0}, [](auto sum, const auto & pids) { return sum + pids.size(); });
        if (total_rows > std::numeric_limits<UInt32>::max()) [[unlikely]]
        {
            absl::InlinedVector<UInt32, SCATTER_INLINE_SHARDS> shard_count_placeholder(num_shards, 0);
            return scatterFallback(source_columns, pids_per_source, std::span<const UInt32>(shard_count_placeholder.data(), num_shards));
        }
    }

    /// Row counts provided by caller — skip the per-call pids re-scan.
    if (!rows_per_shard.empty())
    {
        chassert(rows_per_shard.size() == num_shards);
        return scatterDispatch(source_columns, pids_per_source, rows_per_shard);
    }

    /// Convenience path: count rows per shard internally (single-column callers).
    /// Inline (stack) for num_shards <= SCATTER_INLINE_SHARDS, heap-backed beyond;
    /// zero-initialised on construction.
    absl::InlinedVector<UInt32, SCATTER_INLINE_SHARDS> rows_per_shard_buf(num_shards, 0);
    countRowsPerShard(pids_per_source, std::span<UInt32>(rows_per_shard_buf.data(), num_shards));
    return scatterDispatch(source_columns, pids_per_source, rows_per_shard_buf);
}

namespace
{

/// Materialize transparent wrappers (Const/Sparse/Replicated) collected from a composite
/// column's subcolumns so the recursive scatterDispatch sees a single concrete
/// representation per position. The top-level scatter() only normalizes the outer column,
/// not subcolumns that differ across chunks — e.g. a sparse tuple element in one chunk and
/// a full one in another. `LowCardinality` is deliberately left intact (see
/// `materializeTransparentWrappers`). Materialized columns are owned by `holder`, which must
/// outlive the dispatch call. No-op (no copy) when every column is already full.
void materializeWrappers(
    std::vector<const IColumn *> & cols, // STYLE_CHECK_ALLOW_STD_CONTAINERS
    absl::InlinedVector<ColumnPtr, SCATTER_INLINE_SOURCES> & holder)
{
    bool any_wrapper = false;
    for (const IColumn * c : cols)
        if (c->isConst() || c->isSparse() || c->isReplicated())
        {
            any_wrapper = true;
            break;
        }
    if (!any_wrapper)
        return;

    holder.reserve(cols.size());
    for (const IColumn *& c : cols)
    {
        holder.push_back(materializeTransparentWrappers(*c));
        c = holder.back().get();
    }
}

// ── Nullable scatter ──────────────────────────────────────────────────────────
// Null map is just a ColumnVector<UInt8> — reuse scatterFixed<UInt8>.
// Forward the pre-computed rows_per_shard to both the null-map and the nested call.

MutableColumns scatterNullable(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();

    // Scatter null maps — ColumnVector<UInt8> path, no row-count re-scan.
    std::vector<const IColumn *> null_map_cols(source_columns.size()); // STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (size_t b = 0; b < source_columns.size(); ++b)
        null_map_cols[b] = &assert_cast<const ColumnNullable &>(*source_columns[b]).getNullMapColumn();

    MutableColumns scattered_null_maps = scatterFixed<UInt8>(null_map_cols, pids_per_source, rows_per_shard);

    // Scatter nested columns — forward rows_per_shard into recursive dispatch.
    std::vector<const IColumn *> nested_cols(source_columns.size()); // STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (size_t b = 0; b < source_columns.size(); ++b)
        nested_cols[b] = &assert_cast<const ColumnNullable &>(*source_columns[b]).getNestedColumn();

    /// Nested columns can differ in concrete representation across chunks (e.g. sparse in
    /// one, full in another); normalize before the recursive dispatch.
    absl::InlinedVector<ColumnPtr, SCATTER_INLINE_SOURCES> nested_holder;
    materializeWrappers(nested_cols, nested_holder);

    MutableColumns scattered_nested = scatterDispatch(nested_cols, pids_per_source, rows_per_shard);

    // Wrap into ColumnNullable.
    MutableColumns dst(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        dst[s] = ColumnNullable::create(std::move(scattered_nested[s]), std::move(scattered_null_maps[s]));

    return dst;
}

// ── Tuple scatter ─────────────────────────────────────────────────────────────

MutableColumns scatterTuple(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();

    const auto & probe_tuple = assert_cast<const ColumnTuple &>(*source_columns[0]);
    const size_t num_elements = probe_tuple.getColumns().size();

    // ColumnTuple(size_t) is the dedicated constructor for zero-element ("Unit") tuples.
    // ColumnTuple::create(MutableColumns&&) with an empty vector triggers LOGICAL_ERROR.
    if (num_elements == 0)
    {
        MutableColumns dst(num_shards);
        for (size_t s = 0; s < num_shards; ++s)
            dst[s] = ColumnTuple::create(rows_per_shard[s]);
        return dst;
    }

    // For each tuple element position, collect the element column from every source.
    std::vector<std::vector<const IColumn *>> element_cols(num_elements, std::vector<const IColumn *>(source_columns.size())); // STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (size_t b = 0; b < source_columns.size(); ++b)
    {
        const auto & tup = assert_cast<const ColumnTuple &>(*source_columns[b]);
        for (size_t e = 0; e < num_elements; ++e)
            element_cols[e][b] = &tup.getColumn(e);
    }

    // Scatter each element — forward the same rows_per_shard to every element scatter.
    // Element columns can differ in concrete representation across chunks (e.g. a sparse
    // element in one chunk, full in another); normalize before the recursive dispatch.
    std::vector<MutableColumns> scattered_elements(num_elements); // STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (size_t e = 0; e < num_elements; ++e)
    {
        absl::InlinedVector<ColumnPtr, SCATTER_INLINE_SOURCES> elem_holder;
        materializeWrappers(element_cols[e], elem_holder);
        scattered_elements[e] = scatterDispatch(element_cols[e], pids_per_source, rows_per_shard);
    }

    // Assemble per-shard ColumnTuple.
    MutableColumns dst(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
    {
        MutableColumns shard_elements(num_elements);
        for (size_t e = 0; e < num_elements; ++e)
            shard_elements[e] = std::move(scattered_elements[e][s]);
        dst[s] = ColumnTuple::create(std::move(shard_elements));
    }

    return dst;
}

} // anonymous namespace

}
