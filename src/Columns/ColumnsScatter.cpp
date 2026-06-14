#include <Columns/ColumnsScatter.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnVector.h>
#include <Columns/ColumnsNumber.h>
#include <base/types.h>
#include <Common/Arena.h>
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
/// used by the normalization pass that strips transparent wrappers before dispatch.
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
    /// heap-backed beyond. The hot loop indexes the raw `wp` data pointer.
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
// Runtime element size n = getN().

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
    /// data pointers.
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

// ── Representation normalization ───────────────────────────────────────────────
// The single boundary normalizer. scatter() applies it once per source, so dispatch,
// the typed kernels, the composite kernels, and the fallback all assume wrapper-free input.

/// Recursively strip Const/Sparse/Replicated at every nesting level (generic via
/// forEachSubcolumn, the same mechanism as IColumn::convertToFullIfNeeded), while preserving
/// LowCardinality as a leaf so its type still matches the transform's port/header contract.
/// Returns the same ColumnPtr when nothing changed (cheap no-op for already-full leaves).
ColumnPtr normalizeRepresentation(ColumnPtr col)
{
    col = col->convertToFullColumnIfConst()
              ->convertToFullColumnIfReplicated()
              ->convertToFullColumnIfSparse();

    /// LowCardinality is preserved (not stripped) and treated as a leaf.
    if (col->getDataType() == TypeIndex::LowCardinality)
        return col;

    Columns new_subcolumns;
    bool any_changed = false;
    col->forEachSubcolumn(
        [&](const IColumn::WrappedPtr & subcolumn)
        {
            auto new_sub = normalizeRepresentation(subcolumn->getPtr());
            any_changed |= (new_sub.get() != subcolumn.get());
            new_subcolumns.push_back(std::move(new_sub));
        });

    if (!any_changed)
        return col;

    auto mutable_col = IColumn::mutate(std::move(col));
    size_t i = 0;
    mutable_col->forEachMutableSubcolumn([&](IColumn::WrappedPtr & subcolumn) { subcolumn = std::move(new_subcolumns[i++]); });
    return std::move(mutable_col);
}

// ── Fallback ──────────────────────────────────────────────────────────────────
// Per-source, per-shard delegation to the legacy IColumn::scatter() + insertRangeFrom.
// Correct but allocating; used for any type without a fast path. Inputs are pre-normalized
// by every caller, so nested concrete representations are uniform across sources (required:
// IColumn::scatter preserves each source's nested representation and insertRangeFrom
// assert_casts the concrete nested type).

[[nodiscard]] MutableColumns scatterFallback(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard)
{
    const size_t num_shards = rows_per_shard.size();

    MutableColumns dst(num_shards);
    for (size_t s = 0; s < num_shards; ++s)
        dst[s] = source_columns[0]->cloneEmpty();

    // Hoist the selector; resize it per source column instead of reallocating.
    IColumn::Selector sel;
    for (size_t b = 0; b < source_columns.size(); ++b)
    {
        const auto & pids = pids_per_source[b];
        sel.resize(pids.size());
        for (size_t j = 0; j < pids.size(); ++j) // widen UInt32 pids to UInt64 selector
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

// ── Internal dispatch ─────────────────────────────────────────────────────────
// O(1) TypeIndex table lookup. All kernels receive the pre-computed rows_per_shard;
// composite kernels (Nullable, Tuple) recurse here for their nested types.

[[nodiscard]] MutableColumns scatterDispatch(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    std::span<const UInt32> rows_per_shard)
{
    /// Input is wrapper-free (normalized at the scatter() boundary). LowCardinality reports an
    /// unmapped slot and routes to scatterFallback via the table default, preserving its type.
    static constexpr auto table = buildScatterTable();
    return table[static_cast<size_t>(source_columns[0]->getDataType())](source_columns, pids_per_source, rows_per_shard);
}

/// True for composite types whose subcolumns may hide Const/Sparse/Replicated that a
/// top-level wrapper probe cannot see (e.g. a full Tuple wrapping a sparse element). Leaf
/// fast-path types return false, so the hot path skips normalization entirely.
[[nodiscard]] inline bool probeMayHaveSubcolumns(const IColumn * col)
{
    switch (col->getDataType())
    {
        case TypeIndex::Tuple:
        case TypeIndex::Nullable:
        case TypeIndex::Array:
        case TypeIndex::Map:
        case TypeIndex::Variant:
        case TypeIndex::Dynamic:
        case TypeIndex::Object:
            return true;
        default:
            return false;
    }
}

/// Normalize every source once (recursive), holding the rebuilt columns in `holder` (which
/// must outlive the dispatch) and returning a span of borrowed pointers into them.
std::span<const IColumn * const> normalizeAll(
    std::span<const IColumn * const> source_columns,
    absl::InlinedVector<ColumnPtr, SCATTER_INLINE_SOURCES> & holder,
    absl::InlinedVector<const IColumn *, SCATTER_INLINE_SOURCES> & ptrs)
{
    holder.reserve(source_columns.size());
    ptrs.reserve(source_columns.size());
    for (const IColumn * col : source_columns)
    {
        holder.push_back(normalizeRepresentation(col->getPtr()));
        ptrs.push_back(holder.back().get());
    }
    return std::span<const IColumn * const>(ptrs.data(), ptrs.size());
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

    /// UInt32 per-shard count overflow: the fast kernels (and the all-const compact path
    /// below) size destinations from UInt32 counts, so a flush routing > UINT32_MAX rows to
    /// one shard would wrap. Divert to the size_t-safe fallback. Must run before the all-const
    /// compact path, which also uses UInt32 counts.
    {
        const auto total_rows = std::accumulate(
            pids_per_source.begin(), pids_per_source.end(), size_t{0}, [](auto sum, const auto & pids) { return sum + pids.size(); });
        if (total_rows > std::numeric_limits<UInt32>::max()) [[unlikely]]
        {
            /// Normalize here so scatterFallback can assume wrapper-free input (its callers all do).
            absl::InlinedVector<ColumnPtr, SCATTER_INLINE_SOURCES> overflow_holder;
            absl::InlinedVector<const IColumn *, SCATTER_INLINE_SOURCES> overflow_ptrs;
            const auto normalized_sources = normalizeAll(source_columns, overflow_holder, overflow_ptrs);
            absl::InlinedVector<UInt32, SCATTER_INLINE_SHARDS> shard_count_placeholder(num_shards, 0);
            return scatterFallback(normalized_sources, pids_per_source, std::span<const UInt32>(shard_count_placeholder.data(), num_shards));
        }
    }

    /// Fast compact path for a homogeneous batch of equal-valued `ColumnConst` sources.
    /// `ColumnConst::scatter` expands the constant into one value per row per shard,
    /// but when all sources carry the same constant value, shard s only needs
    /// `ColumnConst(value, rows_per_shard[s])` — no materialization required. This keeps
    /// memory O(1) for constant-key workloads (e.g. `GROUP BY toUInt64(1)`) and avoids
    /// expanding large const-heavy batches that `BufferedShardByHashTransform`
    /// accumulates before a flush.
    const bool all_const = std::all_of(
        source_columns.begin(), source_columns.end(), [](const IColumn * c) { return c->isConst(); });
    if (all_const)
    {
        /// A single const source is trivially equal to itself: shard s just needs
        /// `rows_per_shard[s]` copies of that one value, which is exactly what the legacy
        /// `ColumnConst::scatter` produces (`cloneResized` re-wraps the same data, no
        /// materialization). Because this needs no value comparison it also works for const
        /// wrappers whose nested value cannot be serialized (e.g. `ColumnConst(ColumnFunction)`),
        /// where the legacy path simply cloned — serializing such a value just to compare it
        /// would throw `NOT_IMPLEMENTED` and turn this optimization into a regression.
        bool all_same_value = source_columns.size() == 1;

        /// Multiple const sources (a batch of chunks) may carry bit-different — but SQL-equal —
        /// values, so the compact clone is only safe when every const holds the byte-identical
        /// value. Byte-exact equality, not `compareAt`: scatter is a physical split of *every*
        /// column (not just keys), so this compact path must preserve the exact source bytes.
        /// `compareAt` is ordering-equality — it treats `+0.0`/`-0.0` and all `NaN` payloads as
        /// equal — so cloning `source_columns[0]` for a shard that received rows from a
        /// bit-different const chunk would silently rewrite their payload. Compare the serialized
        /// single value of each const; any bit difference falls through to the materializing
        /// dispatch below. The comparison needs serialization, so when the nested value is not
        /// serializable (`ColumnFunction`) there is no byte-exact test available: skip the
        /// optimization and let the materializing dispatch handle the batch rather than throwing.
        if (!all_same_value
            && assert_cast<const ColumnConst &>(*source_columns[0]).getDataColumn().getDataType() != TypeIndex::Function)
        {
            Arena arena;
            const char * probe_begin = nullptr;
            const std::string_view probe_bytes
                = assert_cast<const ColumnConst &>(*source_columns[0]).getDataColumn().serializeValueIntoArena(0, arena, probe_begin, nullptr);
            all_same_value = std::all_of(
                source_columns.begin() + 1, source_columns.end(),
                [&](const IColumn * c)
                {
                    const char * begin = nullptr;
                    return assert_cast<const ColumnConst &>(*c).getDataColumn().serializeValueIntoArena(0, arena, begin, nullptr) == probe_bytes;
                });
        }

        if (all_same_value)
        {
            absl::InlinedVector<UInt32, SCATTER_INLINE_SHARDS> rps_buf;
            std::span<const UInt32> rps = rows_per_shard;
            if (rps.empty())
            {
                rps_buf.resize(num_shards, 0);
                countRowsPerShard(pids_per_source, std::span<UInt32>(rps_buf.data(), num_shards));
                rps = std::span<const UInt32>(rps_buf.data(), num_shards);
            }
            MutableColumns dst(num_shards);
            for (size_t s = 0; s < num_shards; ++s)
                dst[s] = source_columns[0]->cloneResized(rps[s]);
            return dst;
        }
    }

    /// Normalize representation once, recursively. Sources at one position share a logical
    /// type but may differ in concrete representation (full vs Const/Sparse/Replicated, e.g.
    /// after constant folding or UNION), and composites may hide such wrappers in their
    /// subcolumns. After this pass no wrapper remains at any level (LowCardinality preserved as
    /// a leaf), so dispatch, the typed/composite kernels, and the fallback all assume full
    /// input. `normalized` owns the rebuilt columns and outlives the recursive dispatch below.
    /// Fast-skip clean leaf batches (the hot path); always recurse for composites whose nested
    /// columns a top-level wrapper probe cannot see.
    absl::InlinedVector<ColumnPtr, SCATTER_INLINE_SOURCES> normalized;
    absl::InlinedVector<const IColumn *, SCATTER_INLINE_SOURCES> full_ptrs;
    const bool needs_normalization = std::any_of(
        source_columns.begin(), source_columns.end(),
        [](const IColumn * col)
        { return col->isConst() || col->isSparse() || col->isReplicated() || probeMayHaveSubcolumns(col); });

    if (needs_normalization)
        source_columns = normalizeAll(source_columns, normalized, full_ptrs);

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
    // Nested columns are already wrapper-free (normalized at the scatter() boundary).
    std::vector<const IColumn *> nested_cols(source_columns.size()); // STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (size_t b = 0; b < source_columns.size(); ++b)
        nested_cols[b] = &assert_cast<const ColumnNullable &>(*source_columns[b]).getNestedColumn();

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
    // Element columns are already wrapper-free (normalized at the scatter() boundary).
    std::vector<MutableColumns> scattered_elements(num_elements); // STYLE_CHECK_ALLOW_STD_CONTAINERS
    for (size_t e = 0; e < num_elements; ++e)
        scattered_elements[e] = scatterDispatch(element_cols[e], pids_per_source, rows_per_shard);

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
