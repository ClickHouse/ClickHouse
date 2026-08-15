#pragma once

#include <Columns/IColumn.h>
#include <Common/PODArray.h>

#include <base/defines.h>
#include <base/types.h>

#include <atomic>
#include <cstring>
#include <span>
#include <vector>

#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
#include <arm_acle.h>
#endif

namespace DB::ColumnsScatter
{

/** Generic, type-complete column scatter: physically splits batches of columns into exact-sized
  * per-partition destinations. Two public layers:
  *
  * Layer 0 — performance primitives. Thread-agnostic, single-threaded per call; callers own worker
  * coordination, barriers, and destination allocation. Raw performance idioms (byte cursors,
  * uninitialized exact-sized allocation, non-temporal stores) with documented invariants. These are
  * the radix scatter kernels of `RadixHashJoin` (histogram → prefix sum + exact allocation →
  * software-write-combining scatter), extracted so any consumer can compose them — including the
  * join's own 3-barrier parallel drivers.
  *
  * Layer 1 — safe one-shot surface: `scatter` below. Batched per column-position, allocates
  * exact-sized destinations internally, dispatches to a typed kernel in O(1), normalizes transparent
  * wrappers, falls back to legacy `IColumn::scatter` for exotic leaf types. No raw pointers escape.
  */

/// ------------------------------------------------------------------------------------------------
/// Layer 0 — constants and batching model (values proven by the `hash_join_bench` ancestry and kept
/// in lockstep with the benchmark-derived kernels)
/// ------------------------------------------------------------------------------------------------

constexpr size_t LINE_BYTES = 64;
/// Fanout from which the SWWC + non-temporal path wins over plain per-partition cursors.
constexpr size_t SWWC_MIN_FANOUT = 256;
/// Below this fanout histograms use 4 interleaved lanes to break the load-increment-store chain.
constexpr size_t HIST_INTERLEAVE_MAX_FANOUT = 2048;
/// Batch sizing: the boundary cost (cursor sweeps, partial-line flushes) stays a small fraction of
/// the lines written in between.
constexpr size_t SCATTER_BATCH_MIN_ROWS = 256 << 10;
constexpr size_t SCATTER_BATCH_LINES_PER_PARTITION = 64;
/// Per-pass fanout ceiling: the SWWC staging cache ceiling (~76 B/partition/worker must fit L2).
/// Also the reason 16-bit partition ids suffice on the per-pass hot paths.
constexpr size_t MAX_FANOUT_PER_PASS = 8192;

inline size_t scatterBatchRowsTarget(size_t fanout)
{
    return std::max(SCATTER_BATCH_MIN_ROWS, fanout * SCATTER_BATCH_LINES_PER_PARTITION * (LINE_BYTES / sizeof(UInt64)));
}

/// SWWC is enabled only for widths that divide the 64-byte line and are covered by the 16-byte
/// minimum alignment of column data (so the per-partition staging line fills to exactly 64 bytes).
inline bool widthSupportsSwwc(size_t w)
{
    return w == 1 || w == 2 || w == 4 || w == 8 || w == 16;
}

/// ------------------------------------------------------------------------------------------------
/// Layer 0 — route hashing. Deliberately independent of the CRC32C the join's leaf hash tables use
/// for bucketing: otherwise partition assignment would correlate with in-table bucket placement and
/// each leaf table would see a skewed hash space. The hot single-UInt64 path exactly matches the
/// benchmark: ISO-polynomial CRC32 on aarch64, golden-ratio multiply-shift elsewhere. Wider and
/// composite keys use the width-generic multiply-shift fold. All passes of a multi-pass radix plan
/// slice disjoint bit ranges of this one 32-bit route word (consumed MSB-first).
/// ------------------------------------------------------------------------------------------------

ALWAYS_INLINE inline UInt32 routeWord(UInt64 key)
{
#if defined(__aarch64__) && defined(__ARM_FEATURE_CRC32)
    return __crc32d(-1U, key);
#else
    return static_cast<UInt32>((key * 0x9E3779B97F4A7C15ULL) >> 32);
#endif
}

ALWAYS_INLINE inline UInt64 mixStep(UInt64 h, UInt64 x)
{
    return (h ^ x) * 0x9E3779B97F4A7C15ULL;
}

ALWAYS_INLINE inline UInt32 finalizeRoute(UInt64 h)
{
    return static_cast<UInt32>(h >> 32);
}

/// Fold `w` bytes at `p` into the accumulator, 8 bytes at a time with a zero-padded tail.
/// The tail dispatches on its size to constant-size copies: a runtime-size copy lowers to a
/// per-row libc `memcpy` call on the runtime-width paths, while constant sizes lower to plain
/// loads (and constant-width callers fold the switch away entirely). Each case copies exactly
/// `w - i` bytes into the zeroed chunk, so the fold value is bit-identical to a runtime-size copy.
ALWAYS_INLINE inline UInt64 foldBytes(UInt64 h, const char * p, size_t w)
{
    size_t i = 0;
    for (; i + 8 <= w; i += 8)
    {
        UInt64 x = 0;
        memcpy(&x, p + i, sizeof(x));
        h = mixStep(h, x);
    }
    if (i < w)
    {
        UInt64 x = 0;
        switch (w - i) // NOLINT(bugprone-switch-missing-default-case): the tail size is provably in [1, 7]
        {
            case 1: memcpy(&x, p + i, 1); break;
            case 2: memcpy(&x, p + i, 2); break;
            case 3: memcpy(&x, p + i, 3); break;
            case 4: memcpy(&x, p + i, 4); break;
            case 5: memcpy(&x, p + i, 5); break;
            case 6: memcpy(&x, p + i, 6); break;
            case 7: memcpy(&x, p + i, 7); break;
        }
        h = mixStep(h, x);
    }
    return h;
}

ALWAYS_INLINE inline UInt32 routeWordBytes(const char * p, size_t w)
{
    return finalizeRoute(foldBytes(0, p, w));
}

/// ------------------------------------------------------------------------------------------------
/// Layer 0 — per-worker scatter state: write cursors (byte-granular), and for the SWWC path one
/// 64-byte staging line per partition plus a byte fill counter.
///
/// Invariant: staged bytes for partition p live at staging + p*64 + [m, fill), where
/// m = (uintptr)cursors[p] & 63. `seed` seeds `fill` with the cursor misalignment; before the first
/// flush the cursor has not advanced (m == fill start), after the first flush the cursor is
/// line-aligned (m == 0). Column-data bases are >= 16-byte aligned and per-worker start offsets are
/// multiples of the element width, so for the SWWC-enabled widths (1,2,4,8,16) m is a multiple of
/// the width and the staging line fills to exactly 64 bytes.
/// ------------------------------------------------------------------------------------------------
struct ScatterScratch
{
    size_t fanout = 0;
    bool use_swwc = false;
    PaddedPODArray<char> staging_mem;
    char * staging = nullptr;
    PaddedPODArray<char *> cursors;
    PaddedPODArray<UInt32> fill;

    void init(size_t fanout_, bool use_swwc_)
    {
        fanout = fanout_;
        use_swwc = use_swwc_;
        cursors.resize(fanout);
        if (use_swwc)
        {
            staging_mem.resize(fanout * LINE_BYTES + LINE_BYTES);
            staging = reinterpret_cast<char *>(
                (reinterpret_cast<uintptr_t>(staging_mem.data()) + LINE_BYTES - 1) & ~static_cast<uintptr_t>(LINE_BYTES - 1));
            fill.resize(fanout);
        }
    }

    void setUseSwwc(bool use_swwc_)
    {
        chassert(!use_swwc_ || staging);
        use_swwc = use_swwc_;
    }

    void seed(size_t p, char * cursor)
    {
        cursors[p] = cursor;
        if (use_swwc)
            fill[p] = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cursor) & (LINE_BYTES - 1));
    }

    /// Flush residual staged bytes of every partition and publish the non-temporal stores.
    void drain()
    {
        if (!use_swwc)
            return;
        for (size_t p = 0; p < fanout; ++p)
        {
            const UInt32 f = fill[p];
            if (!f)
                continue;
            char * cur = cursors[p];
            const UInt32 m = static_cast<UInt32>(reinterpret_cast<uintptr_t>(cur) & (LINE_BYTES - 1));
            if (f > m)
            {
                memcpy(cur, staging + p * LINE_BYTES + m, f - m);
                cursors[p] = cur + (f - m);
            }
            fill[p] = 0;
        }
        /// NT stores are weakly ordered; make them visible before the outputs are read.
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }
};

/// ------------------------------------------------------------------------------------------------
/// Layer 0 — chunk-level kernels. One call scatters one chunk of one column; write cursors persist
/// in the caller-owned ScatterScratch across chunks (seed once per column, scatter all chunks,
/// drain once). The hot row loops live inside the module's translation unit — the call overhead is
/// per chunk, never per row. Partition ids come in two widths: UInt16 is the bandwidth-optimal form
/// for per-pass fanouts (<= MAX_FANOUT_PER_PASS); UInt32 is the general form.
/// ------------------------------------------------------------------------------------------------

/// Scatter one chunk of a single-column key (route computed from the key bytes), emitting pids as a
/// by-product when `pids_out` is non-null. Partition = (routeWord(key) >> shift) & mask.
void scatterKeyChunk(
    size_t key_width, const char * keys, size_t n, UInt32 shift, UInt32 mask, UInt16 * pids_out, bool use_swwc, ScatterScratch & scratch);
void scatterKeyChunk(
    size_t key_width, const char * keys, size_t n, UInt32 shift, UInt32 mask, UInt32 * pids_out, bool use_swwc, ScatterScratch & scratch);

/// Scatter one chunk of a fixed-width column via precomputed pids.
void scatterPidChunk(size_t width, const UInt16 * pids, const char * data, size_t n, bool use_swwc, ScatterScratch & scratch);
void scatterPidChunk(size_t width, const UInt32 * pids, const char * data, size_t n, bool use_swwc, ScatterScratch & scratch);

/// Histogram one chunk's rows from a single key column. At interleaved fanouts pass `lanes`
/// (4 * fanout counters, caller-owned, persistent across chunks, reduced once at the end);
/// pass nullptr to accumulate directly into `hist`.
void histogramKeyChunk(size_t key_width, const char * keys, size_t n, UInt32 shift, UInt32 mask, UInt32 * hist, UInt32 * lanes, size_t fanout);
void histogramKeyChunk(size_t key_width, const char * keys, size_t n, UInt32 shift, UInt32 mask, UInt64 * hist, UInt64 * lanes, size_t fanout);

/// Histogram one chunk's rows from precomputed route words (composite-key mode).
void histogramRouteChunk(const UInt32 * routes, size_t n, UInt32 shift, UInt32 mask, UInt32 * hist, UInt32 * lanes, size_t fanout);
void histogramRouteChunk(const UInt32 * routes, size_t n, UInt32 shift, UInt32 mask, UInt64 * hist, UInt64 * lanes, size_t fanout);

/// Histogram one chunk's rows from precomputed pids.
void histogramPidChunk(const UInt16 * pids, size_t n, UInt32 * hist, UInt32 * lanes, size_t fanout);
void histogramPidChunk(const UInt16 * pids, size_t n, UInt64 * hist, UInt64 * lanes, size_t fanout);
void histogramPidChunk(const UInt32 * pids, size_t n, UInt32 * hist, UInt32 * lanes, size_t fanout);
void histogramPidChunk(const UInt32 * pids, size_t n, UInt64 * hist, UInt64 * lanes, size_t fanout);

void reduceHistogramLanes(UInt32 * hist, const UInt32 * lanes, size_t fanout);
void reduceHistogramLanes(UInt64 * hist, const UInt64 * lanes, size_t fanout);

/// Exact-sized uninitialized destination allocation for a fixed-width column: `cloneEmpty` +
/// `insertRawUninitialized`. No memset — pages are first-touched by the scatter writes themselves.
/// Returns the column and the raw write base spanning exactly rows * `sizeOfValueIfFixed` bytes.
std::pair<MutableColumnPtr, std::span<char>> allocateUninitializedFixed(const IColumn & sample, size_t rows);

/// ------------------------------------------------------------------------------------------------
/// Layer 0 — variable-length (String-shaped) chunk kernel: two coupled per-shard output streams,
/// chars (data-dependent row lengths, byte cursors) and offsets (destination offsets are REBASED
/// running per-shard byte totals, not copies of source offsets). Callers compute per-shard byte
/// totals with `stringBytesPerShard`, allocate exactly, seed, and scatter chunk by chunk; cursors
/// and rebased totals persist across chunks like `ScatterScratch` cursors do.
/// ------------------------------------------------------------------------------------------------
struct StringScatterState
{
    /// One 32-byte record per shard so a row touches ONE line of cursor state, not three arrays
    /// (at fanout 8192 the state alone is 256 KiB — per-row line count dominates the L2 traffic).
    struct ShardCursor
    {
        char * chars = nullptr;
        UInt64 * offsets = nullptr;
        /// Running rebased byte total == the value the NEXT row's destination offset gets after
        /// adding its length. Seed with 0 for fresh destinations.
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

/// Accumulate per-shard chars-byte totals for one chunk (`offsets` in `ColumnString` form:
/// offsets[i] = end of row i, offsets[-1] readable as 0 via the PODArray left pad).
void stringBytesPerShard(const UInt64 * offsets, const UInt16 * pids, size_t n, UInt64 * bytes_per_shard);
void stringBytesPerShard(const UInt64 * offsets, const UInt32 * pids, size_t n, UInt64 * bytes_per_shard);

/// Scatter one chunk of a String column via precomputed pids. The chars copy follows the
/// `memcpySmallAllowReadWriteOverflow15` contract (implemented in-module; see the kernel comment
/// for why the library function is not called): every row write may touch up to 15 bytes past the
/// row's end, so EACH shard's chars destination must be its own overflow-tolerant allocation (e.g.
/// a per-shard `PaddedPODArray`, as a `ColumnString` naturally provides). Carving multiple shard
/// regions out of one shared buffer is NOT supported — a row written to shard p would clobber the
/// head of shard p+1.
void scatterStringChunk(const char * chars, const UInt64 * offsets, const UInt16 * pids, size_t n, StringScatterState & state);
void scatterStringChunk(const char * chars, const UInt64 * offsets, const UInt32 * pids, size_t n, StringScatterState & state);

/// ------------------------------------------------------------------------------------------------
/// Dispatch introspection — test-visible proof of which path handled a column. plannedKernel is a
/// pure probe of the dispatch table; DispatchTrace records what actually ran (one entry per Layer-1
/// call). The disabled-trace cost is one predictable null check per call — never per row.
/// ------------------------------------------------------------------------------------------------

enum class ScatterKernelId : UInt8
{
    FixedWidth,     /// raw-byte kernels: ColumnVector, ColumnDecimal, ColumnFixedString
    String,         /// fused chars + rebased-offsets kernel with per-shard byte cursors
    Nullable,       /// null-map via the width-1 fixed kernel + nested dispatched recursively
    Tuple,          /// per-element recursive dispatch, shards reassembled
    Array,          /// rebased offsets + element-level pid expansion + nested dispatched recursively
    Map,            /// delegates to the nested Array(Tuple(key, value)) kernel
    LowCardinality, /// type-preserving: index stream via the fixed kernel, dictionary shared,
                    /// (single source; a multi-source batch merges per-source legacy scatters)
    ConstCompact,   /// all-const equal-value batch: cloneResized per shard, O(1) memory
    Fallback,       /// legacy IColumn::scatter + insertRangeFrom
};

const char * toString(ScatterKernelId id);

/// The kernel the dispatch table maps this column's post-normalization type to. (Transparent
/// wrappers report their nested type, so probing a wrapped column answers for the normalized form.)
ScatterKernelId plannedKernel(const IColumn & column);

struct DispatchTrace
{
    struct Entry
    {
        TypeIndex type;
        ScatterKernelId kernel;
    };
    std::vector<Entry> entries; /// STYLE_CHECK_ALLOW_STD_CONTAINERS (test-only introspection surface)
};

/// Install (or remove, with nullptr) the calling thread's trace; returns the previous one.
DispatchTrace * exchangeDispatchTrace(DispatchTrace * trace);

/// ------------------------------------------------------------------------------------------------
/// Layer 1 — safe one-shot surface
/// ------------------------------------------------------------------------------------------------

/// Count rows routed to each shard from a batch of pid spans. `rows_per_shard` must be pre-zeroed
/// with size == num_shards. Compute once per flush and pass to every `scatter` call of that flush
/// (one per column-position) to avoid redundant re-counting.
void countRowsPerShard(std::span<const std::span<const UInt16>> pids_per_source, std::span<UInt32> rows_per_shard);
void countRowsPerShard(std::span<const std::span<const UInt32>> pids_per_source, std::span<UInt32> rows_per_shard);

/** Batched, type-dispatched physical scatter of one column-position.
  *
  * - `source_columns[b]` is the column extracted from chunk b for one column-position; all sources
  *   must share the same concrete type. Row j of source b is routed to shard `pids_per_source[b][j]`.
  * - Destinations are allocated exact-sized inside; the k-th result holds, in source order, every
  *   row routed to shard k, with the same concrete type as the sources.
  * - `rows_per_shard` (optional): precomputed by `countRowsPerShard`; when empty, counts are
  *   computed internally. When non-empty its size must equal num_shards and its elements must equal
  *   the exact per-shard pid counts: the values drive exact-sized destination allocation, so an
  *   undercount is undefined behavior (heap overflow) in release builds — contents are verified in
  *   debug/sanitizer builds only.
  * - Transparent wrappers (ColumnConst/ColumnSparse/ColumnReplicated) are normalized away
  *   recursively before dispatch; ColumnLowCardinality is preserved. An all-const batch with
  *   byte-identical values stays compact (ColumnConst results).
  * - Misuse (span size mismatches, pid count != column size, zero shards, source TypeIndex
  *   mismatch, mismatched FixedString widths or tuple arity) throws `LOGICAL_ERROR` in every build
  *   mode. DEEPER concrete-type mismatches between same-TypeIndex sources (e.g. differently-typed
  *   tuple elements of equal width), out-of-range pids, and wrong rows_per_shard contents are
  *   checked in debug/sanitizer builds only — in release they are undefined behavior.
  */
[[nodiscard]] MutableColumns scatter(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt16>> pids_per_source,
    size_t num_shards,
    std::span<const UInt32> rows_per_shard = {});
[[nodiscard]] MutableColumns scatter(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt32>> pids_per_source,
    size_t num_shards,
    std::span<const UInt32> rows_per_shard = {});

}
