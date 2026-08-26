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

/** Splits batches of columns into exact-sized per-partition destinations.
  *
  * Two surfaces. The chunk kernels below are raw and single-threaded per call - the caller owns
  * worker coordination, destination allocation and the write cursors - so a parallel driver can run
  * histogram, prefix sum and scatter as its own phases. `scatter` at the bottom is the one-shot
  * surface: it allocates, dispatches on type and lets no raw pointer escape.
  */

constexpr size_t LINE_BYTES = 64;
/// Below this fanout the per-partition cursors still hit in cache, so write combining only adds work.
constexpr size_t SWWC_MIN_FANOUT = 256;
/// Above this fanout the 4 interleaved histogram lanes cost more cache than the dependency chain
/// they break.
constexpr size_t HIST_INTERLEAVE_MAX_FANOUT = 2048;
/// Sized so the per-batch boundary work (cursor sweeps, partial-line flushes) stays small against
/// the lines written in between.
constexpr size_t SCATTER_BATCH_MIN_ROWS = 256 << 10;
constexpr size_t SCATTER_BATCH_LINES_PER_PARTITION = 64;
/// One pass may fan out no wider than its staging cache fits in L2 (~76 B per partition per
/// worker). This is also what keeps partition ids inside 16 bits on the per-pass hot paths.
constexpr size_t MAX_FANOUT_PER_PASS = 1024;

inline size_t scatterBatchRowsTarget(size_t fanout)
{
    return std::max(SCATTER_BATCH_MIN_ROWS, fanout * SCATTER_BATCH_LINES_PER_PARTITION * (LINE_BYTES / sizeof(UInt64)));
}

/// Splits log2(fanout) partition bits into MSB-first passes under the per-pass cap, balanced rather
/// than greedy: 15 bits under a 10-bit cap split 8 + 7, not 10 + 5, because the widest pass sets the
/// staging footprint. Empty for fanout <= 1.
std::vector<size_t> computePassBits(size_t fanout, size_t max_fanout_per_pass); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

/// Write combining needs the staging line to fill to exactly 64 bytes, which holds only for widths
/// that divide the line and are covered by the 16-byte minimum alignment of column data.
inline bool widthSupportsSwwc(size_t w)
{
    return w == 1 || w == 2 || w == 4 || w == 8 || w == 16;
}

/** Route hashing. Deliberately not the CRC32C a hash table uses for bucketing: sharing the function
  * would correlate partition assignment with in-table cell placement, leaving every partition's
  * table a skewed slice of the hash space. Every pass of a multi-pass plan slices a disjoint bit
  * range of the one 32-bit route word, MSB first.
  */

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

/// Fold `w` bytes at `p` into the accumulator, 8 at a time with a zero-padded tail. The tail
/// switches on its size because a runtime-size `memcpy` lowers to a libc call per row on the
/// runtime-width paths, while each constant size lowers to a load - and constant-width callers fold
/// the switch away. The cases copy exactly `w - i` bytes, so the result is bit-identical either way.
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

/** Per-worker write cursors, plus one 64-byte staging line and a fill counter per partition when
  * write combining is on.
  *
  * Invariant: partition p's staged bytes live at `staging + p*64 + [m, fill)` with
  * `m = (uintptr) cursors[p] & 63`. `seed` starts `fill` at the cursor's misalignment, so the first
  * flush writes only the bytes past it and leaves the cursor line-aligned (m == 0) from then on.
  * Column data is at least 16-byte aligned and per-worker offsets are whole elements, so for the
  * write-combined widths m is a multiple of the width and the line always fills to exactly 64 bytes.
  */
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

    /// Must run before any destination is read: it flushes the partial lines and publishes the
    /// non-temporal stores.
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
        /// Non-temporal stores are weakly ordered.
        std::atomic_thread_fence(std::memory_order_seq_cst);
    }
};

/** Chunk kernels: one call scatters one chunk of one column, with the cursors living in the
  * caller's `ScatterScratch` across chunks - seed once per column, scatter every chunk, drain once.
  * The row loops stay inside this module's translation unit, so the call cost is per chunk and never
  * per row. Partition ids come in both widths because UInt16 halves the pid bandwidth and covers any
  * fanout up to `MAX_FANOUT_PER_PASS`.
  */

/// Partition is `(routeWord(key) >> shift) & mask`; the pids are written out too when `pids_out` is
/// non-null, so a later column can reuse them.
void scatterKeyChunk(
    size_t key_width, const char * keys, size_t n, UInt32 shift, UInt32 mask, UInt16 * pids_out, bool use_swwc, ScatterScratch & scratch);
void scatterKeyChunk(
    size_t key_width, const char * keys, size_t n, UInt32 shift, UInt32 mask, UInt32 * pids_out, bool use_swwc, ScatterScratch & scratch);

void scatterPidChunk(size_t width, const UInt16 * pids, const char * data, size_t n, bool use_swwc, ScatterScratch & scratch);
void scatterPidChunk(size_t width, const UInt32 * pids, const char * data, size_t n, bool use_swwc, ScatterScratch & scratch);

/// `lanes` is 4 * fanout caller-owned counters, persistent across chunks and reduced once at the
/// end; pass nullptr below `HIST_INTERLEAVE_MAX_FANOUT` to count straight into `hist`.
void histogramKeyChunk(size_t key_width, const char * keys, size_t n, UInt32 shift, UInt32 mask, UInt32 * hist, UInt32 * lanes, size_t fanout);
void histogramKeyChunk(size_t key_width, const char * keys, size_t n, UInt32 shift, UInt32 mask, UInt64 * hist, UInt64 * lanes, size_t fanout);

/// For composite keys, whose route words are computed once and reused by every pass.
void histogramRouteChunk(const UInt32 * routes, size_t n, UInt32 shift, UInt32 mask, UInt32 * hist, UInt32 * lanes, size_t fanout);
void histogramRouteChunk(const UInt32 * routes, size_t n, UInt32 shift, UInt32 mask, UInt64 * hist, UInt64 * lanes, size_t fanout);

void histogramPidChunk(const UInt16 * pids, size_t n, UInt32 * hist, UInt32 * lanes, size_t fanout);
void histogramPidChunk(const UInt16 * pids, size_t n, UInt64 * hist, UInt64 * lanes, size_t fanout);
void histogramPidChunk(const UInt32 * pids, size_t n, UInt32 * hist, UInt32 * lanes, size_t fanout);
void histogramPidChunk(const UInt32 * pids, size_t n, UInt64 * hist, UInt64 * lanes, size_t fanout);

void reduceHistogramLanes(UInt32 * hist, const UInt32 * lanes, size_t fanout);
void reduceHistogramLanes(UInt64 * hist, const UInt64 * lanes, size_t fanout);

/// Returns the column and a write base spanning exactly `rows * sizeOfValueIfFixed` bytes. The
/// memory is left uninitialized on purpose: the scatter writes are what first-touch the pages.
std::pair<MutableColumnPtr, std::span<char>> allocateUninitializedFixed(const IColumn & sample, size_t rows);

/** State for the String-shaped kernels, which drive two coupled output streams per shard: chars,
  * whose row lengths are data-dependent, and offsets, which are rebased running per-shard totals
  * rather than copies of the source offsets. Callers size the chars streams with
  * `stringBytesPerShard`, allocate, seed, and then scatter chunk by chunk.
  */
struct StringScatterState
{
    /// Packed into one 32-byte record so a row touches a single line of cursor state instead of
    /// three arrays: at fanout 8192 the state alone is 256 KiB, where the per-row line count is
    /// what dominates L2 traffic.
    struct ShardCursor
    {
        char * chars = nullptr;
        UInt64 * offsets = nullptr;
        /// What the next row's destination offset becomes once its length is added; 0 for a fresh
        /// destination.
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

/// `offsets` is in `ColumnString` form - offsets[i] ends row i, and offsets[-1] reads as 0 through
/// the PODArray left pad.
void stringBytesPerShard(const UInt64 * offsets, const UInt16 * pids, size_t n, UInt64 * bytes_per_shard);
void stringBytesPerShard(const UInt64 * offsets, const UInt32 * pids, size_t n, UInt64 * bytes_per_shard);

/// The chars copy follows the `memcpySmallAllowReadWriteOverflow15` contract: a row write may touch
/// up to 15 bytes past the row's end, so every shard's chars destination must be its own
/// overflow-tolerant allocation - a `ColumnString` is one. Carving the shards out of a single shared
/// buffer is not supported: a row written to shard p would clobber the head of shard p + 1.
void scatterStringChunk(const char * chars, const UInt64 * offsets, const UInt16 * pids, size_t n, StringScatterState & state);
void scatterStringChunk(const char * chars, const UInt64 * offsets, const UInt32 * pids, size_t n, StringScatterState & state);

/// Which kernel handled a column - the tests assert on it, since a silent fall back to
/// `IColumn::scatter` is otherwise invisible.
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

/// Answers for the normalized type, so a wrapped column reports what its nested column will take.
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

/// Install (or remove, with nullptr) the calling thread's trace; returns the previous one. A thread
/// without one pays a single null check per call, never per row.
DispatchTrace * exchangeDispatchTrace(DispatchTrace * trace);

/// `rows_per_shard` must be pre-zeroed with size == num_shards. Count once per flush and hand the
/// result to every `scatter` call of that flush rather than re-counting per column-position.
void countRowsPerShard(std::span<const std::span<const UInt16>> pids_per_source, std::span<UInt32> rows_per_shard);
void countRowsPerShard(std::span<const std::span<const UInt32>> pids_per_source, std::span<UInt32> rows_per_shard);

/** Scatters one column-position of a batch of chunks: `source_columns[b]` is that position's column
  * from chunk b, all of the same concrete type, and row j of source b goes to shard
  * `pids_per_source[b][j]`. Result k holds every row routed to shard k in source order.
  *
  * Transparent wrappers are normalized away before dispatch, `ColumnLowCardinality` is preserved,
  * and an all-const batch of byte-identical values stays a `ColumnConst`.
  *
  * `rows_per_shard` comes from `countRowsPerShard`; pass it empty to count internally. When passed,
  * its values drive exact-sized allocation, so an undercount overflows the heap. That, out-of-range
  * pids, and concrete-type mismatches deeper than the TypeIndex are checked in debug and sanitizer
  * builds only - in release they are undefined behavior. The shallower misuses (span sizes, pid
  * count against column size, zero shards, TypeIndex, FixedString width, tuple arity) throw
  * `LOGICAL_ERROR` in every build.
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
