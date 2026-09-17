#pragma once

#include <Columns/IColumn.h>
#include <Common/PODArray.h>

#include <base/defines.h>
#include <base/types.h>

#include <atomic>
#include <cstring>
#include <span>
#include <vector>

namespace DB::ColumnsScatter
{

/** Splits batches of columns into exact-sized per-shard destinations. A shard is one destination;
  * the fanout is the shard count; a pid is a row's shard id; SWWC is software write-combining.
  *
  * Two surfaces. The chunk kernels are single-threaded per call and own nothing. The caller
  * allocates destinations, seeds write cursors, and coordinates workers. A parallel driver can then
  * run histogram, prefix-sum, and scatter itself. `scatter` is the one-shot surface: it allocates,
  * dispatches on type, and lets no raw pointer escape.
  */

constexpr size_t LINE_BYTES = 64;
/// Below this fanout the per-shard cursors still hit in cache, so software write-combining (SWWC) only adds work.
constexpr size_t SWWC_MIN_FANOUT = 256;
/// Above this fanout the 4 interleaved histogram lanes cost more cache than the dependency chain they break.
constexpr size_t HIST_INTERLEAVE_MAX_FANOUT = 2048;
/// Sized so the per-batch boundary work (cursor sweeps, partial-line flushes) stays small against
/// the lines written in between.
constexpr size_t SCATTER_BATCH_MIN_ROWS = 256 << 10;
constexpr size_t SCATTER_BATCH_LINES_PER_SHARD = 64;

inline size_t scatterBatchRowsTarget(size_t fanout)
{
    return std::max(SCATTER_BATCH_MIN_ROWS, fanout * SCATTER_BATCH_LINES_PER_SHARD * (LINE_BYTES / sizeof(UInt64)));
}

/// Splits log2(fanout) shard bits into MSB-first passes, each at most `max_fanout_per_pass` wide.
/// The split is balanced, not greedy: 15 bits under a 10-bit cap give 8 + 7, not 10 + 5.
/// The widest pass sets the staging footprint. Empty for fanout <= 1.
std::vector<size_t> computePassBits(size_t fanout, size_t max_fanout_per_pass); /// STYLE_CHECK_ALLOW_STD_CONTAINERS

/// Write combining needs the staging line to fill to exactly 64 bytes. That holds only for widths
/// that divide the line and that the 16-byte minimum alignment of column data covers.
inline bool widthSupportsSwwc(size_t w)
{
    return w == 1 || w == 2 || w == 4 || w == 8 || w == 16;
}

/** Per-worker write cursors, plus one 64-byte staging line and a fill counter per shard when write
  * combining is on. Invariant: shard p's staged bytes live at `staging + p*64 + [m, fill)` with
  * `m = (uintptr) cursors[p] & 63`. `seed` starts `fill` at the cursor's misalignment. The first
  * flush then writes only the bytes past that and leaves the cursor line-aligned (`m == 0`).
  * Column data is at least 16-byte aligned and per-worker offsets are whole elements. For the
  * write-combined widths, `m` is a multiple of the width and the line always fills to exactly 64 bytes.
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

    /// Must run before any destination is read: it flushes the partial lines and publishes the non-temporal stores.
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

/** Chunk kernels: one call scatters one chunk of one column. Cursors live in the caller's
  * `ScatterScratch` across chunks: seed once per column, scatter every chunk, drain once. The row
  * loops stay in this translation unit, so the call cost is per chunk, never per row. Pids are
  * `UInt16` because the narrower id halves the pid stream's bandwidth. The caller keeps its shard
  * count, plus any spare id it needs, under 2^16.
  */

void scatterPidChunk(size_t width, const UInt16 * pids, const char * data, size_t n, bool use_swwc, ScatterScratch & scratch);

/// `lanes` is 4 * fanout caller-owned counters: four interleaved partial histograms that break the
/// load-increment-store dependency chain. Reduce them once at the end. Pass nullptr above
/// `HIST_INTERLEAVE_MAX_FANOUT` to count straight into `hist`.
void histogramPidChunk(const UInt16 * pids, size_t n, UInt64 * hist, UInt64 * lanes, size_t fanout);
void reduceHistogramLanes(UInt64 * hist, const UInt64 * lanes, size_t fanout);

/// Returns the column and a write base spanning exactly `rows * sizeOfValueIfFixed` bytes. The
/// memory is left uninitialized on purpose: the scatter writes first-touch the pages.
std::pair<MutableColumnPtr, std::span<char>> allocateUninitializedFixed(const IColumn & sample, size_t rows);

/// Which kernel handled a column. Tests assert on it. A silent fall back to `IColumn::scatter` is
/// otherwise invisible.
enum class ScatterKernelId : UInt8
{
    FixedWidth,     /// raw-byte kernels: ColumnVector, ColumnDecimal, ColumnFixedString
    String,         /// fused chars + rebased-offsets kernel with per-shard byte cursors
    Nullable,       /// null-map via the width-1 fixed kernel + nested dispatched recursively
    Tuple,          /// per-element recursive dispatch, shards reassembled
    Array,          /// rebased offsets + element-level pid expansion + nested dispatched recursively
    Map,            /// delegates to the nested Array(Tuple(key, value)) kernel
    LowCardinality, /// stays LowCardinality: indexes through the fixed-width kernel, one shared
                    /// dictionary. Single source only; a multi-source batch goes through
                    /// `IColumn::scatter` per source and merges.
    ConstCompact,   /// all-const equal-value batch: cloneResized per shard, O(1) memory
    Fallback,       /// `IColumn::scatter` per source + insertRangeFrom
};

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
/// result to every `scatter` call of that flush rather than recounting per column-position.
void countRowsPerShard(std::span<const std::span<const UInt16>> pids_per_source, std::span<UInt32> rows_per_shard);

/** Scatters one column-position of a batch of chunks. `source_columns[b]` is that position's column
  * from chunk b; all chunks share one concrete type. Row j of source b goes to shard
  * `pids_per_source[b][j]`. Result k holds every row routed to shard k, in source order.
  *
  * Transparent wrappers are stripped before dispatch. `ColumnLowCardinality` is preserved. An
  * all-const batch of byte-identical values stays a `ColumnConst`.
  *
  * `rows_per_shard` comes from `countRowsPerShard`. Pass it empty to count internally. When passed,
  * its values size the allocations, so an undercount overflows the heap. An undercount, out-of-range
  * pids and concrete-type mismatches deeper than TypeIndex are checked in debug and sanitizer
  * builds only. In release they are undefined behavior. Shallower misuses throw `LOGICAL_ERROR` in
  * every build: span sizes, pid count against column size, zero shards, TypeIndex, FixedString
  * width, tuple arity.
  */
[[nodiscard]] MutableColumns scatter(
    std::span<const IColumn * const> source_columns,
    std::span<const std::span<const UInt16>> pids_per_source,
    size_t num_shards,
    std::span<const UInt32> rows_per_shard = {});

}
