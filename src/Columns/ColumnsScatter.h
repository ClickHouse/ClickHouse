#pragma once

#include <Columns/IColumn.h>
#include <base/types.h>
#include <Common/PODArray.h>
#include <Common/VectorWithMemoryTracking.h>

#include <algorithm>
#include <span>
#include <utility>

/// Typed kernels that split a column into pieces, given one piece id per row. Every array of a piece gets exactly the
/// size its rows need. `Id` is `UInt16` or `UInt64`.

namespace DB::ColumnsScatter
{

/// True if `scatter` below gives the same pieces as the `insertFrom` loop of `IColumn::scatter` for this column. It
/// holds for `ColumnVector`, `ColumnDecimal`, `FixedString` and `String`. It holds for `Nullable(X)` and `Array(X)`
/// when it holds for X. Inside an `Array` or a `Nullable`, it holds for a `Tuple` when it holds for every element. It
/// never holds for `Const`, `Sparse` or `Replicated` at any depth, nor for any other type.
bool hasKernel(const IColumn & column);

/// Splits the column as `IColumn::scatterWithCounts` does: row i goes to piece `selector[i]`, and `counts[k]` is the
/// number of rows with `selector[i] == k`. Precondition: `hasKernel(column)`.
template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatter(
    const IColumn & column, const PaddedPODArray<Id> & selector, std::span<const size_t> counts);

/// Splits several columns of one type, such as one column of several blocks, into `counts.size()` pieces. Row j of
/// `sources[b]` goes to piece `ids[b][j]`, and each piece holds its rows in source order. `counts` is what
/// `countRowsPerPiece(ids, counts.size())` returns. With one source, the result is what `IColumn::scatterWithCounts`
/// returns. Several sources may differ in their wrappers: if all of them are `Const` with the same value, the pieces
/// are `Const`, otherwise the wrappers are removed. The caller passes sources of one data type. Only their type ids
/// (`getDataType`) are compared, not their nested types, scales or widths. Throws `LOGICAL_ERROR` when `sources` is
/// empty or differs in size from `ids`, when a source and its ids differ in size, when `counts` does not sum to the
/// number of ids, or when the type ids of the sources differ. Also throws when `Id` is `UInt16` and there are more than
/// 65536 pieces. Debug and sanitizer builds also check `counts` against the ids.
template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatter(
    std::span<const IColumn * const> sources, std::span<const std::span<const Id>> ids, std::span<const size_t> counts);

/// Returns `num_pieces` counts: count k is the number of ids equal to k over all spans. Throws `LOGICAL_ERROR` before
/// counting when an id is `num_pieces` or more, or when `Id` is `UInt16` and `num_pieces` is more than 65536.
template <typename Id>
VectorWithMemoryTracking<size_t> countRowsPerPiece(std::span<const std::span<const Id>> ids, size_t num_pieces);


/// The API below scatters one fixed-width column chunk by chunk, with `UInt16` piece ids. The caller allocates the
/// pieces, for example with `allocateUninitializedFixed`. It then seeds one cursor per piece and calls
/// `scatterPidChunk` for each chunk.

constexpr size_t LINE_BYTES = 64;
/// Above this fanout, the 4 histogram lanes take more cache than breaking the dependency chain is worth.
constexpr size_t HIST_INTERLEAVE_MAX_FANOUT = 2048;
constexpr size_t SCATTER_BATCH_MIN_ROWS = 256 << 10;
constexpr size_t SCATTER_BATCH_LINES_PER_SHARD = 64;

/// The number of rows of a chunk for `fanout` pieces: at least `SCATTER_BATCH_MIN_ROWS`, and enough 8-byte rows for
/// `SCATTER_BATCH_LINES_PER_SHARD` cache lines per piece.
inline size_t scatterBatchRowsTarget(size_t fanout)
{
    return std::max(SCATTER_BATCH_MIN_ROWS, fanout * SCATTER_BATCH_LINES_PER_SHARD * (LINE_BYTES / sizeof(UInt64)));
}

/// Splits the `log2(fanout)` bits of a piece id into passes, most significant first. A pass has at most
/// `log2(max_fanout_per_pass)` bits, and a cap below 2 acts as 2: passes of 1 bit. The split uses the fewest passes
/// this cap allows, and every pass but the last has `ceil(log2(fanout) / passes)` bits: 15 bits under a 10-bit cap give
/// 8 + 7, not 10 + 5. Returns an empty list for a `fanout` of 1 or less. Otherwise `fanout` must be a power of two.
VectorWithMemoryTracking<size_t> computePassBits(size_t fanout, size_t max_fanout_per_pass);

/// The write cursors of one worker, one per piece. Call `seed` for each piece that gets rows before the first
/// `scatterPidChunk`. The cursors then keep their place from chunk to chunk.
struct ScatterScratch
{
    PaddedPODArray<char *> cursors;

    /// Makes `fanout` null cursors.
    void init(size_t fanout) { cursors.resize_fill(fanout, nullptr); }

    void seed(size_t piece, char * cursor) { cursors[piece] = cursor; }
};

/// `data` holds `n` rows of `width` bytes. Copies row i to the cursor of piece `pids[i]` and moves that cursor past the
/// row. Writes exactly `width` bytes per row.
void scatterPidChunk(size_t width, const UInt16 * pids, const char * data, size_t n, ScatterScratch & scratch);

/// Adds one to `hist[pid]` for each pid in `pids[0, n)`. When `lanes` is not null, it counts into `lanes` instead of
/// `hist`. `lanes` holds 4 * `fanout` counters, and `reduceHistogramLanes` adds them to `hist`. Both functions only add
/// to their counters, so the caller zeroes `hist` as well as `lanes`.
void histogramPidChunk(const UInt16 * pids, size_t n, UInt64 * hist, UInt64 * lanes, size_t fanout);
void reduceHistogramLanes(UInt64 * hist, const UInt64 * lanes, size_t fanout);

/// Returns an empty clone of `sample` grown to `rows` uninitialized values with no spare capacity, and the bytes of
/// these values. `sample` is a fixed-width column.
std::pair<MutableColumnPtr, std::span<char>> allocateUninitializedFixed(const IColumn & sample, size_t rows);

}
