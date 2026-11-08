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
/// 65536 pieces. Debug and sanitizer builds also check `counts` against the ids. `allow_combining` is the hint
/// of `chooseStoreMode`, which it asks for each fixed-width column that is written row by row.
template <typename Id>
VectorWithMemoryTracking<MutableColumnPtr> scatter(
    std::span<const IColumn * const> sources,
    std::span<const std::span<const Id>> ids,
    std::span<const size_t> counts,
    bool allow_combining = false);

/// Returns `num_pieces` counts: count k is the number of ids equal to k over all spans. Throws `LOGICAL_ERROR` before
/// counting when an id is `num_pieces` or more, or when `Id` is `UInt16` and `num_pieces` is more than 65536.
template <typename Id>
VectorWithMemoryTracking<size_t> countRowsPerPiece(std::span<const std::span<const Id>> ids, size_t num_pieces);


/// The API below scatters one fixed-width column chunk by chunk, with `UInt16` piece ids. The caller allocates the
/// pieces, for example with `allocateUninitializedFixed`. It initializes a `ScatterScratch` with the mode from
/// `chooseStoreMode`, seeds one cursor per piece, calls `scatterPidChunk` for each chunk, and calls `drain` after the
/// last chunk.

constexpr size_t LINE_BYTES = 64;
/// Below this many pieces, the write cursors of the pieces stay in cache, so staging rows in lines only adds work.
constexpr size_t COMBINING_MIN_PIECES = 256;
/// With more pieces, the staging lines (64 bytes per piece) would pass 4 MiB per column.
constexpr size_t COMBINING_MAX_PIECES = 65536;
/// The last line of a piece is usually partial and goes out with a regular copy. With fewer lines per piece than this
/// on average, that line holds a larger share of the rows.
constexpr size_t COMBINING_MIN_LINES_PER_PIECE = 2;
/// Whether `chooseStoreMode` ever returns `Combined`. Neoverse cores combine consecutive stores to one line in
/// hardware, without reading the line first, so staging only adds a copy. The non-temporal pair store is also slower
/// there than a regular one.
constexpr bool COMBINING_ENABLED =
#if defined(__aarch64__)
    false;
#else
    true;
#endif
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

/// How the fixed-width loop stores a row.
enum class StoreMode : UInt8
{
    /// The row goes straight to the write cursor of its piece.
    Direct,
    /// The row goes to a 64-byte staging line of its piece. A full line goes to the piece with one store: a
    /// non-temporal one when the piece's cursor is line-aligned, a regular one for the first, partial line of a piece.
    /// Only for widths 1, 2, 4, 8 and 16.
    Combined,
};

/// The mode for a fixed-width column of `width` bytes and `rows` rows split into `pieces` pieces. `allow_combining` is
/// the hint of a caller that knows the pieces are about equally big and the output is large, as in a hash partitioning.
/// Without the hint, and always when `COMBINING_ENABLED` is false, the mode is `Direct`. With it, the mode is `Combined`
/// for the widths 1, 2, 4, 8 and 16 with `COMBINING_MIN_PIECES` to `COMBINING_MAX_PIECES` pieces and at least
/// `COMBINING_MIN_LINES_PER_PIECE` lines per piece on average, and `Direct` otherwise. `scatter` calls it for each
/// fixed-width column with its own hint. A caller of the chunk API calls it for the whole column, not for a chunk.
StoreMode chooseStoreMode(size_t width, size_t pieces, size_t rows, bool allow_combining);

/// The write cursors of one worker, one per piece, and in the `Combined` mode the staging line and the fill of each
/// piece. Call `seed` for each piece that gets rows before the first `scatterPidChunk`. The cursors then keep their
/// place from chunk to chunk. After the last chunk of a column, call `drain` before anyone reads the pieces.
struct ScatterScratch
{
    /// The cursors. `init` puts the rest of the state in the same allocation, after them: the store mode, the fill of
    /// each piece and the staging lines. A scratch that stays `Direct` allocates the cursors only.
    PaddedPODArray<char *> cursors;

    StoreMode mode() const;

    /// Makes `fanout` null cursors, and for `Combined` the staging lines.
    void init(size_t fanout, StoreMode mode_);

    /// Changes the mode for the next column. Call it after `drain` and before `seed`. Throws `LOGICAL_ERROR` for
    /// `Combined` when `init` made no staging lines.
    void setStoreMode(StoreMode mode_);

    void seed(size_t piece, char * cursor);

    /// For `Combined`: stores the rows that are still staged and orders the non-temporal stores before later memory
    /// accesses. In both modes, it ends the column: every cursor is null afterwards, so seed the pieces of the next
    /// column that get rows.
    void drain();

    /// Only for a scratch that `init` gave staging lines. Per piece, the offset in its staging line where the next
    /// row goes. The staged bytes are `[cursor % 64, fill)`, because the first line of a piece starts at the
    /// misalignment of its cursor.
    UInt32 * fills();
    /// `fanout` lines of 64 bytes, aligned to 64 bytes.
    char * lines();

private:
    bool hasStaging() const { return cursors.capacity() > cursors.size(); }
    /// The bytes after the cursors, up to the end of the allocation.
    char * spare() { return reinterpret_cast<char *>(cursors.data() + cursors.size()); }
    const char * spare() const { return reinterpret_cast<const char *>(cursors.data() + cursors.size()); }
};

/// `data` holds `n` rows of `width` bytes. Copies row i to the cursor of piece `pids[i]` and moves that cursor past the
/// row. Writes exactly `width` bytes per row. `mode` is the mode of `scratch`. For `Combined`, the rows are staged and
/// reach the pieces at the latest in `drain`. Throws `LOGICAL_ERROR` when `mode` is not the mode of `scratch`, or when
/// `Combined` is asked for a width other than 1, 2, 4, 8 and 16, or for a piece whose cursor is not a multiple of
/// `width`.
void scatterPidChunk(size_t width, const UInt16 * pids, const char * data, size_t n, StoreMode mode, ScatterScratch & scratch);

/// Adds one to `hist[pid]` for each pid in `pids[0, n)`. When `lanes` is not null, it counts into `lanes` instead of
/// `hist`. `lanes` holds 4 * `fanout` counters, and `reduceHistogramLanes` adds them to `hist`. Both functions only add
/// to their counters, so the caller zeroes `hist` as well as `lanes`.
void histogramPidChunk(const UInt16 * pids, size_t n, UInt64 * hist, UInt64 * lanes, size_t fanout);
void reduceHistogramLanes(UInt64 * hist, const UInt64 * lanes, size_t fanout);

/// Returns an empty clone of `sample` grown to `rows` uninitialized values with no spare capacity, and the bytes of
/// these values. `sample` is a fixed-width column.
std::pair<MutableColumnPtr, std::span<char>> allocateUninitializedFixed(const IColumn & sample, size_t rows);

}
