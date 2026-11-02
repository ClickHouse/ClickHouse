#pragma once

#include <Interpreters/AdaptiveAggregation/MultiPassTopK/BoundingKey.h>
#include <Interpreters/AdaptiveAggregation/MultiPassTopK/TopKCandidates.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <array>
#include <limits>
#include <mutex>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

/// The sketch of the bounding key of a multi-pass top-K aggregation (see `TopKBoundingKey`): the rows of its values in
/// hash cells, which bound the rows of the values that the restricted passes do not take.

namespace DB::MultiPassTopKImpl
{

/// The sketch summarizes the rows of the values of the bounding key in this many hash cells, on the high bits of the
/// CRC32 of a value. The streams merge their sketches a stripe of cells at a time.
constexpr size_t key_sketch_cell_bits = 16;
constexpr size_t key_sketch_cells = size_t{1} << key_sketch_cell_bits;
constexpr size_t key_sketch_stripes = 64;

/// The sketch also counts the rows of finer cells, on more high bits of the same hash, so every cell holds a run of
/// fine cells. A fine cell holds few values, so the few that hold many rows make their fine cells stand out. The
/// counters of a stream are a byte each, to keep its fine cells in fewer cache lines, and a counter that would reach
/// the limit moves its rows to the overflow of its stream: few fine cells hold that many rows.
constexpr size_t key_sketch_fine_cell_bits = 20;
constexpr size_t key_sketch_fine_cells = size_t{1} << key_sketch_fine_cell_bits;
constexpr size_t key_sketch_fine_cells_per_cell = key_sketch_fine_cells / key_sketch_cells;
constexpr UInt64 fine_rows_limit = std::numeric_limits<UInt8>::max();

/// The rows of the fine cells of a stream: a byte per cell, and the rows that overflowed it.
struct TopKKeySketchFineRows
{
    std::vector<UInt8> rows = std::vector<UInt8>(key_sketch_fine_cells);
    std::unordered_map<UInt32, UInt64> overflow;

    ALWAYS_INLINE void add(UInt32 cell, UInt64 count)
    {
        const UInt64 sum = rows[cell] + count;
        if (sum < fine_rows_limit)
        {
            rows[cell] = static_cast<UInt8>(sum);
        }
        else
        {
            overflow[cell] += sum;
            rows[cell] = 0;
        }
    }
};

/// The rows of the values of the bounding key in hash cells, which bound the rows of every value of a cell from above,
/// and a candidate value of every cell with its net votes: a Misra-Gries summary of one counter, whose candidate is the
/// value that holds most rows of its cell, if one does. The values are the bits of the key (see `withKeyData`).
struct TopKKeySketchCell
{
    UInt64 rows = 0;
    UInt64 value = 0;
    UInt64 votes = 0;

    /// Merges `count` votes for `candidate` into the candidate of the cell.
    ALWAYS_INLINE void vote(UInt64 candidate, UInt64 count)
    {
        if (value == candidate)
        {
            votes += count;
        }
        else if (votes < count)
        {
            value = candidate;
            votes = count - votes;
        }
        else
        {
            votes -= count;
        }
    }

    /// The fewest rows that `candidate` holds: its votes when it is the candidate of the cell, which count its rows
    /// less those that the rows of other values cancelled, and none otherwise.
    UInt64 minRowsOf(UInt64 candidate) const { return value == candidate ? votes : 0; }

    /// The most rows that a value of the cell other than the candidate holds: the cancelled votes pair the rows of
    /// different values, so another value holds at most half of the rows that the votes of the candidate miss.
    UInt64 maxRowsOfOther() const { return (rows - votes) / 2; }

    /// The most rows that a value of the cell holds: the candidate holds at most its votes more than another value.
    UInt64 maxRowsOfAny() const { return votes + maxRowsOfOther(); }
};

/// The fields of a cell are together, so a run of a value touches one cache line of the cells.
using TopKKeySketchCells = std::vector<TopKKeySketchCell>;

/// The bounds of the values of the bounding key other than some values that a pass took, whose rows it counted exactly
/// (see `TopKKeySketch::boundsOfOtherValues`): the most rows that such a value of every cell may hold, the rows of the
/// taken values in every fine cell that holds one of them, and their rows in all.
struct TopKOtherValueBounds
{
    std::vector<UInt64> cells;
    std::unordered_map<size_t, UInt64> fine_rows_of_taken;
    UInt64 rows_of_taken = 0;
};

/// The sketch of a bounding mode, which the streams of its pass merge into. The other methods read it once every stream
/// merged its own.
class TopKKeySketch
{
public:
    /// Merges the sketch of a stream, starting from the stripe `first`, so that the streams that finish at once merge
    /// different stripes. A stripe of cells covers a stripe of fine cells.
    void merge(const TopKKeySketchCells & local, const TopKKeySketchFineRows & local_fine, size_t first);

    /// The rows of the input, which the cells partition.
    UInt64 rows() const;

    /// The candidates of the `count` cells of the most rows, each value once. `count` is positive.
    std::unordered_set<UInt64> candidatesOfHeaviestCells(size_t count) const;

    /// The fewest rows that `values` hold together: the votes of those that are the candidates of their cells (see
    /// `TopKKeySketchCell::minRowsOf`).
    UInt64 minRowsOf(const std::unordered_set<UInt64> & values) const;

    /// The bounds of the values other than `taken`, the values that a pass took, whose rows it counted in
    /// `rows_of_taken`. A value of a cell other than `taken` holds at most the rows of the cell less those of `taken`,
    /// and at most what the votes of the cell leave to it (see `TopKKeySketchCell::maxRowsOfOther`).
    TopKOtherValueBounds boundsOfOtherValues(
        const std::unordered_set<UInt64> & taken, const std::unordered_map<UInt64, UInt64> & rows_of_taken) const;

    /// The fine cells where a value other than the taken ones may hold `min_rows` rows, and their rows less those of
    /// the taken values. Such a value is in a fine cell whose rows less those of the taken values reach `min_rows`, of
    /// a cell whose bound reaches them.
    std::pair<std::shared_ptr<TopKCellSet>, UInt64> hotFineCells(const TopKOtherValueBounds & bounds, UInt64 min_rows) const;

private:
    std::array<std::mutex, key_sketch_stripes> mutexes;
    TopKKeySketchCells merged = TopKKeySketchCells(key_sketch_cells);
    std::vector<UInt64> fine_rows = std::vector<UInt64>(key_sketch_fine_cells);
};

using TopKKeySketchPtr = std::shared_ptr<TopKKeySketch>;

/// The filter of the values of the hot fine cells, other than `excluded` (see `FunctionTopKHotFineCells`).
TopKKeyFilter makeHotFineCellsFilter(
    const TopKBoundingKey & key, std::shared_ptr<const TopKCellSet> hot_cells, std::shared_ptr<const std::unordered_set<UInt64>> excluded);

/// The sketch pass of the bounding mode: summarizes the values of the bounding key of the whole input into `sketch`,
/// reading only the columns of the key and the filters.
QueryPlan makeKeySketchPlan(const MultiPassTopKTemplate & templ, const TopKKeySketchPtr & sketch);

}
