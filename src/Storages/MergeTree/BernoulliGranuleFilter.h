#pragma once

#include <Core/Types.h>
#include <Common/PODArray.h>

#include <memory>
#include <vector>

#include <boost/dynamic_bitset.hpp>
#include <pcg_random.hpp>


namespace DB
{

class MergeTreeIndexGranularity;

/// Pre-computed per-part Bernoulli filter that determines which granules to read
/// and which rows within each granule to select. Built once per part before
/// distributing work to threads, ensuring thread-count-independent determinism.
///
///   Build (single-threaded, once per part):
///
///     mark 0         mark 1         mark 2         mark 3        ...   mark K
///     |-- 8192 rows --|-- 8192 rows --|-- 8192 rows --|-- ...           |-- ...
///     *  *   *  *      *     *        (none)         *  *               *
///     ^ckpt[0]                                                          ^ckpt[1]
///     selected: 1      selected: 1    selected: 0    selected: 1  ...   selected: 1
///
///   Read (per thread, any row range):
///     binary-search checkpoints -> restore RNG -> replay the geometric skip
///     forward (up to `CHECKPOINT_STRIDE` marks) until reaching the row range.
///
/// Each checkpoint stores `(mark_start_row, remaining_skip, rng_state)` so that any
/// thread can replay the exact hit sequence for its row range. One checkpoint is
/// kept every `CHECKPOINT_STRIDE` marks, so the per-part checkpoint memory is
/// `O(marks / CHECKPOINT_STRIDE)`. The `granules_selected` bitmap is still one bit
/// per mark and is consulted by `canSkipMark`.
class BernoulliGranuleFilter
{
public:
    struct Checkpoint
    {
        size_t mark_start_row; /// absolute row index of mark's first row
        size_t remaining_skip; /// geometric skip counter at mark boundary (rows remaining before next hit)
        pcg64 rng; /// RNG state at mark boundary
    };

    /// Save one checkpoint every this many marks. Replay walks at most
    /// `CHECKPOINT_STRIDE - 1` marks of the geometric-skip sequence to seek
    /// from the resumed checkpoint to the requested row range. Increasing this
    /// shrinks per-part memory linearly and grows the per-replay walk linearly;
    /// 16 keeps the worst-case walk well below per-granule scan cost while
    /// reducing checkpoint memory ~16x compared to one-checkpoint-per-mark.
    static constexpr size_t CHECKPOINT_STRIDE = 16;

    /// Returns true if the mark contains no sampled rows and can be skipped.
    bool canSkipMark(size_t mark) const;

    /// Replay filter into a fresh column: append num_rows values (0 or 1).
    void appendToFilter(PaddedPODArray<UInt8> & filter_data, size_t starting_row, size_t num_rows) const;

    /// AND replay with existing filter: zero positions NOT selected by Bernoulli.
    void andWithFilter(PaddedPODArray<UInt8> & filter_data, size_t filter_offset, size_t starting_row, size_t num_rows) const;

    /// Build a Bernoulli filter for a single part.
    /// Walks the geometric-skip sequence once for the entire part and records
    /// which granules contain sampled rows and saves RNG checkpoints at each mark boundary.
    ///
    /// @param index_granularity  Granularity metadata for the part
    /// @param total_rows         Actual row count of the part (from part metadata)
    /// @param probability        Sampling probability in (0, 1)
    /// @param part_seed          Deterministic seed for this part
    static std::shared_ptr<BernoulliGranuleFilter>
    build(const MergeTreeIndexGranularity & index_granularity, size_t total_rows, Float64 probability, UInt64 part_seed);

private:
    /// Which granules contain at least one sampled row.
    boost::dynamic_bitset<> granules_selected;

    /// One checkpoint every `CHECKPOINT_STRIDE` marks, for geometric skip replay.
    std::vector<Checkpoint> checkpoints;

    /// Cached log(1 - p) for geometric skip replay.
    double log_one_minus_p = 0;
};

}
