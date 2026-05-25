#include <Storages/MergeTree/BernoulliGranuleFilter.h>
#include <Storages/MergeTree/MergeTreeIndexGranularity.h>

#include <base/defines.h>

#include <algorithm>
#include <cmath>
#include <cstring>


namespace DB
{

namespace
{
/// Geometric skip: compute the number of rows to skip before the next hit.
size_t nextGeometricSkip(pcg64 & rng, double log_one_minus_p)
{
    /// Generate a uniform random value u in the open interval (0, 1).
    /// We use the top 53 bits of the 64-bit PCG output (>> 11) to get 53 bits
    /// of mantissa precision (matching IEEE 754 double). Adding 1.0 to both
    /// numerator and denominator ensures u is strictly in (0, 1), avoiding
    /// log(0) in the geometric inverse-CDF below.
    double u = (static_cast<double>(rng() >> 11) + 1.0) * (1.0 / (double(1ULL << 53) + 1.0));
    double raw = std::floor(std::log(u) / log_one_minus_p);
    return raw < double(SIZE_MAX >> 1) ? static_cast<size_t>(raw) : SIZE_MAX >> 1;
}

/// Replay the geometric skip from a checkpoint, calling `callback(offset)` for each hit
/// in [starting_row, starting_row + num_rows). `offset` is relative to starting_row.
void replayRange(
    const std::vector<BernoulliGranuleFilter::Checkpoint> & checkpoints,
    double log_one_minus_p,
    size_t starting_row,
    size_t num_rows,
    auto && callback)
{
    if (checkpoints.empty() || num_rows == 0)
        return;

    /// Binary search for the checkpoint at or before starting_row.
    /// upper_bound finds the first checkpoint with mark_start_row > starting_row,
    /// so prev(it) is the last checkpoint with mark_start_row <= starting_row.
    auto it = std::upper_bound(
        std::begin(checkpoints), std::end(checkpoints), starting_row,
        [](size_t row, const auto & cp) { return row < cp.mark_start_row; });
    size_t lo = (it == std::begin(checkpoints)) ? 0 : std::distance(std::begin(checkpoints), std::prev(it));

    /// Restore RNG state from checkpoint.
    pcg64 rng = checkpoints[lo].rng;
    size_t abs_pos = checkpoints[lo].mark_start_row + checkpoints[lo].remaining_skip;

    size_t end_row = starting_row + num_rows;

    /// Advance past hits before our range.
    while (abs_pos < starting_row)
    {
        size_t skip = nextGeometricSkip(rng, log_one_minus_p) + 1;
        abs_pos += skip;
    }

    /// Emit hits within our range.
    while (abs_pos < end_row)
    {
        callback(abs_pos - starting_row);
        size_t skip = nextGeometricSkip(rng, log_one_minus_p) + 1;
        abs_pos += skip;
    }
}
}


bool BernoulliGranuleFilter::canSkipMark(size_t mark) const
{
    return mark < granules_selected.size() && !granules_selected.test(mark);
}

std::shared_ptr<BernoulliGranuleFilter>
BernoulliGranuleFilter::build(const MergeTreeIndexGranularity & index_granularity, size_t total_rows, Float64 probability, UInt64 part_seed)
{
    auto filter = std::make_shared<BernoulliGranuleFilter>();

    size_t num_marks = index_granularity.getMarksCountWithoutFinal();
    if (num_marks == 0 || total_rows == 0)
        return filter;

    chassert(probability > 0.0 && probability < 1.0);

    filter->log_one_minus_p = std::log(1.0 - probability);
    filter->granules_selected.resize(num_marks, false);
    filter->checkpoints.reserve((num_marks + CHECKPOINT_STRIDE - 1) / CHECKPOINT_STRIDE);

    pcg64 rng(part_seed);
    size_t remaining_skip = nextGeometricSkip(rng, filter->log_one_minus_p);

    size_t cumulative_row = 0;
    for (size_t mark = 0; mark < num_marks; ++mark)
    {
        /// Save a sparse checkpoint at the start of every CHECKPOINT_STRIDE marks.
        /// Replay binary-searches for the latest checkpoint at-or-before its
        /// `starting_row` and walks the geometric-skip sequence forward from there.
        if (mark % CHECKPOINT_STRIDE == 0)
            filter->checkpoints.emplace_back(cumulative_row, remaining_skip, rng);

        const size_t original_rows_in_mark = index_granularity.getMarkRows(mark);
        size_t rows_in_mark = original_rows_in_mark;
        /// Clamp the last granule to actual row count.
        if (cumulative_row + rows_in_mark > total_rows)
            rows_in_mark = total_rows - cumulative_row;

        /// Walk hits across this mark. Each iteration records one hit at
        /// offset `remaining_skip` within the unconsumed tail of the mark,
        /// advances past it, and draws the next skip distance.
        while (remaining_skip < rows_in_mark)
        {
            filter->granules_selected.set(mark);
            const size_t rows_consumed = remaining_skip + 1; /// skipped rows + the hit row itself
            rows_in_mark -= rows_consumed;
            remaining_skip = nextGeometricSkip(rng, filter->log_one_minus_p);
        }

        /// The next hit lies past the end of this mark; carry the leftover
        /// skip distance over into the next mark.
        remaining_skip -= rows_in_mark;
        cumulative_row += original_rows_in_mark;
        cumulative_row = std::min(cumulative_row, total_rows);
    }

    return filter;
}

void BernoulliGranuleFilter::appendToFilter(PaddedPODArray<UInt8> & filter_data, size_t starting_row, size_t num_rows) const
{
    size_t old_size = filter_data.size();
    filter_data.resize_fill(old_size + num_rows, 0);

    replayRange(checkpoints, log_one_minus_p, starting_row, num_rows, [&](size_t offset) { filter_data[old_size + offset] = 1; });
}

void BernoulliGranuleFilter::andWithFilter(
    PaddedPODArray<UInt8> & filter_data, size_t filter_offset, size_t starting_row, size_t num_rows) const
{
    /// Zero-allocation clear-then-set: zero non-hit positions in-place,
    /// keep existing filter values at hit positions (AND with 1 = no-op).
    size_t next_zero_from = 0;
    replayRange(checkpoints, log_one_minus_p, starting_row, num_rows, [&](size_t offset)
    {
        /// Zero positions [next_zero_from, offset) - these are NOT Bernoulli hits.
        if (offset > next_zero_from)
            memset(&filter_data[filter_offset + next_zero_from], 0, offset - next_zero_from);
        /// Position 'offset' IS a hit - keep existing filter value.
        next_zero_from = offset + 1;
    });
    /// Zero remaining positions after last hit.
    if (num_rows > next_zero_from)
        memset(&filter_data[filter_offset + next_zero_from], 0, num_rows - next_zero_from);
}

}
