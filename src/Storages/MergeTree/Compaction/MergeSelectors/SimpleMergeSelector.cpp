#include <Storages/MergeTree/Compaction/MergeSelectors/PartitionStatistics.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/DisjointPartsRangesSet.h>

#include <base/interpolate.h>

#include <Common/thread_local_rng.h>

#include <algorithm>
#include <cmath>
#include <cassert>
#include <random>

namespace DB
{

namespace
{

/** Estimates best set of parts to merge within passed alternatives.
  */
class Estimator
{
public:
    explicit Estimator(const PartsRanges & parts_ranges)
        : disjoint_set(parts_ranges)
    {
    }

    void consider(RangesIterator range_it, PartsIterator begin, PartsIterator end, size_t sum_size, size_t sum_rows, size_t size_prev_at_left, const SimpleMergeSelector::Settings & settings)
    {
        if (settings.enable_heuristic_to_remove_small_parts_at_right)
            while (end >= begin + 3 && static_cast<double>((end - 1)->size) < settings.heuristic_to_remove_small_parts_at_right_max_ratio * static_cast<double>(sum_size))
                --end;

        double current_score = score(static_cast<double>(end - begin), static_cast<double>(sum_size), static_cast<double>(settings.size_fixed_cost_to_add));

        if (settings.enable_heuristic_to_align_parts
            && static_cast<double>(size_prev_at_left) > static_cast<double>(sum_size) * settings.heuristic_to_align_parts_min_ratio_of_sum_size_to_prev_part)
        {
            double difference = std::abs(log2(static_cast<double>(sum_size) / static_cast<double>(size_prev_at_left)));
            if (difference < settings.heuristic_to_align_parts_max_absolute_difference_in_powers_of_two)
                current_score *= interpolateLinear(settings.heuristic_to_align_parts_max_score_adjustment, 1,
                    difference / settings.heuristic_to_align_parts_max_absolute_difference_in_powers_of_two);
        }

        ranges.emplace_back(range_it, begin, end, sum_size, sum_rows, current_score);
    }

    static double score(double count, double sum_size, double sum_size_fixed_cost)
    {
        /** Consider we have two alternative ranges of data parts to merge.
          * Assume time to merge a range is proportional to sum size of its parts.
          *
          * Cost of query execution is proportional to total number of data parts in a moment of time.
          * Let define our target: to minimize average (in time) total number of data parts.
          *
          * Let calculate integral of total number of parts, if we are going to do merge of one or another range.
          * It must be lower, and thus we decide, what range is better to merge.
          *
          * The integral is lower iff the following formula is lower:
          *
          *  sum_size / (count - 1)
          *
          * But we have some tunes to prefer longer ranges.
          */
        return (sum_size + sum_size_fixed_cost * count) / (count - 1.9);
    }

    std::optional<PartsRange> buildMergeRange(const MergeConstraint & constraint)
    {
        constexpr static auto range_compare = [](const ScoredRange & lhs, const ScoredRange & rhs)
        {
            /// If the ranges have the same score, use the range that is most likely to have the lower merge level -
            /// the right one, according to the block number sorting.
            return lhs.score > rhs.score || (lhs.score == rhs.score && lhs.range_begin < rhs.range_begin);
        };

        if (!is_heap_constructed)
        {
            std::make_heap(ranges.begin(), ranges.end(), range_compare);
            is_heap_constructed = true;
        }

        while (!ranges.empty())
        {
            std::pop_heap(ranges.begin(), ranges.end(), range_compare);
            const auto [range_it, range_begin, range_end, bytes, rows, _] = std::move(ranges.back());
            ranges.pop_back();

            if (bytes <= constraint.max_size_bytes && rows <= constraint.max_size_rows && disjoint_set.addRangeIfPossible(range_it, range_begin, range_end))
                return PartsRange(range_begin, range_end);
        }

        return std::nullopt;
    }

private:
    struct ScoredRange
    {
        RangesIterator range_it;
        PartsIterator range_begin;
        PartsIterator range_end;
        size_t bytes = 0;
        size_t rows = 0;
        double score;
    };

    DisjointPartsRangesSet disjoint_set;
    std::vector<ScoredRange> ranges;
    bool is_heap_constructed = false;
};


/**
 * 1       _____
 *        /
 * 0_____/
 *      ^  ^
 *     min max
 */
double mapPiecewiseLinearToUnit(double value, double min, double max)
{
    return value <= min ? 0
        : (value >= max ? 1
        : ((value - min) / (max - min)));
}


/** Is allowed to merge parts in range with specific properties.
  */
bool allow(
    double sum_size,
    double max_size,
    double min_age,
    double partition_size,
    double min_size_to_lower_base_log,
    double max_size_to_lower_base_log,
    PartsIterator begin,
    PartsIterator end,
    const IMergeSelector::RangeFilter & range_filter,
    const SimpleMergeSelector::Settings & settings)
{
    if (range_filter && !range_filter({begin, end}))
        return false;

    if (settings.min_age_to_force_merge && min_age >= static_cast<double>(settings.min_age_to_force_merge))
        return true;

    const size_t size = end - begin;

    if (settings.min_parts_to_merge_at_once && size < settings.min_parts_to_merge_at_once)
        return false;

    /// Map size to 0..1 using logarithmic scale
    /// Use log(1 + x) instead of log1p(x) because our sum_size is always integer.
    /// Also log1p seems to be slow and significantly affect performance of merges assignment.
    double size_normalized = mapPiecewiseLinearToUnit(log(1 + sum_size), min_size_to_lower_base_log, max_size_to_lower_base_log);
    /// Calculate boundaries for age
    double min_age_to_lower_base = interpolateLinear(static_cast<double>(settings.min_age_to_lower_base_at_min_size), static_cast<double>(settings.min_age_to_lower_base_at_max_size), size_normalized);
    double max_age_to_lower_base = interpolateLinear(static_cast<double>(settings.max_age_to_lower_base_at_min_size), static_cast<double>(settings.max_age_to_lower_base_at_max_size), size_normalized);
    /// Map age to 0..1
    double age_normalized = mapPiecewiseLinearToUnit(min_age, min_age_to_lower_base, max_age_to_lower_base);
    /// Map partition_size to 0..1
    double num_parts_normalized = mapPiecewiseLinearToUnit(partition_size, static_cast<double>(settings.min_parts_to_lower_base), static_cast<double>(settings.max_parts_to_lower_base));
    /// The ratio should be within [0, 1]
    double combined_ratio = std::min(1.0, age_normalized + num_parts_normalized);

    double lowered_base = interpolateLinear(settings.base, 2.0, combined_ratio);
    if (settings.use_blurry_base)
    {
        double partition_fill_factor = std::max(0., 1 - partition_size / static_cast<double>(settings.parts_to_throw_insert));
        /// Scale factor controls when (relativelty to the number of parts in partition)
        /// do we activate our special algorithm.
        /// With standard parameters the logic kicks in starting from 80% empty factor.
        /// The division by 2 is due to the fact that for normal distribution nearly 95.4%
        /// of all observations fall within two standard deviations.
        double scaling_factor = std::pow(partition_fill_factor, settings.blurry_base_scale_factor) / 2;
        /// The base lower than 1 doesn't make sense, so we try to avoid it.
        std::normal_distribution<double> distribution{lowered_base, (lowered_base - 1) * scaling_factor};
        /// The threshold should be strictly bigger than 1, because we don't allow to merge the part with itself.
        lowered_base = std::min(distribution(thread_local_rng), std::max(1.01, lowered_base));
    }

    return (sum_size + static_cast<double>(size) * static_cast<double>(settings.size_fixed_cost_to_add)) / (max_size + static_cast<double>(settings.size_fixed_cost_to_add)) >= lowered_base;
}


size_t calculateRangeWithStochasticSliding(size_t parts_count, size_t parts_threshold)
{
    auto mean = static_cast<double>(parts_count);
    std::normal_distribution<double> distribution{mean, mean / 4};

    size_t right_boundary = static_cast<size_t>(distribution(thread_local_rng));
    if (right_boundary > parts_count)
        right_boundary = 2 * parts_count - right_boundary;

    right_boundary = std::max(right_boundary, parts_threshold);

    return right_boundary - parts_threshold;
}

void selectWithinPartsRange(
    RangesIterator range_it,
    const MergeConstraint & constraint,
    const IMergeSelector::RangeFilter & range_filter,
    Estimator & estimator,
    const SimpleMergeSelector::Settings & settings,
    double min_size_to_lower_base_log,
    double max_size_to_lower_base_log)
{
    const PartsRange & parts = *range_it;
    size_t parts_count = parts.size();
    if (parts_count <= 1)
        return;

    /// If the parts in the parts vector are sorted by block number,
    /// it may not be ideal to only select parts for merging from the first N ones.
    /// This is because if there are more than N parts in the partition,
    /// we will not be able to assign a merge for newly created parts.
    /// As a result, the total number of parts within the partition could
    /// grow uncontrollably, similar to a snowball effect.
    /// To address this we will try to assign a merge taking into consideration
    /// only last N parts.
    const size_t parts_threshold = settings.window_size;
    size_t begin = 0;
    if (parts_count >= parts_threshold)
    {
        if (settings.enable_stochastic_sliding)
            begin = calculateRangeWithStochasticSliding(parts_count, parts_threshold);
        else
            begin = parts_count - parts_threshold;
    }

    /// Enable heuristic for lowering selected merge ranges. This can increase number of
    /// concurrently running merges and thus increase the merge speed.
    size_t max_parts_to_merge_at_once = settings.max_parts_to_merge_at_once;
    if (settings.enable_heuristic_to_lower_max_parts_to_merge_at_once)
    {
        assert(settings.partitions_stats);
        assert(range_it->size() > 1);
        const auto & partition_stats = settings.partitions_stats->at(range_it->front().info.getPartitionId());

        if (static_cast<double>(partition_stats.part_count) < settings.base)
        {
            /// Partition is not filled
        }
        else if (partition_stats.part_count >= settings.parts_to_throw_insert)
        {
            /// Partition is fully filled - let's lower the max parts to merge to base to enable only small merges
            max_parts_to_merge_at_once = std::max<size_t>(2, static_cast<size_t>(settings.base));
        }
        else
        {
            /// Partition is not fully filled but but may be approaching it. Let's lower max parts to merge according to the fullness.
            size_t exponent = settings.heuristic_to_lower_max_parts_to_merge_at_once_exponent;
            max_parts_to_merge_at_once = static_cast<size_t>(
                settings.base +
                (static_cast<double>(max_parts_to_merge_at_once) - settings.base) * (1.0 - std::pow((static_cast<double>(partition_stats.part_count) - settings.base) / (static_cast<double>(settings.parts_to_throw_insert) - settings.base), exponent))
            );
        }
    }

    for (; begin < parts_count; ++begin)
    {
        size_t sum_size = parts[begin].size;
        size_t sum_rows = parts[begin].rows;
        size_t max_size = parts[begin].size;
        size_t min_age = parts[begin].age;

        for (size_t end = begin + 2; end <= parts_count; ++end)
        {
            assert(end > begin);
            if (max_parts_to_merge_at_once && end - begin > max_parts_to_merge_at_once)
                break;

            size_t cur_size = parts[end - 1].size;
            size_t cur_age = parts[end - 1].age;
            size_t cur_rows = parts[end - 1].rows;

            sum_size += cur_size;
            sum_rows += cur_rows;
            max_size = std::max(max_size, cur_size);
            min_age = std::min(min_age, cur_age);

            if (sum_size > constraint.max_size_bytes)
                break;

            if (sum_rows > constraint.max_size_rows)
                break;

            auto range_begin = parts.begin() + begin;
            auto range_end = parts.begin() + end;

            if (allow(
                    static_cast<double>(sum_size),
                    static_cast<double>(max_size),
                    static_cast<double>(min_age),
                    static_cast<double>(parts_count),
                    min_size_to_lower_base_log,
                    max_size_to_lower_base_log,
                    range_begin,
                    range_end,
                    range_filter,
                    settings))
                estimator.consider(
                    range_it,
                    range_begin,
                    range_end,
                    sum_size,
                    sum_rows,
                    begin == 0 ? 0 : parts[begin - 1].size,
                    settings);
        }
    }
}

}

PartsRanges SimpleMergeSelector::select(
    const PartsRanges & parts_ranges,
    const MergeConstraints & merge_constraints,
    const RangeFilter & range_filter) const
{
    Estimator estimator(parts_ranges);

    /// Precompute logarithm of settings boundaries, because log function is quite expensive in terms of performance
    const double min_size_to_lower_base_log = log(1 + settings.min_size_to_lower_base);
    const double max_size_to_lower_base_log = log(1 + settings.max_size_to_lower_base);

    /// Using max size constraint to create more merge candidates
    for (auto range_it = parts_ranges.begin(); range_it != parts_ranges.end(); ++range_it)
        selectWithinPartsRange(range_it, merge_constraints[0], range_filter, estimator, settings, min_size_to_lower_base_log, max_size_to_lower_base_log);

    PartsRanges result;
    for (const auto & constraint : merge_constraints)
    {
        if (auto range = estimator.buildMergeRange(constraint))
            result.push_back(std::move(range.value()));
        else
            break;
    }

    return result;
}

}
