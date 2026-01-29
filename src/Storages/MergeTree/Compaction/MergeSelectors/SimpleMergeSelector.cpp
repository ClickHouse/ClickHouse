#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/MergeSelectorFactory.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/DisjointPartsRangesSet.h>

#include <base/interpolate.h>

#include <Common/thread_local_rng.h>

#include <algorithm>
#include <cmath>
#include <cassert>
#include <random>

namespace DB
{

void registerSimpleMergeSelector(MergeSelectorFactory & factory)
{
    factory.registerPublicSelector("Simple", MergeSelectorAlgorithm::SIMPLE, [](const std::any & settings)
    {
        return std::make_shared<SimpleMergeSelector>(std::any_cast<SimpleMergeSelector::Settings>(settings));
    });
}

void registerStochasticSimpleMergeSelector(MergeSelectorFactory & factory)
{
    factory.registerPublicSelector("StochasticSimple", MergeSelectorAlgorithm::STOCHASTIC_SIMPLE, [](const std::any & settings)
    {
        return std::make_shared<SimpleMergeSelector>(std::any_cast<SimpleMergeSelector::Settings>(settings));
    });
}

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

    void consider(RangesIterator range_it, PartsIterator begin, PartsIterator end, size_t sum_size, size_t size_prev_at_left, const SimpleMergeSelector::Settings & settings)
    {
        if (settings.enable_heuristic_to_remove_small_parts_at_right)
            while (end >= begin + 3 && (end - 1)->size < settings.heuristic_to_remove_small_parts_at_right_max_ratio * sum_size)
                --end;

        double current_score = score(end - begin, sum_size, settings.size_fixed_cost_to_add);

        if (settings.enable_heuristic_to_align_parts
            && size_prev_at_left > sum_size * settings.heuristic_to_align_parts_min_ratio_of_sum_size_to_prev_part)
        {
            double difference = std::abs(log2(static_cast<double>(sum_size) / size_prev_at_left));
            if (difference < settings.heuristic_to_align_parts_max_absolute_difference_in_powers_of_two)
                current_score *= interpolateLinear(settings.heuristic_to_align_parts_max_score_adjustment, 1,
                    difference / settings.heuristic_to_align_parts_max_absolute_difference_in_powers_of_two);
        }

        ranges.emplace_back(range_it, begin, end, sum_size, current_score);
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

    std::optional<PartsRange> buildMergeRange(size_t max_total_size_to_merge)
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
            const auto [range_it, range_begin, range_end, size, _] = std::move(ranges.back());
            ranges.pop_back();

            if (size <= max_total_size_to_merge && disjoint_set.addRangeIfPossible(range_it, range_begin, range_end))
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
        size_t size = 0;
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

    if (settings.min_age_to_force_merge && min_age >= settings.min_age_to_force_merge)
        return true;

    const size_t size = end - begin;

    if (settings.min_parts_to_merge_at_once && size < settings.min_parts_to_merge_at_once)
        return false;

    /// Map size to 0..1 using logarithmic scale
    /// Use log(1 + x) instead of log1p(x) because our sum_size is always integer.
    /// Also log1p seems to be slow and significantly affect performance of merges assignment.
    double size_normalized = mapPiecewiseLinearToUnit(log(1 + sum_size), min_size_to_lower_base_log, max_size_to_lower_base_log);
    /// Calculate boundaries for age
    double min_age_to_lower_base = interpolateLinear(settings.min_age_to_lower_base_at_min_size, settings.min_age_to_lower_base_at_max_size, size_normalized);
    double max_age_to_lower_base = interpolateLinear(settings.max_age_to_lower_base_at_min_size, settings.max_age_to_lower_base_at_max_size, size_normalized);
    /// Map age to 0..1
    double age_normalized = mapPiecewiseLinearToUnit(min_age, min_age_to_lower_base, max_age_to_lower_base);
    /// Map partition_size to 0..1
    double num_parts_normalized = mapPiecewiseLinearToUnit(partition_size, settings.min_parts_to_lower_base, settings.max_parts_to_lower_base);
    /// The ratio should be within [0, 1]
    double combined_ratio = std::min(1.0, age_normalized + num_parts_normalized);

    double lowered_base = interpolateLinear(settings.base, 2.0, combined_ratio);
    if (settings.use_blurry_base)
    {
        double partition_fill_factor = std::max(0., 1 - partition_size / settings.parts_to_throw_insert);
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

    return (sum_size + size * settings.size_fixed_cost_to_add) / (max_size + settings.size_fixed_cost_to_add) >= lowered_base;
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
    const size_t max_total_size_to_merge,
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

    for (; begin < parts_count; ++begin)
    {
        size_t sum_size = parts[begin].size;
        size_t max_size = parts[begin].size;
        size_t min_age = parts[begin].age;

        for (size_t end = begin + 2; end <= parts_count; ++end)
        {
            assert(end > begin);
            if (settings.max_parts_to_merge_at_once && end - begin > settings.max_parts_to_merge_at_once)
                break;

            size_t cur_size = parts[end - 1].size;
            size_t cur_age = parts[end - 1].age;

            sum_size += cur_size;
            max_size = std::max(max_size, cur_size);
            min_age = std::min(min_age, cur_age);

            if (sum_size > max_total_size_to_merge)
                break;

            auto range_begin = parts.begin() + begin;
            auto range_end = parts.begin() + end;

            if (allow(sum_size, max_size, min_age, parts_count, min_size_to_lower_base_log, max_size_to_lower_base_log, range_begin, range_end, range_filter, settings))
                estimator.consider(
                    range_it,
                    range_begin,
                    range_end,
                    sum_size,
                    begin == 0 ? 0 : parts[begin - 1].size,
                    settings);
        }
    }
}

}

PartsRanges SimpleMergeSelector::select(
    const PartsRanges & parts_ranges,
    const MergeSizes & max_merge_sizes,
    const RangeFilter & range_filter) const
{
    Estimator estimator(parts_ranges);

    /// Precompute logarithm of settings boundaries, because log function is quite expensive in terms of performance
    const double min_size_to_lower_base_log = log(1 + settings.min_size_to_lower_base);
    const double max_size_to_lower_base_log = log(1 + settings.max_size_to_lower_base);

    /// Using max size constraint to create more merge candidates
    for (auto range_it = parts_ranges.begin(); range_it != parts_ranges.end(); ++range_it)
        selectWithinPartsRange(range_it, max_merge_sizes[0], range_filter, estimator, settings, min_size_to_lower_base_log, max_size_to_lower_base_log);

    PartsRanges result;
    for (size_t max_merge_size : max_merge_sizes)
    {
        if (auto range = estimator.buildMergeRange(max_merge_size))
            result.push_back(std::move(range.value()));
        else
            break;
    }

    return result;
}

}
