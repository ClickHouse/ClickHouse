#include <Storages/MergeTree/SimpleMergeSelector.h>

#include <Common/interpolate.h>

#include <cmath>
#include <cassert>
#include <iostream>


namespace DB
{

namespace
{

/** Estimates best set of parts to merge within passed alternatives.
  */
struct Estimator
{
    using Iterator = SimpleMergeSelector::PartsRange::const_iterator;

    void consider(Iterator begin, Iterator end, size_t sum_size, size_t size_prev_at_left, const SimpleMergeSelector::Settings & settings)
    {
        double current_score = score(end - begin, sum_size, settings.size_fixed_cost_to_add);

        if (settings.enable_heuristic_to_align_parts
            && size_prev_at_left > sum_size * settings.heuristic_to_align_parts_min_ratio_of_sum_size_to_prev_part)
        {
            double difference = std::abs(log2(static_cast<double>(sum_size) / size_prev_at_left));
            if (difference < settings.heuristic_to_align_parts_max_absolute_difference_in_powers_of_two)
                current_score *= interpolateLinear(settings.heuristic_to_align_parts_max_score_adjustment, 1,
                    difference / settings.heuristic_to_align_parts_max_absolute_difference_in_powers_of_two);
        }

        if (settings.enable_heuristic_to_remove_small_parts_at_right)
            while (end >= begin + 3 && (end - 1)->size < settings.heuristic_to_remove_small_parts_at_right_max_ratio * sum_size)
                --end;

        if (!min_score || current_score < min_score)
        {
            min_score = current_score;
            best_begin = begin;
            best_end = end;
        }
    }

    SimpleMergeSelector::PartsRange getBest() const
    {
        return SimpleMergeSelector::PartsRange(best_begin, best_end);
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

    double min_score = 0;
    Iterator best_begin {};
    Iterator best_end {};
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
    double range_size,
    double partition_size,
    double min_size_to_lower_base_log,
    double max_size_to_lower_base_log,
    const SimpleMergeSelector::Settings & settings)
{
//    std::cerr << "sum_size: " << sum_size << "\n";

    /// Map size to 0..1 using logarithmic scale
    /// Use log(1 + x) instead of log1p(x) because our sum_size is always integer.
    /// Also log1p seems to be slow and significantly affect performance of merges assignment.
    double size_normalized = mapPiecewiseLinearToUnit(log(1 + sum_size), min_size_to_lower_base_log, max_size_to_lower_base_log);

//    std::cerr << "size_normalized: " << size_normalized << "\n";

    /// Calculate boundaries for age
    double min_age_to_lower_base = interpolateLinear(settings.min_age_to_lower_base_at_min_size, settings.min_age_to_lower_base_at_max_size, size_normalized);
    double max_age_to_lower_base = interpolateLinear(settings.max_age_to_lower_base_at_min_size, settings.max_age_to_lower_base_at_max_size, size_normalized);

//    std::cerr << "min_age_to_lower_base: " << min_age_to_lower_base << "\n";
//    std::cerr << "max_age_to_lower_base: " << max_age_to_lower_base << "\n";

    /// Map age to 0..1
    double age_normalized = mapPiecewiseLinearToUnit(min_age, min_age_to_lower_base, max_age_to_lower_base);

//    std::cerr << "age: " << min_age << "\n";
//    std::cerr << "age_normalized: " << age_normalized << "\n";

    /// Map partition_size to 0..1
    double num_parts_normalized = mapPiecewiseLinearToUnit(partition_size, settings.min_parts_to_lower_base, settings.max_parts_to_lower_base);

//    std::cerr << "partition_size: " << partition_size << "\n";
//    std::cerr << "num_parts_normalized: " << num_parts_normalized << "\n";

    double combined_ratio = std::min(1.0, age_normalized + num_parts_normalized);

//    std::cerr << "combined_ratio: " << combined_ratio << "\n";

    double lowered_base = interpolateLinear(settings.base, 2.0, combined_ratio);

//    std::cerr << "------- lowered_base: " << lowered_base << "\n";

    return (sum_size + range_size * settings.size_fixed_cost_to_add) / (max_size + settings.size_fixed_cost_to_add) >= lowered_base;
}


void selectWithinPartition(
    const SimpleMergeSelector::PartsRange & parts,
    const size_t max_total_size_to_merge,
    Estimator & estimator,
    const SimpleMergeSelector::Settings & settings,
    double min_size_to_lower_base_log,
    double max_size_to_lower_base_log)
{
    size_t parts_count = parts.size();
    if (parts_count <= 1)
        return;

    for (size_t begin = 0; begin < parts_count; ++begin)
    {
        /// If too many parts, select only from first, to avoid complexity.
        if (begin > 1000)
            break;

        if (!parts[begin].shall_participate_in_merges)
            continue;

        size_t sum_size = parts[begin].size;
        size_t max_size = parts[begin].size;
        size_t min_age = parts[begin].age;

        for (size_t end = begin + 2; end <= parts_count; ++end)
        {
            assert(end > begin);
            if (settings.max_parts_to_merge_at_once && end - begin > settings.max_parts_to_merge_at_once) //-V658
                break;

            if (!parts[end - 1].shall_participate_in_merges)
                break;

            size_t cur_size = parts[end - 1].size;
            size_t cur_age = parts[end - 1].age;

            sum_size += cur_size;
            max_size = std::max(max_size, cur_size);
            min_age = std::min(min_age, cur_age);

            if (max_total_size_to_merge && sum_size > max_total_size_to_merge)
                break;

            if (allow(sum_size, max_size, min_age, end - begin, parts_count, min_size_to_lower_base_log, max_size_to_lower_base_log, settings))
                estimator.consider(
                    parts.begin() + begin,
                    parts.begin() + end,
                    sum_size,
                    begin == 0 ? 0 : parts[begin - 1].size,
                    settings);
        }
    }
}

}


SimpleMergeSelector::PartsRange SimpleMergeSelector::select(
    const PartsRanges & parts_ranges,
    size_t max_total_size_to_merge)
{
    Estimator estimator;

    /// Precompute logarithm of settings boundaries, because log function is quite expensive in terms of performance
    const double min_size_to_lower_base_log = log(1 + settings.min_size_to_lower_base);
    const double max_size_to_lower_base_log = log(1 + settings.max_size_to_lower_base);

    for (const auto & part_range : parts_ranges)
        selectWithinPartition(part_range, max_total_size_to_merge, estimator, settings, min_size_to_lower_base_log, max_size_to_lower_base_log);

    return estimator.getBest();
}

}
