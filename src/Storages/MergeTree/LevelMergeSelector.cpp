#include <Storages/MergeTree/LevelMergeSelector.h>

#include <cmath>


namespace DB
{

namespace
{

/** Estimates best set of parts to merge within passed alternatives.
  * It is selected simply: by minimal size.
  */
struct Estimator
{
    using Iterator = LevelMergeSelector::PartsRange::const_iterator;

    void consider(Iterator begin, Iterator end, size_t sum_size)
    {
        double current_score = sum_size;

        if (!min_score || current_score < min_score)
        {
            min_score = current_score;
            best_begin = begin;
            best_end = end;
        }
    }

    LevelMergeSelector::PartsRange getBest() const
    {
        return LevelMergeSelector::PartsRange(best_begin, best_end);
    }

    double min_score = 0;
    Iterator best_begin {};
    Iterator best_end {};
};


void selectWithinPartition(
    const LevelMergeSelector::PartsRange & parts,
    const size_t max_total_size_to_merge,
    Estimator & estimator,
    const LevelMergeSelector::Settings & settings)
{
    size_t parts_size = parts.size();
    if (parts_size < settings.parts_to_merge)
        return;

    /// To easily calculate sum size in any range.
    size_t parts_count = parts.size();
    size_t prefix_sum = 0;
    std::vector<size_t> prefix_sums(parts.size() + 1);

    for (size_t i = 0; i < parts_count; ++i)
    {
        prefix_sum += parts[i].size;
        prefix_sums[i + 1] = prefix_sum;
    }

    /// Use "corrected" level. It will be non-decreasing while traversing parts right to left.
    /// This is done for compatibility with another algorithms.
    size_t corrected_level_at_left = 0;
    size_t corrected_level_at_right = 0;

    size_t range_end = parts_size;
    size_t range_begin = range_end - settings.parts_to_merge;

    for (size_t i = range_begin; i < range_end; ++i)
        if (corrected_level_at_left < parts[i].level)
            corrected_level_at_left = parts[i].level;

    while (true)
    {
        if (corrected_level_at_left < parts[range_begin].level)
            corrected_level_at_left = parts[range_begin].level;

        if (corrected_level_at_right < parts[range_end - 1].level)
            corrected_level_at_right = parts[range_end - 1].level;

        /// Leftmost range of same corrected level.
        if (corrected_level_at_left == corrected_level_at_right
            && (range_begin == 0 || parts[range_begin - 1].level > corrected_level_at_left))
        {
            size_t range_size = prefix_sums[range_end] - prefix_sums[range_begin];

            if (range_size <= max_total_size_to_merge)
                estimator.consider(parts.begin() + range_begin, parts.begin() + range_end, range_size);

            break;    /// Minimum level is enough.
        }

        if (range_begin == 0)
            break;

        --range_begin;
        --range_end;
    }
}

}


LevelMergeSelector::PartsRange LevelMergeSelector::select(
    const PartsRanges & parts_ranges,
    const size_t max_total_size_to_merge)
{
    Estimator estimator;

    for (const auto & parts_range: parts_ranges)
        selectWithinPartition(parts_range, max_total_size_to_merge, estimator, settings);

    return estimator.getBest();
}

}
