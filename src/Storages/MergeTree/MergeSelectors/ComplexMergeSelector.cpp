#include <Storages/MergeTree/MergeSelectors/ComplexMergeSelector.h>
#include <Storages/MergeTree/MergeSelectors/MergeSelectorFactory.h>
#include <Core/MergeSelectorAlgorithm.h>

#include <base/interpolate.h>
#include <Common/thread_local_rng.h>

#include <cmath>
#include <cassert>


namespace DB
{

void registerComplexMergeSelector(MergeSelectorFactory & factory)
{
    factory.registerPublicSelector("Complex", MergeSelectorAlgorithm::COMPLEX, [](const std::any & settings)
    {
        return std::make_shared<ComplexMergeSelector>(std::any_cast<ComplexMergeSelector::Settings>(settings));
    });
}

namespace
{

/** Estimates best set of parts to merge within passed alternatives.
  */
struct Estimator
{
    using Iterator = ComplexMergeSelector::PartsRange::const_iterator;

    void consider(Iterator begin, Iterator end, size_t sum_size, double sum_size_log_size, const ComplexMergeSelector::Settings &)
    {
        double current_score = score(sum_size, sum_size_log_size);

        if (max_score == 0.0 || current_score > max_score)
        {
            max_score = current_score;
            best_begin = begin;
            best_end = end;
        }
    }

    ComplexMergeSelector::PartsRange getBest() const
    {
        return ComplexMergeSelector::PartsRange(best_begin, best_end);
    }

    static double score(double sum_size, double sum_size_log_size)
    {
        // TODO(serxa): check what is faster log or log2
        // TODO(serxa): add comment explaining why entropy maximization is the goal
        return log2(sum_size) - sum_size_log_size / sum_size;
    }

    double max_score = 0.0;
    Iterator best_begin {};
    Iterator best_end {};
};

void selectWithinPartition(
    const ComplexMergeSelector::PartsRange & parts,
    const size_t max_total_size_to_merge,
    Estimator & estimator,
    const ComplexMergeSelector::Settings & settings)
{
    size_t parts_count = parts.size();
    if (parts_count <= 1)
        return;

    for (size_t begin = 0; begin < parts_count; ++begin)
    {
        if (!parts[begin].shall_participate_in_merges)
            continue;

        // TODO(serxa): optimize out unnecessary log2() calls
        size_t sum_size = parts[begin].size;
        double sum_size_log_size = parts[begin].size * log2(parts[begin].size);
        size_t max_size = parts[begin].size;
        size_t min_age = parts[begin].age;

        for (size_t end = begin + 2; end <= parts_count; ++end)
        {
            assert(end > begin);
            if (settings.max_parts_to_merge_at_once && end - begin > settings.max_parts_to_merge_at_once)
                break;

            if (!parts[end - 1].shall_participate_in_merges)
                break;

            size_t cur_size = parts[end - 1].size;
            size_t cur_age = parts[end - 1].age;

            sum_size += cur_size;
            sum_size_log_size += cur_size * log2(cur_size);
            max_size = std::max(max_size, cur_size);
            min_age = std::min(min_age, cur_age);

            if (max_total_size_to_merge && sum_size > max_total_size_to_merge)
                break;

            // TODO(serxa): add constraint to control write amplification
            estimator.consider(
                parts.begin() + begin,
                parts.begin() + end,
                sum_size,
                sum_size_log_size,
                settings);
        }
    }
}

}

ComplexMergeSelector::PartsRange ComplexMergeSelector::select(
    const PartsRanges & parts_ranges,
    size_t max_total_size_to_merge)
{
    Estimator estimator;

    for (const auto & part_range : parts_ranges)
        selectWithinPartition(part_range, max_total_size_to_merge, estimator, settings);

    return estimator.getBest();
}

}
