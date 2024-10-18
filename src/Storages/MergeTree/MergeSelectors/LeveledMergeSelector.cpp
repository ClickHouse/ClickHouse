#include <Storages/MergeTree/MergeSelectors/LeveledMergeSelector.h>
#include <Storages/MergeTree/MergeSelectors/MergeSelectorFactory.h>
#include <Core/MergeSelectorAlgorithm.h>

#include <base/interpolate.h>
#include <Common/thread_local_rng.h>

#include <cmath>
#include <cassert>
#include <iostream>
#include <random>
#include <numeric>
#include <optional>


namespace DB
{

void registerLeveledMergeSelector(MergeSelectorFactory & factory)
{
    factory.registerPublicSelector("Leveled", MergeSelectorAlgorithm::LEVELED, [](const std::any & settings)
    {
        return std::make_shared<LeveledMergeSelector>(std::any_cast<LeveledMergeSelector::Settings>(settings));
    });
}

namespace
{

struct WindowDescription
{
    IMergeSelector::PartsRange range;
    size_t end_index = 0;
    size_t sum = 0;
    double deviation = 0;
};

bool compareByDeviation(const WindowDescription & lhs, const WindowDescription & rhs)
{
    return lhs.deviation < rhs.deviation;
}

std::optional<WindowDescription> selectWithinPartition(
    const LeveledMergeSelector::PartsRange & parts,
    const size_t max_total_size_to_merge,
    const LeveledMergeSelector::Settings & settings)
{
    size_t parts_count = parts.size();
    if (parts_count <= settings.max_parts_to_merge_at_once)
        return std::nullopt;

    std::vector<WindowDescription> windows;

    for (size_t window_size = 2; window_size <= settings.max_parts_to_merge_at_once; window_size += settings.batch_size_step)
    {
        std::vector<WindowDescription> level;
        level.reserve(parts_count);

        size_t sum_in_window = 0;
        std::for_each(parts.begin(), parts.begin() + window_size, [&](auto elem) { sum_in_window += elem.size; });

        for (size_t i = window_size; i < parts_count; ++i)
        {
            if (sum_in_window > max_total_size_to_merge)
            {
                sum_in_window += parts[i].size - parts[i - window_size].size;
                continue;
            }

            double deviation = 0;
            for (size_t j = i - window_size; j < i; ++j)
                deviation += std::abs(parts[j].size - static_cast<double>(sum_in_window) / parts_count);
            deviation /= std::log(std::log(parts_count));

            auto window = WindowDescription
            {
                .range = {parts.begin() + i - window_size, parts.begin() + i},
                .end_index = i,
                .sum = sum_in_window,
                .deviation = deviation
            };
            level.push_back(std::move(window));
        }

        std::partial_sort(
            level.begin(),
            std::min(level.begin() + settings.windows_to_look_at_single_level, level.end()),
            level.end(),
            [](const auto & lhs, const auto & rhs){ return lhs.sum < rhs.sum; }
        );

        std::move
        (
            level.begin(),
            std::min(level.begin() + settings.windows_to_look_at_single_level, level.end()),
            std::back_inserter(windows)
        );
    }

    if (windows.empty())
        return std::nullopt;

    return *std::min_element(windows.begin(), windows.end(), compareByDeviation);
}

}


LeveledMergeSelector::PartsRange LeveledMergeSelector::select(
    const PartsRanges & parts_ranges,
    size_t max_total_size_to_merge)
{
    std::vector<WindowDescription> partitions;
    for (const auto & part_range : parts_ranges)
        if (auto res = selectWithinPartition(part_range, max_total_size_to_merge, settings); res.has_value())
            partitions.push_back(res.value());

    if (partitions.empty())
        return LeveledMergeSelector::PartsRange{};

    auto best = *std::min_element(partitions.begin(), partitions.end(), compareByDeviation);

    std::cout << "Best " << best.deviation << best.sum << std::endl;
    return best.range;
}

}
