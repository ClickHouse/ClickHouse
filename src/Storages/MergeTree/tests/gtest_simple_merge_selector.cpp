#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <numeric>
#include <ranges>
#include <string>
#include <vector>

using namespace DB;

namespace
{

constexpr size_t MiB = 1024 * 1024;

PartsRange makePartsRange(const std::vector<size_t> & sizes, time_t age)
{
    PartsRange parts_range;
    for (size_t i = 0; i < sizes.size(); ++i)
    {
        std::string part_name = "all_" + std::to_string(i) + "_" + std::to_string(i) + "_0";
        parts_range.push_back(PartProperties
        {
            .name = part_name,
            .info = MergeTreePartInfo::fromPartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING),
            .size = sizes[i],
            .age = age,
            .rows = 100,
        });
    }

    return parts_range;
}

PartitionsStatistics makeStatistics(const PartsRange & parts_range, time_t partition_min_age)
{
    PartitionsStatistics statistics;
    statistics["all"] = PartitionStatistics{
        .min_age = partition_min_age,
        .part_count = parts_range.size(),
        .total_size = std::accumulate(parts_range.begin(), parts_range.end(), size_t(0), [](size_t sum, const auto & part) { return sum + part.size; }),
    };

    return statistics;
}

}

TEST(SimpleMergeSelector, TestRowsConstraint)
{
    PartsRange parts_range;
    for (const auto & part_name : {"all_0_0_0", "all_1_1_0", "all_2_2_0"})
    {
        parts_range.push_back(PartProperties
        {
            .name = part_name,
            .info = MergeTreePartInfo::fromPartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING),
            .size = 10 * 1024,
            .age = 0,
            .rows = 100,
        });
    }

    PartitionsStatistics statistics;
    statistics["all"] = PartitionStatistics{
        .min_age = std::ranges::min(parts_range | std::views::transform(&PartProperties::age)),
        .part_count = parts_range.size(),
        .total_size = 10 * 1024 * 3,
    };

    SimpleMergeSelector::Settings settings;
    settings.base = 2.0;
    settings.partitions_stats = &statistics;

    SimpleMergeSelector selector(settings);
    size_t max_bytes = 100 * 1024 * 1024;

    {
        size_t max_rows = 1000;
        std::vector<MergeConstraint> constraints{{max_bytes, max_rows}};
        auto selected = selector.select({parts_range}, constraints, nullptr);

        ASSERT_EQ(selected.size(), 1);
        ASSERT_EQ(selected[0].size(), 3);
    }

    {
        size_t max_rows = 250;
        std::vector<MergeConstraint> constraints{{max_bytes, max_rows}};
        auto selected = selector.select({parts_range}, constraints, nullptr);

        ASSERT_EQ(selected.size(), 1);
        ASSERT_EQ(selected[0].size(), 2);
    }

    {
        size_t max_rows = 50;
        std::vector<MergeConstraint> constraints{{max_bytes, max_rows}};
        auto selected = selector.select({parts_range}, constraints, nullptr);

        ASSERT_EQ(selected.size(), 0);
    }
}

TEST(SimpleMergeSelector, ForceMergeByPartitionAge)
{
    /// A large part with a small one after it is not merged by the base heuristic.
    auto parts_range = makePartsRange({10 * MiB, 1024}, /*age=*/7200);
    auto statistics = makeStatistics(parts_range, /*partition_min_age=*/7200);

    std::vector<MergeConstraint> constraints{{100 * MiB, 1000}};

    {
        SimpleMergeSelector::Settings settings;
        settings.partitions_stats = &statistics;

        auto selected = SimpleMergeSelector(settings).select({parts_range}, constraints, nullptr);
        ASSERT_EQ(selected.size(), 0);
    }

    {
        SimpleMergeSelector::Settings settings;
        settings.partitions_stats = &statistics;
        settings.min_partition_age_to_force_merge = 3600;

        auto selected = SimpleMergeSelector(settings).select({parts_range}, constraints, nullptr);
        ASSERT_EQ(selected.size(), 1);
        ASSERT_EQ(selected[0].size(), 2);
    }
}

TEST(SimpleMergeSelector, NoForceMergeWhilePartitionReceivesInserts)
{
    auto parts_range = makePartsRange({10 * MiB, 1024}, /*age=*/7200);
    /// The youngest part of the partition (in another range) is too young.
    auto statistics = makeStatistics(parts_range, /*partition_min_age=*/5);

    SimpleMergeSelector::Settings settings;
    settings.partitions_stats = &statistics;
    settings.min_partition_age_to_force_merge = 3600;

    std::vector<MergeConstraint> constraints{{100 * MiB, 1000}};
    auto selected = SimpleMergeSelector(settings).select({parts_range}, constraints, nullptr);

    ASSERT_EQ(selected.size(), 0);
}

TEST(SimpleMergeSelector, ForceMergeByPartitionAgeKeepsRightTail)
{
    auto parts_range = makePartsRange({10 * MiB, 10 * MiB, 1024}, /*age=*/7200);
    auto statistics = makeStatistics(parts_range, /*partition_min_age=*/7200);

    std::vector<MergeConstraint> constraints{{100 * MiB, 1000}};

    {
        SimpleMergeSelector::Settings settings;
        settings.partitions_stats = &statistics;

        /// The right-tail heuristic drops the trailing small part.
        auto selected = SimpleMergeSelector(settings).select({parts_range}, constraints, nullptr);
        ASSERT_EQ(selected.size(), 1);
        ASSERT_EQ(selected[0].size(), 2);
    }

    {
        SimpleMergeSelector::Settings settings;
        settings.partitions_stats = &statistics;
        settings.min_partition_age_to_force_merge = 3600;

        auto selected = SimpleMergeSelector(settings).select({parts_range}, constraints, nullptr);
        ASSERT_EQ(selected.size(), 1);
        ASSERT_EQ(selected[0].size(), 3);
    }
}
