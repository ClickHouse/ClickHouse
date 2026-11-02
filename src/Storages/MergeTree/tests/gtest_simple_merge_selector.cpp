#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>

#include <gtest/gtest.h>

#include <algorithm>
#include <ranges>

using namespace DB;

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
