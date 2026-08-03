#include <gtest/gtest.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/TTLMergeSelector.h>

using namespace DB;

TEST(SimpleMergeSelector, TestRowsConstraint)
{
    SimpleMergeSelector::Settings settings;
    settings.base = 2.0;
    SimpleMergeSelector selector(settings);
    std::vector<std::string> part_names = {"all_0_0_0", "all_1_1_0", "all_2_2_0"};
    PartsRange parts_range;

    for (const auto & part_name : part_names)
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


TEST(TTLIndexClearMergeSelector, IncompleteTTLMetadataIsImmediatelyDue)
{
    const time_t current_time = 100;
    TTLIndexClearMergeSelector selector(current_time);

    PartProperties part{
        .name = "all_0_0_0",
        .info = MergeTreePartInfo::fromPartName("all_0_0_0", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING),
        .all_ttl_calculated_if_any = false,
        .size = 100,
        .rows = 100,
        .next_index_clear_ttl = 0,
        .can_preserve_files_for_index_clear = false,
    };

    std::vector<MergeConstraint> constraints{{1000, 1000}};
    auto selected = selector.select({PartsRange{part}}, constraints, nullptr);

    ASSERT_EQ(selected.size(), 1);
    ASSERT_EQ(selected.front().size(), 1);
    EXPECT_EQ(selected.front().front().name, part.name);
}

TEST(TTLIndexClearMergeSelector, TestRowsConstraint)
{
    const time_t current_time = 100;
    TTLIndexClearMergeSelector selector(current_time);

    auto make_part = [&](bool can_preserve_files_for_index_clear)
    {
        return PartProperties
        {
            .name = "all_0_0_0",
            .info = MergeTreePartInfo::fromPartName("all_0_0_0", MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING),
            .size = 100,
            .rows = 1000,
            .next_index_clear_ttl = current_time,
            .can_preserve_files_for_index_clear = can_preserve_files_for_index_clear,
        };
    };

    std::vector<MergeConstraint> constraints{{1000, 100}};

    for (const bool can_preserve_files_for_index_clear : {false, true})
    {
        auto selected = selector.select({PartsRange{make_part(can_preserve_files_for_index_clear)}}, constraints, nullptr);
        ASSERT_TRUE(selected.empty());
    }
}
