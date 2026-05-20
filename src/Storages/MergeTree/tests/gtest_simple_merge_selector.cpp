#include <gtest/gtest.h>
#include <Storages/MergeTree/Compaction/MergeSelectors/SimpleMergeSelector.h>

#include <string>
#include <vector>

using namespace DB;

namespace
{

constexpr size_t MiB = 1024 * 1024;

PartsRange makePartsRange(const std::vector<size_t> & sizes, const std::vector<time_t> & ages)
{
    EXPECT_EQ(sizes.size(), ages.size());
    if (sizes.size() != ages.size())
        return {};
    PartsRange parts_range;
    for (size_t i = 0; i < sizes.size(); ++i)
    {
        String part_name = "all_" + std::to_string(i) + "_" + std::to_string(i) + "_0";
        parts_range.push_back(PartProperties
        {
            .name = part_name,
            .info = MergeTreePartInfo::fromPartName(part_name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING),
            .size = sizes[i],
            .age = ages[i],
            .rows = 100,
        });
    }

    return parts_range;
}

std::vector<String> getPartNames(const PartsRange & parts)
{
    std::vector<String> names;
    names.reserve(parts.size());

    for (const auto & part : parts)
        names.push_back(part.name);

    return names;
}

PartsRanges selectRightTailRange(SimpleMergeSelector::Settings settings, const std::vector<time_t> & ages)
{
    settings.base = 2.0;
    settings.enable_heuristic_to_align_parts = false;

    SimpleMergeSelector selector(settings);
    auto parts_range = makePartsRange({10 * MiB, 10 * MiB, 1024}, ages);
    std::vector<MergeConstraint> constraints{{100 * MiB, 1000}};

    return selector.select({parts_range}, constraints, nullptr);
}

}

TEST(SimpleMergeSelector, TestRowsConstraint)
{
    SimpleMergeSelector::Settings settings;
    settings.base = 2.0;
    SimpleMergeSelector selector(settings);
    auto parts_range = makePartsRange({10 * 1024, 10 * 1024, 10 * 1024}, {0, 0, 0});

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

TEST(SimpleMergeSelector, RemovesSmallPartsAtRightByDefault)
{
    auto selected = selectRightTailRange(SimpleMergeSelector::Settings{}, {100, 100, 100});

    ASSERT_EQ(selected.size(), 1);
    EXPECT_EQ(getPartNames(selected[0]), std::vector<String>({"all_0_0_0", "all_1_1_0"}));
}

TEST(SimpleMergeSelector, DoesNotRemoveSmallPartsAtRightWhenAllPartsAreOldEnough)
{
    SimpleMergeSelector::Settings settings;
    settings.merge_selector_min_age_to_disable_right_tail_heuristic = 60;

    auto selected = selectRightTailRange(settings, {100, 100, 100});

    ASSERT_EQ(selected.size(), 1);
    EXPECT_EQ(getPartNames(selected[0]), std::vector<String>({"all_0_0_0", "all_1_1_0", "all_2_2_0"}));
}

TEST(SimpleMergeSelector, RemovesSmallPartsAtRightWhenAllPartsAreTooYoung)
{
    SimpleMergeSelector::Settings settings;
    settings.merge_selector_min_age_to_disable_right_tail_heuristic = 60;

    auto selected = selectRightTailRange(settings, {10, 10, 10});

    ASSERT_EQ(selected.size(), 1);
    EXPECT_EQ(getPartNames(selected[0]), std::vector<String>({"all_0_0_0", "all_1_1_0"}));
}

TEST(SimpleMergeSelector, RemovesSmallPartsAtRightWhenSomePartsAreTooYoung)
{
    SimpleMergeSelector::Settings settings;
    settings.merge_selector_min_age_to_disable_right_tail_heuristic = 60;

    auto selected = selectRightTailRange(settings, {100, 10, 100});

    ASSERT_EQ(selected.size(), 1);
    EXPECT_EQ(getPartNames(selected[0]), std::vector<String>({"all_0_0_0", "all_1_1_0"}));
}

TEST(SimpleMergeSelector, DoesNotRemoveSmallPartsAtRightAtAgeThreshold)
{
    SimpleMergeSelector::Settings settings;
    settings.merge_selector_min_age_to_disable_right_tail_heuristic = 60;

    auto selected = selectRightTailRange(settings, {60, 60, 60});

    ASSERT_EQ(selected.size(), 1);
    EXPECT_EQ(getPartNames(selected[0]), std::vector<String>({"all_0_0_0", "all_1_1_0", "all_2_2_0"}));
}

TEST(SimpleMergeSelector, DoesNotRemoveSmallPartsAtRightWhenHeuristicDisabled)
{
    SimpleMergeSelector::Settings settings;
    settings.enable_heuristic_to_remove_small_parts_at_right = false;
    settings.merge_selector_min_age_to_disable_right_tail_heuristic = 60;

    auto selected = selectRightTailRange(settings, {10, 10, 10});

    ASSERT_EQ(selected.size(), 1);
    EXPECT_EQ(getPartNames(selected[0]), std::vector<String>({"all_0_0_0", "all_1_1_0", "all_2_2_0"}));
}
