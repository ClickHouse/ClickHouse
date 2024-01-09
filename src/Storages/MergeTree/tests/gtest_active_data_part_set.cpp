#include <Storages/MergeTree/ActiveDataPartSet.h>
#include <Storages/MergeTree/MergeTreeDataFormatVersion.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

auto make_part_info(const std::string & name)
{
    return MergeTreePartInfo::fromPartName(name, MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
}

}

TEST(MergeTree, ActiveDataPartSet)
{
    using enum ActiveDataPartSet::AddPartOutcome;

    ActiveDataPartSet parts(MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING);
    ASSERT_EQ(parts.tryAddPart(make_part_info("all_0_0_0"), nullptr), Added);
    ASSERT_EQ(parts.tryAddPart(make_part_info("all_0_0_0_1"), nullptr), Added);
    ASSERT_EQ(parts.tryAddPart(make_part_info("all_0_0_0_53"), nullptr), Added);
}
