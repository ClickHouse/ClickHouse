#include <Core/Streaming/CursorTree.h>

#include <gtest/gtest.h>

using namespace DB;

TEST(CursorTree, BuildFromFlatMap)
{
    Map flat;
    flat.push_back(Tuple{"all.block_number", Int64(10)});
    flat.push_back(Tuple{"all.block_offset", Int64(20)});

    auto root = buildCursorTree(flat);

    ASSERT_TRUE(root->hasSubtree("all"));
    const auto & all = root->getSubtree("all");
    ASSERT_TRUE(all->hasValue("block_number"));
    ASSERT_TRUE(all->hasValue("block_offset"));
    ASSERT_EQ(all->getValue("block_number"), 10);
    ASSERT_EQ(all->getValue("block_offset"), 20);
}

TEST(CursorTree, RoundTripThroughMap)
{
    Map flat;
    flat.push_back(Tuple{"partition_a.block_number", Int64(5)});
    flat.push_back(Tuple{"partition_b.block_number", Int64(7)});

    auto root = buildCursorTree(flat);
    auto flat2 = cursorTreeToMap(root);

    ASSERT_EQ(flat2.size(), flat.size());
}

TEST(CursorTree, ThrowsOnMissingKey)
{
    auto root = std::make_shared<CursorTreeNode>();
    ASSERT_ANY_THROW((void)root->getValue("missing"));
    ASSERT_ANY_THROW((void)root->getSubtree("missing"));
}

TEST(CursorTree, EmptyMapProducesEmptyTree)
{
    auto root = buildCursorTree(Map{});

    /// No children, no throw on iteration.
    size_t count = 0;
    for (auto it = root->begin(); it != root->end(); ++it)
        ++count;
    ASSERT_EQ(count, 0u);
}
