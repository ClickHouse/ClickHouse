#include <Core/Streaming/CursorTree.h>

#include <Common/Exception.h>

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

TEST(CursorTree, BoundsTheDepthOfADottedKey)
{
    auto dotted_key = [](size_t components)
    {
        String key;
        for (size_t i = 0; i + 1 < components; ++i)
            key += "a.";
        return key + "z";
    };

    auto depth_of = [](const CursorTreeNode * node)
    {
        size_t depth = 1;
        for (; node->hasSubtree("a"); ++depth)
            node = node->getSubtree("a").get();
        return depth;
    };

    Map at_limit;
    at_limit.push_back(Tuple{dotted_key(MAX_CURSOR_TREE_DEPTH), Int64(10)});
    auto root = buildCursorTree(at_limit);

    ASSERT_EQ(depth_of(root.get()), MAX_CURSOR_TREE_DEPTH);

    /// At the limit every recursive consumer must still complete, not only the construction above.
    ASSERT_EQ(cursorTreeToMap(root).size(), 1u);

    auto cloned = root->clone();
    ASSERT_EQ(depth_of(cloned.get()), MAX_CURSOR_TREE_DEPTH);

    auto other = buildCursorTree(at_limit);
    mergeCursors(other, root);
    ASSERT_EQ(depth_of(other.get()), MAX_CURSOR_TREE_DEPTH);

    Map over_limit;
    over_limit.push_back(Tuple{dotted_key(MAX_CURSOR_TREE_DEPTH + 1), Int64(10)});
    ASSERT_THROW((void)buildCursorTree(over_limit), Exception);
}
