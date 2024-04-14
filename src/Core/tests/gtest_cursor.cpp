#include <gtest/gtest.h>

#include <Core/Streaming/CursorTree.h>

using namespace DB;

GTEST_TEST(Cursor, ParsingMap)
{
    Map collapsed_tree = {
        Tuple{"shard-1.partition-1.block_number", 10},
        Tuple{"shard-1.partition-1.block_offset", 42},
    };

    CursorTreeNodePtr tree = buildCursorTree(collapsed_tree);
    Map collapsed_tree_2 = cursorTreeToMap(tree);

    ASSERT_EQ(collapsed_tree.size(), collapsed_tree_2.size());

    std::map<std::string, uint64_t> ct1, ct2;

    for (const auto & leaf : collapsed_tree)
    {
        const auto & tuple = leaf.safeGet<const Tuple &>();
        const auto & dotted_path = tuple.at(0).safeGet<String>();
        const auto & value = tuple.at(1).get<UInt64>();
        ct1[dotted_path] = value;
    }

    for (const auto & leaf : collapsed_tree_2)
    {
        const auto & tuple = leaf.safeGet<const Tuple &>();
        const auto & dotted_path = tuple.at(0).safeGet<String>();
        const auto & value = tuple.at(1).get<UInt64>();
        ct2[dotted_path] = value;
    }

    ASSERT_EQ(ct1, ct2);
}

GTEST_TEST(Cursor, ParsingString)
{
    Map collapsed_tree = {
        Tuple{"block_number", 10},
        Tuple{"block_offset", 42},
    };

    CursorTreeNodePtr tree = buildCursorTree(collapsed_tree);

    String serialized_tree = cursorTreeToString(tree);
    CursorTreeNodePtr tree_2 = buildCursorTree(serialized_tree);

    for (const auto & [k, v] : *tree)
        ASSERT_EQ(std::get<Int64>(v), tree_2->getValue(k));
}

GTEST_TEST(Cursor, Get)
{
    Map collapsed_tree = {
        Tuple{"shard-1.partition-1.block_number", 10},
        Tuple{"shard-1.partition-1.block_offset", 42},
    };

    CursorTreeNodePtr tree = buildCursorTree(collapsed_tree);
    CursorTreeNodePtr shard_tree = tree->getSubtree("shard-1");
    CursorTreeNodePtr partition_tree = shard_tree->getSubtree("partition-1");

    ASSERT_EQ(partition_tree->getValue("block_number"), 10);
    ASSERT_EQ(partition_tree->getValue("block_offset"), 42);
}

GTEST_TEST(Cursor, SimpleOperations)
{
    Map collapsed_tree = {
        Tuple{"part-1.block_number", 10},
        Tuple{"part-1.block_offset", 42},
        Tuple{"part-2.block_number", 110},
        Tuple{"part-2.block_offset", 142},
    };

    CursorTreeNodePtr tree = buildCursorTree(collapsed_tree);
    ASSERT_EQ(tree->getSubtree("part-1")->getValue("block_number"), 10);
    ASSERT_EQ(tree->getSubtree("part-1")->getValue("block_offset"), 42);
    ASSERT_EQ(tree->getSubtree("part-2")->getValue("block_number"), 110);
    ASSERT_EQ(tree->getSubtree("part-2")->getValue("block_offset"), 142);

    tree->getSubtree("part-1")->setValue("block_number", 27);
    ASSERT_EQ(tree->getSubtree("part-1")->getValue("block_number"), 27);

    Map collapsed_partition_tree = {
        Tuple{"block_number", 100},
        Tuple{"block_offset", 200},
    };

    CursorTreeNodePtr partition_tree = buildCursorTree(collapsed_partition_tree);
    tree->setSubtree("part-1", partition_tree);
    ASSERT_EQ(tree->getSubtree("part-1")->getValue("block_number"), 100);
    ASSERT_EQ(tree->getSubtree("part-1")->getValue("block_offset"), 200);
    ASSERT_EQ(tree->getSubtree("part-2")->getValue("block_number"), 110);
    ASSERT_EQ(tree->getSubtree("part-2")->getValue("block_offset"), 142);
}
