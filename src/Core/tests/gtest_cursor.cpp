#include <gtest/gtest.h>

#include <Core/Streaming/ICursor.h>

using namespace DB;

GTEST_TEST(Cursor, Parsing)
{
    Map collapsed_tree = {
      Tuple{"shard-1.partition-1.block_number", 10},
      Tuple{"shard-1.partition-1.block_offset", 42},
    };

    CursorTree tree = CursorTree::fromMap(collapsed_tree);
    Map collapsed_tree_2 = tree.collapse();

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

GTEST_TEST(Cursor, Get)
{
    Map collapsed_tree = {
      Tuple{"shard-1.partition-1.block_number", 10},
      Tuple{"shard-1.partition-1.block_offset", 42},
    };

    CursorTree tree = CursorTree::fromMap(collapsed_tree);
    ASSERT_EQ(tree.getValue({"shard-1", "partition-1", "block_number"}), 10);
    ASSERT_EQ(tree.getValue({"shard-1", "partition-1", "block_offset"}), 42);

    CursorTree shard_tree = tree.getSubtree({"shard-1"});
    ASSERT_EQ(shard_tree.getValue({"partition-1", "block_number"}), 10);
    ASSERT_EQ(shard_tree.getValue({"partition-1", "block_offset"}), 42);

    CursorTree partition_tree = shard_tree.getSubtree({"partition-1"});
    ASSERT_EQ(partition_tree.getValue({"block_number"}), 10);
    ASSERT_EQ(partition_tree.getValue({"block_offset"}), 42);
}

GTEST_TEST(Cursor, SimpleOperations)
{
    Map collapsed_tree = {
      Tuple{"shard-1.partition-1.block_number", 10},
      Tuple{"shard-1.partition-1.block_offset", 42},
    };

    CursorTree tree = CursorTree::fromMap(collapsed_tree);
    ASSERT_EQ(tree.getValue({"shard-1", "partition-1", "block_number"}), 10);

    tree.updateTree({"shard-1", "partition-1", "block_number"}, 27);
    ASSERT_EQ(tree.getValue({"shard-1", "partition-1", "block_number"}), 27);

    Map collapsed_partition_tree = {
      Tuple{"block_number", 100},
      Tuple{"block_offset", 200},
    };

    CursorTree partition_tree = CursorTree::fromMap(collapsed_partition_tree);
    tree.updateTree({"shard-1", "partition-1"}, partition_tree);
    ASSERT_EQ(tree.getValue({"shard-1", "partition-1", "block_number"}), 100);
    ASSERT_EQ(tree.getValue({"shard-1", "partition-1", "block_offset"}), 200);
}
