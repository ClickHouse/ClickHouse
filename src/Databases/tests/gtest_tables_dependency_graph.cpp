#include <Databases/TablesDependencyGraph.h>
#include <Interpreters/StorageID.h>

#include <gtest/gtest.h>

using namespace DB;

namespace
{

size_t countTables(const std::vector<std::vector<StorageID>> & split)
{
    size_t total = 0;
    for (const auto & level : split)
        total += level.size();
    return total;
}

}

/// Regression test for https://github.com/ClickHouse/ClickHouse/issues/103746
///
/// Cyclic nodes get level == CYCLIC_LEVEL (std::numeric_limits<size_t>::max()).
/// The old getTablesSplitByDependencyLevel did resize(sorted_nodes.back()->level + 1),
/// which for a cyclic graph is resize(SIZE_MAX + 1) == resize(0), followed by an
/// out-of-bounds write tables_split_by_level[CYCLIC_LEVEL].emplace_back(...).
/// This aborted the server during RESTORE of backups with cyclic dependencies.
TEST(TablesDependencyGraph, SplitByDependencyLevelPureCycle)
{
    TablesDependencyGraph graph("test");

    StorageID a{"db", "a"};
    StorageID b{"db", "b"};

    /// A depends on B and B depends on A - a pure cycle.
    graph.addDependency(a, b);
    graph.addDependency(b, a);

    ASSERT_TRUE(graph.hasCyclicDependencies());

    /// Must not abort / write out of bounds.
    auto split = graph.getTablesSplitByDependencyLevel();

    /// Both cyclic tables must be present, in the trailing bucket.
    EXPECT_EQ(countTables(split), 2u);
    ASSERT_FALSE(split.empty());
    EXPECT_EQ(split.back().size(), 2u);
}

/// A mix of acyclic tables and a cycle: acyclic ordering by level must be
/// preserved, while cyclic tables are placed into the trailing bucket.
TEST(TablesDependencyGraph, SplitByDependencyLevelMixedCycle)
{
    TablesDependencyGraph graph("test");

    StorageID root{"db", "root"};   /// no dependencies -> level 0
    StorageID leaf{"db", "leaf"};   /// depends on root -> level 1
    StorageID c1{"db", "c1"};
    StorageID c2{"db", "c2"};

    graph.addDependency(leaf, root);

    /// A separate cycle c1 <-> c2.
    graph.addDependency(c1, c2);
    graph.addDependency(c2, c1);

    ASSERT_TRUE(graph.hasCyclicDependencies());

    auto split = graph.getTablesSplitByDependencyLevel();

    /// All four tables must be present.
    EXPECT_EQ(countTables(split), 4u);

    /// Levels 0 and 1 hold the acyclic tables; the trailing bucket holds the cycle.
    ASSERT_GE(split.size(), 3u);
    EXPECT_EQ(split[0].size(), 1u);
    EXPECT_EQ(split[0][0].getTableName(), "root");
    EXPECT_EQ(split[1].size(), 1u);
    EXPECT_EQ(split[1][0].getTableName(), "leaf");
    EXPECT_EQ(split.back().size(), 2u);
}
