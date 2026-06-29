#include <gtest/gtest.h>
#include <Common/Exception.h>
#include <Coordination/CompactChildrenSet.h>
#include <string_view>

using namespace DB;

/// We need stable string data that outlives insert/erase operations,
/// similar to how SnapshotableHashTable's GlobalArena works in production.
static constexpr std::string_view CHILD_A = "child_a";
static constexpr std::string_view CHILD_B = "child_b";
static constexpr std::string_view CHILD_C = "child_c";

TEST(CompactChildrenSet, EmptyState)
{
    CompactChildrenSet cs;
    EXPECT_TRUE(cs.empty());
    EXPECT_EQ(cs.size(), 0);
    EXPECT_FALSE(cs.contains(CHILD_A));
    EXPECT_EQ(cs.begin(), cs.end());
    EXPECT_EQ(cs.heapSizeInBytes(), 0);
}

TEST(CompactChildrenSet, EmptyToSingleTransition)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    EXPECT_FALSE(cs.empty());
    EXPECT_EQ(cs.size(), 1);
    EXPECT_TRUE(cs.contains(CHILD_A));
    EXPECT_FALSE(cs.contains(CHILD_B));
    EXPECT_EQ(cs.heapSizeInBytes(), 0);

    /// Iterator should yield exactly one element
    auto it = cs.begin();
    EXPECT_NE(it, cs.end());
    EXPECT_EQ(*it, CHILD_A);
    ++it;
    EXPECT_EQ(it, cs.end());
}

TEST(CompactChildrenSet, SingleToSetPromotion)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_B);
    EXPECT_FALSE(cs.empty());
    EXPECT_EQ(cs.size(), 2);
    EXPECT_TRUE(cs.contains(CHILD_A));
    EXPECT_TRUE(cs.contains(CHILD_B));
    EXPECT_GT(cs.heapSizeInBytes(), 0);
}

TEST(CompactChildrenSet, DuplicateInsertInSingleMode)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_A);
    EXPECT_EQ(cs.size(), 1);
    EXPECT_EQ(cs.heapSizeInBytes(), 0);
}

TEST(CompactChildrenSet, DuplicateInsertInSetMode)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_B);
    cs.insert(CHILD_A);
    EXPECT_EQ(cs.size(), 2);
}

TEST(CompactChildrenSet, SetToSingleDemotion)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_B);
    EXPECT_EQ(cs.size(), 2);

    cs.erase(CHILD_A);
    EXPECT_EQ(cs.size(), 1);
    EXPECT_FALSE(cs.contains(CHILD_A));
    EXPECT_TRUE(cs.contains(CHILD_B));
    /// After demotion, heap allocation is freed
    EXPECT_EQ(cs.heapSizeInBytes(), 0);
}

TEST(CompactChildrenSet, SetToEmptyDemotion)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_B);

    cs.erase(CHILD_A);
    cs.erase(CHILD_B);
    EXPECT_TRUE(cs.empty());
    EXPECT_EQ(cs.size(), 0);
    EXPECT_EQ(cs.begin(), cs.end());
}

TEST(CompactChildrenSet, SingleToEmpty)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.erase(CHILD_A);
    EXPECT_TRUE(cs.empty());
    EXPECT_EQ(cs.size(), 0);
}

TEST(CompactChildrenSet, EraseNonExistent)
{
    CompactChildrenSet cs;
    /// Erase on empty — no-op
    cs.erase(CHILD_A);
    EXPECT_TRUE(cs.empty());

    /// Erase non-matching in single mode — no-op
    cs.insert(CHILD_A);
    cs.erase(CHILD_B);
    EXPECT_EQ(cs.size(), 1);
    EXPECT_TRUE(cs.contains(CHILD_A));

    /// Erase non-matching in set mode — no-op
    cs.insert(CHILD_B);
    cs.erase(CHILD_C);
    EXPECT_EQ(cs.size(), 2);
}

TEST(CompactChildrenSet, FullCycle)
{
    /// Empty → single → set → single → empty
    CompactChildrenSet cs;
    EXPECT_TRUE(cs.empty());

    cs.insert(CHILD_A);
    EXPECT_EQ(cs.size(), 1);

    cs.insert(CHILD_B);
    EXPECT_EQ(cs.size(), 2);

    cs.erase(CHILD_A);
    EXPECT_EQ(cs.size(), 1);
    EXPECT_TRUE(cs.contains(CHILD_B));

    cs.erase(CHILD_B);
    EXPECT_TRUE(cs.empty());
}

TEST(CompactChildrenSet, ReserveOnEmpty)
{
    CompactChildrenSet cs;
    cs.reserve(10);
    /// Must still be logically empty after reserve
    EXPECT_TRUE(cs.empty());
    EXPECT_EQ(cs.size(), 0);
    EXPECT_EQ(cs.begin(), cs.end());

    /// Subsequent insert should work
    cs.insert(CHILD_A);
    EXPECT_EQ(cs.size(), 1);
    EXPECT_TRUE(cs.contains(CHILD_A));
}

TEST(CompactChildrenSet, ReserveOnSingle)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.reserve(10);
    /// Should preserve the existing child
    EXPECT_EQ(cs.size(), 1);
    EXPECT_TRUE(cs.contains(CHILD_A));
}

TEST(CompactChildrenSet, ReserveSmallIsNoOp)
{
    CompactChildrenSet cs;
    cs.reserve(0);
    EXPECT_TRUE(cs.empty());
    EXPECT_EQ(cs.heapSizeInBytes(), 0);

    cs.reserve(1);
    EXPECT_TRUE(cs.empty());
    EXPECT_EQ(cs.heapSizeInBytes(), 0);
}

TEST(CompactChildrenSet, Clear)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_B);
    cs.clear();
    EXPECT_TRUE(cs.empty());
    EXPECT_EQ(cs.size(), 0);

    /// Clear on empty — no-op
    cs.clear();
    EXPECT_TRUE(cs.empty());
}

TEST(CompactChildrenSet, CopyFromEmpty)
{
    CompactChildrenSet cs;
    const CompactChildrenSet copy(cs); // NOLINT(performance-unnecessary-copy-initialization) - intentionally testing copy constructor
    EXPECT_TRUE(copy.empty());
}

TEST(CompactChildrenSet, CopyFromSingle)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);

    const CompactChildrenSet copy(cs);
    EXPECT_EQ(copy.size(), 1);
    EXPECT_TRUE(copy.contains(CHILD_A));

    /// Original unchanged
    EXPECT_EQ(cs.size(), 1);
}

TEST(CompactChildrenSet, CopyFromSet)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_B);

    CompactChildrenSet copy(cs);
    EXPECT_EQ(copy.size(), 2);
    EXPECT_TRUE(copy.contains(CHILD_A));
    EXPECT_TRUE(copy.contains(CHILD_B));

    /// Mutating copy doesn't affect original
    copy.erase(CHILD_A);
    EXPECT_EQ(copy.size(), 1);
    EXPECT_EQ(cs.size(), 2);
}

TEST(CompactChildrenSet, CopyAssignmentFromSetToSet)
{
    CompactChildrenSet cs1;
    cs1.insert(CHILD_A);
    cs1.insert(CHILD_B);

    CompactChildrenSet cs2;
    cs2.insert(CHILD_C);
    cs2.insert(CHILD_A);

    cs2 = cs1;
    EXPECT_EQ(cs2.size(), 2);
    EXPECT_TRUE(cs2.contains(CHILD_A));
    EXPECT_TRUE(cs2.contains(CHILD_B));
}

TEST(CompactChildrenSet, CopyAssignmentSelfAssign)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_B);

    auto & ref = cs;
    cs = ref;  // NOLINT: intentional self-assignment test
    EXPECT_EQ(cs.size(), 2);
}

TEST(CompactChildrenSet, MoveFromSingle)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);

    CompactChildrenSet moved(std::move(cs));
    EXPECT_EQ(moved.size(), 1);
    EXPECT_TRUE(moved.contains(CHILD_A));
    EXPECT_TRUE(cs.empty());  // NOLINT: testing moved-from state
}

TEST(CompactChildrenSet, MoveFromSet)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_B);

    CompactChildrenSet moved(std::move(cs));
    EXPECT_EQ(moved.size(), 2);
    EXPECT_TRUE(cs.empty());  // NOLINT: testing moved-from state
}

TEST(CompactChildrenSet, MoveAssignment)
{
    CompactChildrenSet cs1;
    cs1.insert(CHILD_A);
    cs1.insert(CHILD_B);

    CompactChildrenSet cs2;
    cs2.insert(CHILD_C);

    cs2 = std::move(cs1);
    EXPECT_EQ(cs2.size(), 2);
    EXPECT_TRUE(cs2.contains(CHILD_A));
    EXPECT_TRUE(cs1.empty());  // NOLINT: testing moved-from state
}

TEST(CompactChildrenSet, IteratorSetMode)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_B);
    cs.insert(CHILD_C);

    size_t count = 0;
    bool found_a = false;
    bool found_b = false;
    bool found_c = false;
    for (const auto & child : cs)
    {
        if (child == CHILD_A) found_a = true;
        if (child == CHILD_B) found_b = true;
        if (child == CHILD_C) found_c = true;
        ++count;
    }
    EXPECT_EQ(count, 3);
    EXPECT_TRUE(found_a);
    EXPECT_TRUE(found_b);
    EXPECT_TRUE(found_c);
}

TEST(CompactChildrenSet, IteratorPostIncrement)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);

    auto it = cs.begin();
    auto prev = it++;
    EXPECT_EQ(*prev, CHILD_A);
    EXPECT_EQ(it, cs.end());
}

TEST(CompactChildrenSet, RejectsEmptyChildName)
{
#ifndef DEBUG_OR_SANITIZER_BUILD
    /// `LOGICAL_ERROR` aborts in debug/sanitizer builds, so we can only test
    /// the throwing path in release builds.
    CompactChildrenSet cs;
    EXPECT_THROW(cs.insert(""), Exception);

    /// Also in single mode
    cs.insert(CHILD_A);
    EXPECT_THROW(cs.insert(""), Exception);

    /// Also in set mode
    cs.insert(CHILD_B);
    EXPECT_THROW(cs.insert(""), Exception);

    /// Container unchanged after rejected inserts
    EXPECT_EQ(cs.size(), 2);
#endif
}

TEST(CompactChildrenSet, CopyAssignmentSetToSingle)
{
    CompactChildrenSet cs_set;
    cs_set.insert(CHILD_A);
    cs_set.insert(CHILD_B);

    CompactChildrenSet cs_single;
    cs_single.insert(CHILD_C);

    cs_single = cs_set;
    EXPECT_EQ(cs_single.size(), 2);
    EXPECT_TRUE(cs_single.contains(CHILD_A));
    EXPECT_TRUE(cs_single.contains(CHILD_B));
}

TEST(CompactChildrenSet, CopyAssignmentSetToEmpty)
{
    CompactChildrenSet cs_set;
    cs_set.insert(CHILD_A);
    cs_set.insert(CHILD_B);

    CompactChildrenSet cs_empty;
    cs_empty = cs_set;
    EXPECT_EQ(cs_empty.size(), 2);
    EXPECT_TRUE(cs_empty.contains(CHILD_A));
    EXPECT_TRUE(cs_empty.contains(CHILD_B));
}

TEST(CompactChildrenSet, CopyAssignmentSingleToSet)
{
    CompactChildrenSet cs_single;
    cs_single.insert(CHILD_A);

    CompactChildrenSet cs_set;
    cs_set.insert(CHILD_B);
    cs_set.insert(CHILD_C);

    cs_set = cs_single;
    EXPECT_EQ(cs_set.size(), 1);
    EXPECT_TRUE(cs_set.contains(CHILD_A));
    EXPECT_FALSE(cs_set.contains(CHILD_B));
}

TEST(CompactChildrenSet, MoveAssignmentToSetTarget)
{
    CompactChildrenSet src;
    src.insert(CHILD_A);

    CompactChildrenSet tgt;
    tgt.insert(CHILD_B);
    tgt.insert(CHILD_C);

    tgt = std::move(src);
    EXPECT_EQ(tgt.size(), 1);
    EXPECT_TRUE(tgt.contains(CHILD_A));
    EXPECT_TRUE(src.empty());  // NOLINT: testing moved-from state
}

TEST(CompactChildrenSet, MoveAssignmentSetToSet)
{
    CompactChildrenSet src;
    src.insert(CHILD_A);
    src.insert(CHILD_B);

    CompactChildrenSet tgt;
    tgt.insert(CHILD_C);
    tgt.insert(CHILD_A);

    tgt = std::move(src);
    EXPECT_EQ(tgt.size(), 2);
    EXPECT_TRUE(tgt.contains(CHILD_A));
    EXPECT_TRUE(tgt.contains(CHILD_B));
    EXPECT_TRUE(src.empty());  // NOLINT: testing moved-from state
}

TEST(CompactChildrenSet, ReserveOnSetMode)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);
    cs.insert(CHILD_B);

    /// Reserve on already-set-mode container should just forward to the set
    cs.reserve(100);
    EXPECT_EQ(cs.size(), 2);
    EXPECT_TRUE(cs.contains(CHILD_A));
    EXPECT_TRUE(cs.contains(CHILD_B));
}

TEST(CompactChildrenSet, CopyAssignmentEmptyToSet)
{
    CompactChildrenSet cs_empty;

    CompactChildrenSet cs_set;
    cs_set.insert(CHILD_A);
    cs_set.insert(CHILD_B);

    /// Overwrite set-mode target with empty source
    cs_set = cs_empty;
    EXPECT_TRUE(cs_set.empty());
    EXPECT_EQ(cs_set.size(), 0);
}

TEST(CompactChildrenSet, MoveAssignmentFromEmpty)
{
    CompactChildrenSet src;

    CompactChildrenSet tgt;
    tgt.insert(CHILD_A);
    tgt.insert(CHILD_B);

    /// Move empty into set-mode target
    tgt = std::move(src);
    EXPECT_TRUE(tgt.empty());
    EXPECT_TRUE(src.empty());  // NOLINT: testing moved-from state
}

TEST(CompactChildrenSet, ReserveOnSingleThenEraseToZero)
{
    CompactChildrenSet cs;
    cs.insert(CHILD_A);

    /// Promote single to set via reserve
    cs.reserve(10);
    EXPECT_EQ(cs.size(), 1);
    EXPECT_TRUE(cs.contains(CHILD_A));

    /// Erase the only element — should clean up the set
    cs.erase(CHILD_A);
    EXPECT_TRUE(cs.empty());
    EXPECT_EQ(cs.size(), 0);
    EXPECT_EQ(cs.heapSizeInBytes(), 0);
}
