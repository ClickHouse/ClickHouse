#include <gtest/gtest.h>

#include <Processors/QueryPlan/Optimizations/dpTable.h>
#include <Processors/QueryPlan/Optimizations/enumeratorChecker.h>

using namespace DB;

namespace
{

struct TestPlan
{
    UInt32 left = 0;
    UInt32 right = 0;
    std::optional<UInt64> estimated_rows;
    double cost = 0;
    double rows_cost = 0;
    double sel = 0;
    JoinKind kind = JoinKind::Inner;
    JoinStrictness strictness = JoinStrictness::All;
    JoinMethod method = JoinMethod::Hash;
    bool sharded = false;
    Int8 merge_class = -1;
    Int8 order_class = -1;
    Int8 left_plan = -1;
    Int8 right_plan = -1;
    VectorWithMemoryTracking<JoinActionRef *> edges;
};

struct TestEntry : TestPlan
{
    using Plan = TestPlan;
    UInt32 neighbor = 0;
    VectorWithMemoryTracking<TestPlan> ordered = {};
};

using TestDPTable = DPTable<TestEntry, UInt32>;

struct TestOptimizer
{
    LoggerPtr log = getLogger("EnumeratorCheckerTest");
    VectorWithMemoryTracking<JoinActionRef *> edges;

    [[nodiscard]] std::optional<std::pair<JoinKind, JoinStrictness>> resolveJoinMask(UInt32, UInt32) const
    {
        return std::pair{JoinKind::Inner, JoinStrictness::All};
    }
    [[nodiscard]] const VectorWithMemoryTracking<JoinActionRef *> & collectJoinEdgesMask(UInt32, UInt32) const { return edges; }
    [[nodiscard]] bool areTransitivelyConnectedMask(UInt32, UInt32) const { return true; }
    [[nodiscard]] SelectivityEstimate computeSelectivityMask(
        const VectorWithMemoryTracking<JoinActionRef *> &, UInt32, UInt32,
        std::optional<UInt64> left_rows, std::optional<UInt64>) const
    {
        const double value = left_rows == 10 ? 0.1 : 0.01;
        return {.value = value, .reported_value = value, .reliable = true, .has_equi = true};
    }
    [[nodiscard]] double hashJoinWork(UInt32, std::optional<UInt64>, UInt32, std::optional<UInt64>) const { return 0; }
    [[nodiscard]] bool useInterestingOrders() const { return true; }
    [[nodiscard]] bool isReadInOrder(UInt32, Int32) const { return false; }
    [[nodiscard]] bool isShardableLeaf(UInt32, Int32) const { return false; }
    [[nodiscard]] UInt64 mergeKeyClasses(
        const VectorWithMemoryTracking<JoinActionRef *> &, UInt32, UInt32, JoinKind, JoinStrictness) const
    {
        return 1;
    }
    [[nodiscard]] UInt64 joinKeyClasses(const VectorWithMemoryTracking<JoinActionRef *> &, UInt32, UInt32) const
    {
        return 1;
    }
    [[nodiscard]] double mergeJoinCost(
        MergeJoinInput left, UInt32, MergeJoinInput, UInt32, Int32, UInt64, const SelectivityEstimate &) const
    {
        return left.rows == 10 ? 0 : 100;
    }
    [[nodiscard]] bool isInterestingOrder(Int32, UInt32) const { return true; }
    [[nodiscard]] bool isShardedMerge(const MergeJoinInput &, UInt32, const MergeJoinInput &, UInt32, UInt64) const { return false; }
    [[nodiscard]] std::optional<UInt64> estimateCardinality(
        std::optional<UInt64> left_rows, std::optional<UInt64> right_rows,
        const SelectivityEstimate & selectivity, JoinKind kind, JoinStrictness strictness) const
    {
        return estimateJoinCardinality(left_rows, right_rows, selectivity, kind, strictness);
    }
};

TEST(EnumeratorCheckerWithCosts, MergeJoinUsesSelectedInputRowsForSelectivity)
{
    TestOptimizer optimizer;
    EnumeratorCheckerWithCosts<TestDPTable, TestOptimizer> checker(3, optimizer);
    auto & table = checker.getDPTable();

    table.insert(3, 1, 2);
    table[3].left = 1;
    table[3].right = 2;
    table[3].estimated_rows = 100;
    TestPlan sorted;
    sorted.estimated_rows = 10;
    sorted.order_class = 0;
    table[3].ordered.push_back(sorted);
    table[4].estimated_rows = 10;

    checker.accept(7, 3, 4);

    const auto & result = table[7].ordered.at(0);
    ASSERT_EQ(result.left_plan, 0);
    EXPECT_DOUBLE_EQ(result.sel, 0.1);
    EXPECT_EQ(result.estimated_rows, 10);
}

}
