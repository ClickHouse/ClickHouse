#include <gtest/gtest.h>

#include <Analyzer/Passes/RewriteIntersectExceptToJoinPass.h>
#include <Core/Settings.h>

using namespace DB;

/// `join_algorithm` is a preference list that `chooseJoinAlgorithm` walks in order, so the rewrite of
/// `INTERSECT DISTINCT` and `EXCEPT DISTINCT` has to decide from the first algorithm that either runs the
/// join or fails it, and keep the set-operation step (which needs neither a spill threshold nor temporary
/// storage) where that algorithm fails.
namespace
{

Settings makeSettings(const String & join_algorithm, UInt64 max_bytes_before_external_join)
{
    Settings settings;
    settings.set("join_algorithm", join_algorithm);
    settings.set("legacy_join_size_limits_trigger_spilling", false);
    settings.set("max_bytes_before_external_join", max_bytes_before_external_join);
    settings.set("max_bytes_ratio_before_external_join", 0.0);
    return settings;
}

bool executes(const String & join_algorithm, UInt64 max_bytes_before_external_join, JoinStrictness strictness)
{
    return joinAlgorithmExecutesSetOperationJoin(
        makeSettings(join_algorithm, max_bytes_before_external_join), strictness, /*has_merge_unsafe_key=*/ false);
}

}

/// The join is built on every server that executes the plan, from that server's own temporary storage, so a
/// `grace_hash` with a spill threshold that is reached first keeps the set-operation step.
TEST(IntersectExceptJoinAlgorithm, GraceHashWithSpillThresholdFailsTheJoinBeforeHashIsReached)
{
    for (const auto strictness : {JoinStrictness::Semi, JoinStrictness::Anti})
    {
        EXPECT_FALSE(executes("grace_hash,hash", 1000000, strictness));
        EXPECT_FALSE(executes("direct,grace_hash,hash", 1000000, strictness));
        EXPECT_FALSE(executes("grace_hash", 1000000, strictness));
        EXPECT_TRUE(executes("hash,grace_hash", 1000000, strictness));
    }
}

TEST(IntersectExceptJoinAlgorithm, GraceHashWithoutSpillThresholdIsPassedOverUnlessListedAlone)
{
    for (const auto strictness : {JoinStrictness::Semi, JoinStrictness::Anti})
    {
        EXPECT_TRUE(executes("grace_hash,hash", 0, strictness));
        EXPECT_FALSE(executes("grace_hash", 0, strictness));
    }
}

TEST(IntersectExceptJoinAlgorithm, AlgorithmsThatCannotExecuteTheJoinArePassedOver)
{
    EXPECT_TRUE(executes("partial_merge,hash", 0, JoinStrictness::Anti));
    EXPECT_TRUE(executes("partial_merge", 0, JoinStrictness::Semi));
    EXPECT_FALSE(executes("partial_merge", 0, JoinStrictness::Anti));
    EXPECT_TRUE(executes("direct,full_sorting_merge,parallel_hash", 0, JoinStrictness::Semi));
    EXPECT_FALSE(executes("full_sorting_merge,direct", 0, JoinStrictness::Semi));
}
