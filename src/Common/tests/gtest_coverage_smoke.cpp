#include <gtest/gtest.h>

/// Smoke test for LLVM coverage pipeline.
/// Exercises a trivial arithmetic path that is already covered in every
/// master run — should NOT appear in Newly Covered output.
TEST(CoverageSmoke, TrivialArithmetic)
{
    EXPECT_EQ(1 + 1, 2);
}
