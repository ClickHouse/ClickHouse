#include <Common/escapeForFileName.h>

#include <gtest/gtest.h>


using namespace DB;


/// Regression test for the dot-escaping fix in `escapeForFileName`: dots must be
/// kept verbatim rather than percent-encoded. Without the fix this asserts
/// `a%2Eb`, so the test fails on the unfixed (merge-base) binary and passes on
/// the fixed one — exactly what the unit-test bugfix validation job checks.
TEST(BugfixProbe, DotsAreNotEscaped)
{
    EXPECT_EQ(escapeForFileName("a.b"), "a.b");
    EXPECT_EQ(unescapeForFileName(escapeForFileName("a.b")), "a.b");
}
