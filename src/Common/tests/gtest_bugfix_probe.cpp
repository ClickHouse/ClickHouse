#include <Common/escapeForFileName.h>

#include <gtest/gtest.h>


using namespace DB;


/// CI validation only (do not merge): exercises the "failed to reproduce" path of the
/// unit-test bugfix validation job. The assertion is already true on master, and this
/// PR makes no source change, so the test PASSES on both the merge-base "before" binary
/// and the PR binary. The job must therefore report FAIL ("the added test does not catch
/// the bug"), since a real regression test must fail without the fix.
TEST(BugfixProbeNoRepro, AlreadyTrueOnMaster)
{
    EXPECT_EQ(escapeForFileName("abc"), "abc");
    EXPECT_EQ(unescapeForFileName("abc"), "abc");
}
