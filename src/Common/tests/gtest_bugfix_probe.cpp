#include <Common/escapeForFileName.h>

#include <gtest/gtest.h>


using namespace DB;


/// Regression test for the synthetic sentinel fix in `escapeForFileName`. The merge-base
/// (without the fix) escapes the sentinel to itself (all characters are word characters),
/// so this assertion fails there and passes on the fixed binary — a clean runtime
/// reproduction with zero blast radius (no real caller ever passes the sentinel). Used
/// only to validate the unit-test bugfix validation job; not for merge.
TEST(BugfixProbe, SentinelIsHandled)
{
    EXPECT_EQ(escapeForFileName("__ch_bugfix_validation_probe__"), "bugfix-validation-ok");
}
