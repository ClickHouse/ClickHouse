#include <Common/escapeForFileName.h>

#include <gtest/gtest.h>


using namespace DB;


/// CI validation only (do not merge): exercises the "compile failure = inconclusive"
/// path of the unit-test bugfix validation job. This test calls bugfixProbeIntroducedSymbol,
/// a symbol this PR adds in a non-test file (escapeForFileName.h/.cpp). The job overlays
/// ONLY test files onto the merge-base, so on the "before" binary this symbol does not
/// exist and the test fails to COMPILE. The job must report ERROR/inconclusive (a compile
/// failure does not prove the test reproduces a bug), NOT a pass.
TEST(BugfixProbeCompileFail, UsesSymbolIntroducedByThisPR)
{
    EXPECT_EQ(bugfixProbeIntroducedSymbol(), "introduced-by-this-pr");
}
