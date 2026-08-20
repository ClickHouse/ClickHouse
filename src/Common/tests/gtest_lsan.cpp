#include <base/defines.h> // ADDRESS_SANITIZER

#ifdef ADDRESS_SANITIZER

#include <cstdlib>
#include <thread>

#include <base/safeExit.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <sanitizer/lsan_interface.h>

/// Test that ensures that LSan works.
///
/// Regression test for the case when it may not work,
/// because of broken getauxval() [1].
///
///   [1]: https://github.com/ClickHouse/ClickHouse/pull/33957
TEST(SanitizerDeathTest, LSan)
{
    EXPECT_DEATH(
        {
            std::thread leak_in_thread(
                []()
                {
                    void * leak = malloc(4096);
                    ASSERT_NE(leak, nullptr);
                });
            leak_in_thread.join();

            __lsan_do_leak_check();
        },
        ".*LeakSanitizer: detected memory leaks.*");
}

namespace
{

/// Leak in a joined thread, as `SanitizerDeathTest.LSan` above does: a pointer left in the
/// current frame stays reachable through the scanned stack and would not be reported.
void leakInJoinedThread()
{
    std::thread leak_in_thread([] { ASSERT_NE(malloc(4096), nullptr); });
    leak_in_thread.join();
}

/// Separate entry points rather than a forwarded flag, so the default arm exercises the
/// one-argument overload that Keeper actually calls.
[[noreturn]] void leakAndExitByDefault() { leakInJoinedThread(); safeExit(0); }
[[noreturn]] void leakAndExitSkipReporting() { leakInJoinedThread(); safeExit(0, LeakCheck::SkipAndReport); }
[[noreturn]] void leakAndExitSkipQuietly() { leakInJoinedThread(); safeExit(0, LeakCheck::SkipQuietly); }

/// The complete line safeExit() writes, newline included: it is one write(2), and the CI
/// detector filters it with `grep -F -x`, so any drift in its tail breaks that whole-line match.
constexpr auto SKIP_NOTICE_LINE = "Not running the leak check: other threads are still running.\n";

}

/// safeExit() runs the leak check by default, ...
TEST(SanitizerDeathTest, SafeExitRunsLeakCheckByDefault)
{
    EXPECT_DEATH(leakAndExitByDefault(), ".*LeakSanitizer: detected memory leaks.*");
}

/// ... and skips it, saying so, when the caller says other threads are still running.
TEST(SanitizerDeathTest, SafeExitSkipsLeakCheckOnRequest)
{
    EXPECT_EXIT(leakAndExitSkipReporting(), testing::ExitedWithCode(0),
        testing::AllOf(
            testing::HasSubstr(SKIP_NOTICE_LINE),
            testing::Not(testing::ContainsRegex("LeakSanitizer: detected memory leaks"))));
}

/// ... or without a word, for callers whose stderr is program output.
TEST(SanitizerDeathTest, SafeExitSkipsLeakCheckQuietly)
{
    EXPECT_EXIT(leakAndExitSkipQuietly(), testing::ExitedWithCode(0),
        testing::AllOf(
            testing::Not(testing::HasSubstr(SKIP_NOTICE_LINE)),
            testing::Not(testing::ContainsRegex("LeakSanitizer: detected memory leaks"))));
}

#endif
