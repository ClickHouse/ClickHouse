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
void leakAndExit(bool run_leak_check)
{
    std::thread leak_in_thread([] { ASSERT_NE(malloc(4096), nullptr); });
    leak_in_thread.join();
    safeExit(0, run_leak_check);
}

}

/// safeExit() runs the leak check by default, ...
TEST(SanitizerDeathTest, SafeExitRunsLeakCheckByDefault)
{
    EXPECT_DEATH(leakAndExit(/*run_leak_check=*/ true), ".*LeakSanitizer: detected memory leaks.*");
}

/// ... and skips it when the caller says other threads are still running.
TEST(SanitizerDeathTest, SafeExitSkipsLeakCheckOnRequest)
{
    EXPECT_EXIT(leakAndExit(/*run_leak_check=*/ false), testing::ExitedWithCode(0),
        testing::Not(testing::ContainsRegex("LeakSanitizer: detected memory leaks")));
}

#endif
