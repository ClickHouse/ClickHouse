#include <base/defines.h> // ADDRESS_SANITIZER

#ifdef ADDRESS_SANITIZER

#include <cstdlib>
#include <thread>

#include <gtest/gtest.h>
#include <sanitizer/lsan_interface.h>

/// Test that ensures that LSan works.
///
/// Regression test for the case when it may not work,
/// because of broken getauxval() [1].
///
///   [1]: https://github.com/ClickHouse/ClickHouse/pull/33957
TEST(Common, LSan)
{
    int sanitizers_exit_code = 1;

    ASSERT_EXIT({
        std::thread leak_in_thread([]()
        {
            void * leak = malloc(4096);
            ASSERT_NE(leak, nullptr);
        });
        leak_in_thread.join();

        __lsan_do_leak_check();
    }, ::testing::ExitedWithCode(sanitizers_exit_code), ".*LeakSanitizer: detected memory leaks.*");
}

#endif
