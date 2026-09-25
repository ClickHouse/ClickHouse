#include <gtest/gtest.h>

#include <Common/StackTrace.h>
#include <base/defines.h>

#include <pthread.h>

namespace
{

/// Unwinding from the call interprets all 2000 pairs. If each pair kept its own saved state on the
/// stack, that would take about 1 MiB on x86-64 and 3 MiB on ARM64.
size_t NO_INLINE captureAfterManyRememberStates()
{
    __asm__ __volatile__(".rept 2000\n.cfi_remember_state\n.cfi_restore_state\n.endr");
    StackTrace trace;
    return trace.getSize();
}

void * captureOnThread(void * result)
{
    *static_cast<size_t *>(result) = captureAfterManyRememberStates();
    return nullptr;
}

}

TEST(StackTrace, RememberStatePairsFitSmallStack)
{
    pthread_attr_t attr;
    ASSERT_EQ(0, pthread_attr_init(&attr));
    ASSERT_EQ(0, pthread_attr_setstacksize(&attr, 256 * 1024));

    size_t size = 0;
    pthread_t thread{};
    ASSERT_EQ(0, pthread_create(&thread, &attr, captureOnThread, &size));
    ASSERT_EQ(0, pthread_join(thread, nullptr));
    pthread_attr_destroy(&attr);

    /// The capture walks past the function, so it has more than its own frame.
    EXPECT_GT(size, 1u);
}
