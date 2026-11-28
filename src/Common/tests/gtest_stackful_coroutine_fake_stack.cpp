#include <base/defines.h> // ADDRESS_SANITIZER

#if defined(ADDRESS_SANITIZER)

#include <Common/CoroutineStack.h>
#include <Common/StackfulCoroutine.h>
#include <base/getPageSize.h>

#include <gtest/gtest.h>
#include <sanitizer/asan_interface.h>
#include <sys/mman.h>

#include <cerrno>
#include <cstdint>

/// With `detect_stack_use_after_return`, ASan gives each coroutine its own fake stack. It must stay mapped while the
/// coroutine is suspended, and be unmapped when the coroutine finishes or is destroyed while suspended.

namespace
{

bool isMapped(const void * address)
{
    const auto page_size = static_cast<uintptr_t>(getPageSize());
    void * page = reinterpret_cast<void *>(reinterpret_cast<uintptr_t>(address) & ~(page_size - 1));
    unsigned char residency = 0;
    return ::mincore(page, page_size, &residency) == 0 || errno != ENOMEM;
}

}

TEST(StackfulCoroutineFakeStack, KeptWhileSuspendedAndReleasedWhenFinished)
{
    if (!__asan_get_current_fake_stack())
        GTEST_SKIP() << "detect_stack_use_after_return is disabled";

    void * before_suspend = nullptr;
    void * after_resume = nullptr;
    StackfulCoroutine coroutine(CoroutineStack(), [&](auto & suspend)
    {
        before_suspend = __asan_get_current_fake_stack();
        suspend();
        after_resume = __asan_get_current_fake_stack();
    });

    coroutine.resume();
    ASSERT_TRUE(coroutine);
    ASSERT_NE(before_suspend, nullptr);
    EXPECT_TRUE(isMapped(before_suspend));

    coroutine.resume();
    ASSERT_FALSE(coroutine);
    EXPECT_EQ(after_resume, before_suspend);
    EXPECT_FALSE(isMapped(before_suspend));
}

TEST(StackfulCoroutineFakeStack, ReleasedWhenDestroyedWhileSuspended)
{
    if (!__asan_get_current_fake_stack())
        GTEST_SKIP() << "detect_stack_use_after_return is disabled";

    void * fake_stack = nullptr;
    {
        StackfulCoroutine coroutine(CoroutineStack(), [&](auto & suspend)
        {
            fake_stack = __asan_get_current_fake_stack();
            suspend();
        });
        coroutine.resume();
        ASSERT_TRUE(coroutine);
    }

    ASSERT_NE(fake_stack, nullptr);
    EXPECT_FALSE(isMapped(fake_stack));
}

#endif
