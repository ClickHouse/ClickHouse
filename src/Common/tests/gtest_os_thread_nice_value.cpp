#include <gtest/gtest.h>
#include <Common/OSThreadNiceValue.h>

#if defined(OS_LINUX)
#include <sys/resource.h>
#include <linux/capability.h>
#include <Common/hasLinuxCapability.h>
#include <Common/ErrnoException.h>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int CANNOT_SET_THREAD_PRIORITY;
    extern const int CANNOT_GET_THREAD_PRIORITY;
}

int getCurrentNiceValue(UInt32 thread_id = 0)
{
    errno = 0;
    const int value = getpriority(PRIO_PROCESS, thread_id);
    if (value == -1 && errno != 0)
    {
        throw ErrnoException(ErrorCodes::CANNOT_GET_THREAD_PRIORITY, 
            "Failed to get the current nice value for thread {}", thread_id);
    }
    return value;
}

TEST(OSThreadNiceValue, SetCurrentThread)
{
    if (!hasLinuxCapability(CAP_SYS_NICE))
    {
        GTEST_SKIP() << "No CAP_SYS_NICE capability";
    }

    const int original = getCurrentNiceValue();

    ASSERT_NO_THROW(OSThreadNiceValue::set(-5));

    const int current = getCurrentNiceValue();
    ASSERT_EQ(current, -5);

    OSThreadNiceValue::set(original);
}

TEST(OSThreadNiceValue, ScopedGuard)
{
    if (!hasLinuxCapability(CAP_SYS_NICE))
    {
        GTEST_SKIP() << "No CAP_SYS_NICE capability";
    }

    const int original = getCurrentNiceValue();

    OSThreadNiceValue::set(3);
    
    const int changed = getCurrentNiceValue();
    ASSERT_EQ(changed, 3);

    {
        auto guard = OSThreadNiceValue::scoped(5);

        const int current = getCurrentNiceValue();
        ASSERT_EQ(current, 5);
    }

    const int restored = getCurrentNiceValue();
    ASSERT_EQ(restored, 3);
    
    OSThreadNiceValue::set(original);
}

TEST(OSThreadNiceValue, NonPrivilegedPositiveValue)
{
    if (!hasLinuxCapability(CAP_SYS_NICE))
    {
        GTEST_SKIP() << "No CAP_SYS_NICE capability";
    }

    const int original = getCurrentNiceValue();

    ASSERT_NO_THROW(OSThreadNiceValue::set(5));
    
    const int current = getCurrentNiceValue();
    ASSERT_EQ(current, 5);

    OSThreadNiceValue::set(original);
}
#endif
