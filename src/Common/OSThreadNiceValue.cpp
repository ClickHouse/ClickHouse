#include <Common/Exception.h>
#include <Common/ErrnoException.h>
#include <Common/OSThreadNiceValue.h>

#if defined(OS_LINUX)
#include <sys/resource.h>
#include <linux/capability.h>
#include <Common/hasLinuxCapability.h>
#endif

namespace DB
{
    namespace ErrorCodes
    {
        extern const int CANNOT_GET_THREAD_PRIORITY;
        extern const int CANNOT_SET_THREAD_PRIORITY;
    }

    void OSThreadNiceValue::set([[maybe_unused]] const Int32 value, [[maybe_unused]] const UInt32 thread_id)
    {
#if defined(OS_LINUX)
        if (hasLinuxCapability(CAP_SYS_NICE))
        {
            if (setpriority(PRIO_PROCESS, thread_id, value) != 0)
            {
                throw ErrnoException(ErrorCodes::CANNOT_SET_THREAD_PRIORITY,
                    "Failed to set the nice value {} for thread {}", value, thread_id);
            }
        }
#endif
    }

    scope_guard OSThreadNiceValue::scoped([[maybe_unused]] const Int32 value, [[maybe_unused]] const UInt32 thread_id)
    {
#if defined(OS_LINUX)
        errno = 0;
        const Int32 original_value = getpriority(PRIO_PROCESS, thread_id);
        if (original_value == -1 && errno != 0)
        {
            throw ErrnoException(ErrorCodes::CANNOT_GET_THREAD_PRIORITY,
                "Failed to get the current nice value for thread {}", thread_id);
        }

        set(value, thread_id);

        return [original_value, thread_id]()
        {
            try
            {
                set(original_value, thread_id);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        };
#else
        return [](){};
#endif
    }
}
