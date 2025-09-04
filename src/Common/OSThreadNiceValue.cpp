#include <Common/Exception.h>
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
        const extern int CANNOT_SET_THREAD_PRIORITY;
    }

    void OSThreadNiceValue::set(const Int32 value, const UInt32 thread_id)
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

    scope_guard OSThreadNiceValue::scoped(const Int32 value, const UInt32 thread_id)
    {
        set(value, thread_id);
        return [thread_id]()
        {
            try
            {
                set(0, thread_id);
            }
            catch(...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        };
    }
}
