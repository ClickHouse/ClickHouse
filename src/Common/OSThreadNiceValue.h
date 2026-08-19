#pragma once

#include <base/scope_guard.h>
#include <base/types.h>
#include <Common/Exception.h>


namespace DB
{
    struct OSThreadNiceValue
    {
        /// Set the nice value of a thread (0 = current thread).
        /// Linux-only, negative values require CAP_SYS_NICE, otherwise no-op.
        static void set(Int32 value, UInt32 thread_id = 0);

        /// Set the nice value of a thread (0 = current thread), returns a guard that resets it to 0.
        /// Linux-only, negative values require CAP_SYS_NICE, otherwise no-op.
        static scope_guard scoped(Int32 value, UInt32 thread_id = 0);
    };
}
