#pragma once

#include <stddef.h>
#include <cstdint>
#include <utility>
#include <atomic>
#include <common/types.h>
#include <string_view>

/** Allows to count number of simultaneously happening error codes.
  * See also Exception.cpp for incrementing part.
  */

namespace DB
{

namespace ErrorCodes
{
    /// ErrorCode identifier (index in array).
    using ErrorCode = size_t;
    using Value = int;

    /// Get name of error_code by identifier.
    /// Returns statically allocated string.
    std::string_view getName(ErrorCode error_code);

    /// ErrorCode identifier -> current value of error_code.
    extern std::atomic<Value> values[];

    /// Get index just after last error_code identifier.
    ErrorCode end();

    /// Add value for specified error_code.
    inline void increment(ErrorCode error_code)
    {
        if (error_code >= end())
        {
            /// For everything outside the range, use END.
            /// (end() is the pointer pass the end, while END is the last value that has an element in values array).
            error_code = end() - 1;
        }
        values[error_code].fetch_add(1, std::memory_order_relaxed);
    }
}

}
