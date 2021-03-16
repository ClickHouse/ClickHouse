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
    void increment(ErrorCode error_code);
}

}
