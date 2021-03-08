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
    using ErrorCode = int;
    using Value = size_t;

    /// Get name of error_code by identifier.
    /// Returns statically allocated string.
    std::string_view getName(ErrorCode error_code);

    /// ErrorCode identifier -> current value of error_code.
    struct ValuePair
    {
        std::atomic<Value> local;
        std::atomic<Value> remote;
    };
    extern ValuePair values[];

    /// Get index just after last error_code identifier.
    ErrorCode end();

    /// Add value for specified error_code.
    void increment(ErrorCode error_code, bool remote);
}

}
