#pragma once

#include <stddef.h>
#include <cstdint>
#include <utility>
#include <mutex>
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

    struct ValuePair
    {
        Value local = 0;
        Value remote = 0;
        UInt64 last_error_time_ms = 0;
        std::string message;

        ValuePair & operator+=(const ValuePair & value);
    };

    /// Thread-safe
    struct ValuePairHolder
    {
    public:
        void increment(const ValuePair & value_);
        ValuePair get();

    private:
        ValuePair value;
        std::mutex mutex;
    };

    /// ErrorCode identifier -> current value of error_code.
    extern ValuePairHolder values[];

    /// Get index just after last error_code identifier.
    ErrorCode end();

    /// Add value for specified error_code.
    void increment(ErrorCode error_code, bool remote, const std::string & message);
}

}
