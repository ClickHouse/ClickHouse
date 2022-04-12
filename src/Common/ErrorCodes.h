#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>
#include <mutex>
#include <string_view>
#include <vector>
#include <base/types.h>

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
    using FramePointers = std::vector<void *>;

    /// Get name of error_code by identifier.
    /// Returns statically allocated string.
    std::string_view getName(ErrorCode error_code);
    /// Get error code value by name.
    ///
    /// It has O(N) complexity, but this is not major, since it is used only
    /// for test hints, and it does not worth to keep another structure for
    /// this.
    ErrorCode getErrorCodeByName(std::string_view error_name);

    struct Error
    {
        /// Number of times Exception with this ErrorCode had been throw.
        Value count = 0;
        /// Time of the last error.
        UInt64 error_time_ms = 0;
        /// Message for the last error.
        std::string message;
        /// Stacktrace for the last error.
        FramePointers trace;
    };
    struct ErrorPair
    {
        Error local;
        Error remote;
    };

    /// Thread-safe
    struct ErrorPairHolder
    {
    public:
        ErrorPair get();
        void increment(bool remote, const std::string & message, const FramePointers & trace);

    private:
        ErrorPair value;
        std::mutex mutex;
    };

    /// ErrorCode identifier -> current value of error_code.
    extern ErrorPairHolder values[];

    /// Get index just after last error_code identifier.
    ErrorCode end();

    /// Add value for specified error_code.
    void increment(ErrorCode error_code, bool remote, const std::string & message, const FramePointers & trace);
}

}
