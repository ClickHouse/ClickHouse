#pragma once

#include <cstddef>
#include <mutex>
#include <string_view>
#include <vector>
#include <base/defines.h>
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
        /// Number of times Exception with this ErrorCode has been thrown.
        Value count = 0;
        /// Time of the last error.
        UInt64 error_time_ms = 0;
        /// Message for the last error.
        std::string message;
        /// Stacktrace for the last error.
        FramePointers trace;
        /// Initial query id if available.
        std::string query_id;
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
        size_t increment(bool remote, const std::string & message, const FramePointers & trace);
        void extendedMessage(bool remote, size_t error_index, const std::string & new_message);

    private:
        ErrorPair value TSA_GUARDED_BY(mutex);
        std::mutex mutex;
    };

    /// ErrorCode identifier -> current value of error_code.
    extern ErrorPairHolder values[];

    /// Get index just after last error_code identifier.
    ErrorCode end();

    /// Increments the counter of errors for a specified error code, and remembers some information about the last error.
    /// The function returns the index of the passed error among other errors with the same code and the same `remote` flag
    /// since the program startup.
    size_t increment(ErrorCode error_code, bool remote, const std::string & message, const FramePointers & trace);

    /// Extends the error message after it was set by a call to increment().
    /// The result of that call to increment() should be passed to this function as `error_index`.
    /// Exceptions are often extended by calling Exception::addMessage() and in such cases
    /// this function helps to update the information stored in ErrorCodes in order to
    /// make the "system.errors" table able to show the extended error messages.
    void extendedMessage(ErrorCode error_code, bool remote, size_t error_index, const std::string & new_message);
}

}
