#include <algorithm>
#include <array>
#include <memory>
#include <chrono>
#include <iterator>
#include <map>
#include <optional>
#include <ranges>
#include <utility>
#if !defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
#include <Common/CurrentThread.h>
#endif
#include <Common/ErrorCodes.h>
#include <Common/ErrorCodesList.h>
#include <Common/Exception.h>

/// The code of ErrorCodes.h. The list of error codes is in ErrorCodes.cpp.

namespace DB
{
namespace ErrorCodes
{

extern const ErrorCode NO_SUCH_ERROR_CODE;

ErrorValues values;

namespace
{

/// The largest declared error code. Everything outside of [0, largest] is accounted under it.
ErrorCode largestCode()
{
    return List::sortedCodes().back().first;
}

/// The position of `error_code` in `codes` and `names`, if it is declared.
std::optional<size_t> findIndex(ErrorCode error_code)
{
    const auto sorted_codes = List::sortedCodes();
    const auto it = std::ranges::lower_bound(sorted_codes, error_code, {}, &std::pair<ErrorCode, size_t>::first);
    if (it == sorted_codes.end() || it->first != error_code)
        return {};
    return it->second;
}

/** The counters of all error codes.
  *
  * The declared error codes are not dense - there are large gaps between them - so a counter is kept only for a code
  * that actually exists, instead of an `ErrorPairHolder` per every value in the range. Custom error codes (see the
  * setting `allow_custom_error_code_in_throwif`) have no name and are not declared; they used to occupy a slot in the
  * dense array and are still counted separately, but their counters are allocated lazily, on the first occurrence.
  */
class ErrorRegistry
{
public:
    static ErrorRegistry & get()
    {
        /// Intentionally leaked: an error can be reported at any point of the shutdown, after the destructors
        /// of the objects with static storage duration have run.
        static ErrorRegistry & registry = *new ErrorRegistry;
        return registry;
    }

    ErrorPairHolder & operator[]([[maybe_unused]] ErrorCode error_code)
    {
#if defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
        /** A standalone build of the parser needs the names of error codes only, and never counts errors,
          * so it does not pay for the counters of the whole table.
          */
        static ErrorPairHolder stub;
        return stub;
#else
        if (const auto index = findIndex(error_code))
            return holders[*index];

        if (error_code >= 0 && error_code < largestCode())
            return custom(error_code);

        /// For everything outside of the range of declared error codes, use the last one.
        return holders[List::sortedCodes().back().second];
#endif
    }

    /// All error codes that have a counter: the declared ones plus the custom ones seen so far, in ascending order.
    std::vector<ErrorCode> getCodes() const
    {
        std::vector<ErrorCode> result;
        result.reserve(List::codes().size());
        for (const auto & [error_code, _] : List::sortedCodes())
            result.push_back(error_code);

#if !defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
        {
            std::lock_guard lock(custom_mutex);
            for (const auto & [error_code, _] : custom_holders)
                result.push_back(error_code);
        }
        std::ranges::sort(result);
#endif

        return result;
    }

private:
#if !defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
    /// The counter of a custom error code, created on the first occurrence of that code.
    ErrorPairHolder & custom(ErrorCode error_code)
    {
        std::lock_guard lock(custom_mutex);
        /// Entries are never removed and `std::map` keeps them in place, so the reference stays valid.
        return custom_holders.try_emplace(error_code).first->second;
    }

    /** One `ErrorPairHolder` per declared error code, each holding two `Error` structs - the last message,
      * format string, query id and captured stack trace, local and remote - plus a mutex.
      */
    std::unique_ptr<ErrorPairHolder[]> holders = std::make_unique<ErrorPairHolder[]>(List::codes().size());

    std::map<ErrorCode, ErrorPairHolder> custom_holders TSA_GUARDED_BY(custom_mutex);
    mutable std::mutex custom_mutex;
#endif
};

}

ErrorPairHolder & ErrorValues::operator[](ErrorCode error_code)
{
    return ErrorRegistry::get()[error_code];
}

std::string_view getName(ErrorCode error_code)
{
    if (const auto index = findIndex(error_code))
        return List::names()[*index];
    return {};
}

ErrorCode getErrorCodeByName(std::string_view error_name)
{
    const auto names = List::names();
    const auto it = std::ranges::find(names, error_name);
    if (it == names.end())
        throw Exception(NO_SUCH_ERROR_CODE, "No error code with name: '{}'", error_name);
    return List::codes()[static_cast<size_t>(std::distance(names.begin(), it))];
}

std::vector<ErrorCode> getCodes()
{
    return ErrorRegistry::get().getCodes();
}

size_t
increment(ErrorCode error_code, bool remote, const std::string & message, const std::string & format_string, const FramePointers & trace)
{
    return values[error_code].increment(remote, message, format_string, trace);
}

void extendedMessage(ErrorCode error_code, bool remote, size_t error_index, const std::string & message)
{
    values[error_code].extendedMessage(remote, error_index, message);
}

size_t ErrorPairHolder::increment(bool remote, const std::string & message, const std::string & format_string, const FramePointers & trace)
{
    size_t error_index = 0;
#if !defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
    const auto now = std::chrono::system_clock::now();

    std::lock_guard lock(mutex);
    auto & error = remote ? value.remote : value.local;

    error_index = error.count++;
    error.message = message;
    error.format_string = format_string;
    error.trace = trace;
    error.error_time_ms = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch()).count();
    error.query_id = CurrentThread::getQueryId();
#endif

    return error_index;
}

void ErrorPairHolder::extendedMessage(bool remote, size_t error_index, const std::string & new_message)
{
#if !defined(CLICKHOUSE_PARSER_MINIMAL_BUILD)
    std::lock_guard lock(mutex);
    auto & error = remote ? value.remote : value.local;

    /// This function is supposed to extend the current message.
    if ((error.count == error_index + 1) && new_message.starts_with(error.message))
        error.message = new_message;
#endif
}

ErrorPair ErrorPairHolder::get()
{
    std::lock_guard lock(mutex);
    return value;
}
}

}
