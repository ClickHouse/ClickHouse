#include <Functions/AI/AIQuotaTracker.h>
#include <Interpreters/ProcessList.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
    extern const int TIMEOUT_EXCEEDED;
}

bool AIQuotaTracker::quotasExceededLocked()
{
    if (quota_exceeded)
        return true;

    if (max_input_tokens > 0 && input_tokens >= max_input_tokens)
    {
        if (throw_on_quota_exceeded)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "AI input token limit reached or exceeded: {} tokens consumed, maximum: {}. "
                "This is controlled by the 'ai_function_max_input_tokens_per_query' setting",
                input_tokens, max_input_tokens);
        quota_exceeded = true;
        return true;
    }

    if (max_output_tokens > 0 && output_tokens >= max_output_tokens)
    {
        if (throw_on_quota_exceeded)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "AI output token limit reached or exceeded: {} tokens generated, maximum: {}. "
                "This is controlled by the 'ai_function_max_output_tokens_per_query' setting",
                output_tokens, max_output_tokens);
        quota_exceeded = true;
        return true;
    }

    return false;
}

void AIQuotaTracker::throwIfCancelled() const
{
    auto status = query_status.lock();
    if (!status)
        return;

    /// `checkTimeLimit` throws on `KILL QUERY` and when `max_execution_time` is exceeded.
    /// Under `timeout_overflow_mode = 'break'`, it returns false instead of throwing.
    /// A function cannot return fewer rows than it received, so throw in that case too.
    if (!status->checkTimeLimit())
        throw Exception(ErrorCodes::TIMEOUT_EXCEEDED, "Timeout exceeded: elapsed time limit reached in an AI function");
}

bool AIQuotaTracker::checkQuotas()
{
    throwIfCancelled();

    std::lock_guard lock(mutex);
    return quotasExceededLocked();
}

bool AIQuotaTracker::recordApiCall()
{
    throwIfCancelled();

    std::lock_guard lock(mutex);

    /// Don't start a new request once any quota is known-exhausted (e.g. another thread's response
    /// just pushed the token budget over), even though the API-call count itself is still under its
    /// own limit. This keeps token overshoot to the requests already in flight at that moment.
    if (quotasExceededLocked())
        return false;

    if (max_api_calls == 0) /// 0 disables the API-call limit.
        return true;

    if (api_calls < max_api_calls)
    {
        ++api_calls;
        return true;
    }

    if (throw_on_quota_exceeded)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "AI API call limit reached: {} calls made, maximum: {}. "
            "This is controlled by the 'ai_function_max_api_calls_per_query' setting",
            api_calls, max_api_calls);

    quota_exceeded = true;
    return false;
}

AIQuotaTracker::RequestSlot AIQuotaTracker::acquireRequestSlot()
{
    std::unique_lock lock(mutex);
    request_slot_released.wait(lock, [this]() TSA_REQUIRES(mutex) { return requests_in_flight < max_concurrent_requests; });
    /// `std::unique_lock` is not a thread safety analysis capability, but `mutex` is held here.
    ++TSA_SUPPRESS_WARNING_FOR_WRITE(requests_in_flight);
    return RequestSlot(shared_from_this());
}

void AIQuotaTracker::releaseRequestSlot()
{
    {
        std::lock_guard lock(mutex);
        --requests_in_flight;
    }
    request_slot_released.notify_one();
}

void AIQuotaTracker::recordTokens(UInt64 in_tokens, UInt64 out_tokens)
{
    std::lock_guard lock(mutex);
    input_tokens += in_tokens;
    output_tokens += out_tokens;
}

}
