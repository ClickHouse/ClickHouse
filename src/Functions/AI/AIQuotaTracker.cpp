#include <Functions/AI/AIQuotaTracker.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
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

bool AIQuotaTracker::checkQuotas()
{
    std::lock_guard lock(mutex);
    return quotasExceededLocked();
}

bool AIQuotaTracker::recordApiCall()
{
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

void AIQuotaTracker::recordTokens(UInt64 in_tokens, UInt64 out_tokens)
{
    std::lock_guard lock(mutex);
    input_tokens += in_tokens;
    output_tokens += out_tokens;
}

}
