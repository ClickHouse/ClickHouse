#include <Functions/AI/AIQuotaTracker.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

bool AIQuotaTracker::checkQuotas()
{
    if (quota_exceeded.load(std::memory_order_relaxed))
        return true;

    if (UInt64 in_tokens = input_tokens.load(std::memory_order_relaxed); max_input_tokens > 0 && in_tokens >= max_input_tokens)
    {
        if (throw_on_quota_exceeded)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "AI input token limit reached or exceeded: {} tokens consumed, maximum: {}. "
                "This is controlled by the 'ai_function_max_input_tokens_per_query' setting",
                in_tokens, max_input_tokens);
        quota_exceeded.store(true, std::memory_order_relaxed);
        return true;
    }

    if (UInt64 out_tokens = output_tokens.load(std::memory_order_relaxed); max_output_tokens > 0 && out_tokens >= max_output_tokens)
    {
        if (throw_on_quota_exceeded)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "AI output token limit reached or exceeded: {} tokens generated, maximum: {}. "
                "This is controlled by the 'ai_function_max_output_tokens_per_query' setting",
                out_tokens, max_output_tokens);
        quota_exceeded.store(true, std::memory_order_relaxed);
        return true;
    }

    return false;
}

bool AIQuotaTracker::tryReserveApiCall()
{
    if (max_api_calls == 0) /// 0 disables the limit.
        return true;

    /// Bounded atomic increment: claim a slot only while under the limit, so `api_calls` never
    /// exceeds `max_api_calls` no matter how many threads race here.
    UInt64 cur = api_calls.load(std::memory_order_relaxed);
    while (cur < max_api_calls)
    {
        if (api_calls.compare_exchange_weak(cur, cur + 1, std::memory_order_relaxed))
            return true;
    }

    if (throw_on_quota_exceeded)
        throw Exception(ErrorCodes::LIMIT_EXCEEDED,
            "AI API call limit reached: {} calls made, maximum: {}. "
            "This is controlled by the 'ai_function_max_api_calls_per_query' setting",
            cur, max_api_calls);
    quota_exceeded.store(true, std::memory_order_relaxed);
    return false;
}

void AIQuotaTracker::recordTokens(UInt64 in_tokens, UInt64 out_tokens)
{
    input_tokens.fetch_add(in_tokens, std::memory_order_relaxed);
    output_tokens.fetch_add(out_tokens, std::memory_order_relaxed);
}

}
