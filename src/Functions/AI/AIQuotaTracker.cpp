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
    if (quota_exceeded)
        return true;

    if (max_api_calls > 0 && api_calls >= max_api_calls)
    {
        if (throw_on_quota_exceeded)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "AI API call limit reached: {} calls made, maximum: {}. "
                "This is controlled by the 'ai_function_max_api_calls_per_query' setting",
                api_calls, max_api_calls);
        quota_exceeded = true;
        return true;
    }

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

void AIQuotaTracker::recordResponse(UInt64 in_tokens, UInt64 out_tokens)
{
    ++api_calls;
    input_tokens += in_tokens;
    output_tokens += out_tokens;
}

}
