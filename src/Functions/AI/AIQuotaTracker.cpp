#include <Functions/AI/AIQuotaTracker.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

void AIQuotaTracker::recordResponse(UInt64 in_tokens, UInt64 out_tokens)
{
    input_tokens += in_tokens;
    output_tokens += out_tokens;
    ++api_calls;
    checkQuotas();
}

void AIQuotaTracker::checkQuotas()
{
    if (quota_exceeded)
        return;

    if (max_input_tokens > 0 && input_tokens > max_input_tokens)
    {
        if (throw_on_quota_exceeded)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "AI input token limit exceeded: {} tokens consumed, maximum: {}. "
                "This is controlled by the 'ai_function_max_input_tokens_per_query' setting",
                input_tokens, max_input_tokens);
        quota_exceeded = true;
    }

    if (max_output_tokens > 0 && output_tokens > max_output_tokens)
    {
        if (throw_on_quota_exceeded)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "AI output token limit exceeded: {} tokens generated, maximum: {}. "
                "This is controlled by the 'ai_function_max_output_tokens_per_query' setting",
                output_tokens, max_output_tokens);
        quota_exceeded = true;
    }

    if (max_api_calls > 0 && api_calls > max_api_calls)
    {
        if (throw_on_quota_exceeded)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "AI API call limit exceeded: {} calls made, maximum: {}. "
                "This is controlled by the 'ai_function_max_api_calls_per_query' setting",
                api_calls, max_api_calls);
        quota_exceeded = true;
    }
}

}
