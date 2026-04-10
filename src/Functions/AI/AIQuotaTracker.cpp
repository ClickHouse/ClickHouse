#include <Functions/AI/AIQuotaTracker.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

static bool isGracefulQuotaMode(const String & mode)
{
    return mode == "default";
}

bool AIQuotaTracker::checkBeforeDispatch()
{
    if (quota_exceeded.load(MEMORY_ORDER))
        return false;

    if (max_api_calls > 0)
    {
        UInt64 prev = api_calls.fetch_add(1, MEMORY_ORDER);
        if (prev + 1 > max_api_calls)
        {
            api_calls.fetch_sub(1, MEMORY_ORDER);
            if (!isGracefulQuotaMode(on_quota_exceeded))
                throw Exception(
                    ErrorCodes::LIMIT_EXCEEDED,
                    "Limit for AI API calls will be exceeded by next API call: {} calls made, maximum: {}. "
                    "This is controlled by the 'ai_max_api_calls_per_query' setting",
                    prev,
                    max_api_calls);

            quota_exceeded.store(true, MEMORY_ORDER);
            return false;
        }
    }

    return true;
}

void AIQuotaTracker::recordResponse(UInt64 in_tokens, UInt64 out_tokens)
{
    input_tokens.fetch_add(in_tokens, MEMORY_ORDER);
    output_tokens.fetch_add(out_tokens, MEMORY_ORDER);

    if (max_input_tokens > 0 && input_tokens.load(MEMORY_ORDER) > max_input_tokens)
    {
        if (!isGracefulQuotaMode(on_quota_exceeded))
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "Limit for AI input tokens exceeded: {} tokens consumed, maximum: {}. "
                "This is controlled by the 'ai_max_input_tokens_per_query' setting",
                input_tokens.load(MEMORY_ORDER), max_input_tokens);
        quota_exceeded.store(true, MEMORY_ORDER);
    }

    if (max_output_tokens > 0 && output_tokens.load(MEMORY_ORDER) > max_output_tokens)
    {
        if (!isGracefulQuotaMode(on_quota_exceeded))
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "Limit for AI output tokens exceeded: {} tokens generated, maximum: {}. "
                "This is controlled by the 'ai_max_output_tokens_per_query' setting",
                output_tokens.load(MEMORY_ORDER), max_output_tokens);
        quota_exceeded.store(true, MEMORY_ORDER);
    }
}

bool AIQuotaTracker::handleRowError()
{
    if (on_error == "default")
    {
        rows_skipped.fetch_add(1, MEMORY_ORDER);
        return true;
    }
    return false;
}

}
