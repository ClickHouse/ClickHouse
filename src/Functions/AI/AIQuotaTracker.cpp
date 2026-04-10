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

bool AIQuotaTracker::checkBeforeDispatch(UInt64 estimated_input_tokens)
{
    if (quota_exceeded.load(memory_order))
        return false;

    if (max_input_tokens > 0 && estimated_input_tokens > 0)
    {
        UInt64 prev = input_tokens.fetch_add(estimated_input_tokens, memory_order);
        if (prev + estimated_input_tokens > max_input_tokens)
        {
            input_tokens.fetch_sub(estimated_input_tokens, memory_order);
            if (!isGracefulQuotaMode(on_quota_exceeded))
                throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                    "Limit for AI input tokens exceeded: {} tokens used, maximum: {}. "
                    "This is controlled by the 'ai_max_input_tokens_per_query' setting",
                    prev, max_input_tokens);
            quota_exceeded.store(true, memory_order);
            return false;
        }
    }

    if (max_api_calls > 0)
    {
        UInt64 prev = api_calls.fetch_add(1, memory_order);
        if (prev + 1 > max_api_calls)
        {
            api_calls.fetch_sub(1, memory_order);
            if (!isGracefulQuotaMode(on_quota_exceeded))
                throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                    "Limit for AI API calls exceeded: {} calls made, maximum: {}. "
                    "This is controlled by the 'ai_max_api_calls_per_query' setting",
                    prev, max_api_calls);
            quota_exceeded.store(true, memory_order);
            return false;
        }
    }

    return true;
}

void AIQuotaTracker::recordResponse(UInt64 in_tokens, UInt64 out_tokens)
{
    input_tokens.fetch_add(in_tokens, memory_order);
    output_tokens.fetch_add(out_tokens, memory_order);

    if (max_output_tokens > 0 && output_tokens.load(memory_order) > max_output_tokens)
    {
        if (!isGracefulQuotaMode(on_quota_exceeded))
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "Limit for AI output tokens exceeded: {} tokens generated, maximum: {}. "
                "This is controlled by the 'ai_max_output_tokens_per_query' setting",
                output_tokens.load(memory_order), max_output_tokens);
        quota_exceeded.store(true, memory_order);
    }
}

bool AIQuotaTracker::handleRowError()
{
    if (on_error == "default")
    {
        rows_skipped.fetch_add(1, memory_order);
        return true;
    }
    return false;
}

}
