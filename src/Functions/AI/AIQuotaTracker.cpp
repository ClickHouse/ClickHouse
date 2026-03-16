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
    return mode == "null";
}

AIQuotaTracker::AIQuotaTracker(
    UInt64 max_rows_,
    UInt64 max_input_tokens_,
    UInt64 max_output_tokens_,
    UInt64 max_api_calls_,
    const String & on_quota_exceeded_,
    const String & on_error_)
    : max_rows(max_rows_)
    , max_input_tokens(max_input_tokens_)
    , max_output_tokens(max_output_tokens_)
    , max_api_calls(max_api_calls_)
    , on_quota_exceeded(on_quota_exceeded_)
    , on_error(on_error_)
{}

bool AIQuotaTracker::checkBeforeDispatch(UInt64 estimated_input_tokens, UInt64 batch_rows)
{
    if (quota_exceeded.load(std::memory_order_relaxed))
        return false;

    if (max_rows > 0)
    {
        UInt64 prev = rows_processed.fetch_add(batch_rows, std::memory_order_relaxed);
        if (prev + batch_rows > max_rows)
        {
            rows_processed.fetch_sub(batch_rows, std::memory_order_relaxed);
            if (!isGracefulQuotaMode(on_quota_exceeded))
                throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                    "Limit for AI rows exceeded: {} rows processed, maximum: {}. "
                    "This is controlled by the 'llm_max_rows_per_query' setting. "
                    "Set 'llm_on_quota_exceeded' to 'null' to return NULL for remaining rows instead of failing",
                    prev, max_rows);
            quota_exceeded.store(true, std::memory_order_relaxed);
            return false;
        }
    }

    if (max_input_tokens > 0 && estimated_input_tokens > 0)
    {
        UInt64 prev = input_tokens.fetch_add(estimated_input_tokens, std::memory_order_relaxed);
        if (prev + estimated_input_tokens > max_input_tokens)
        {
            input_tokens.fetch_sub(estimated_input_tokens, std::memory_order_relaxed);
            if (!isGracefulQuotaMode(on_quota_exceeded))
                throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                    "Limit for LLM input tokens exceeded: {} tokens used, maximum: {}. "
                    "This is controlled by the 'llm_max_input_tokens_per_query' setting",
                    prev, max_input_tokens);
            quota_exceeded.store(true, std::memory_order_relaxed);
            return false;
        }
    }

    if (max_api_calls > 0)
    {
        UInt64 prev = api_calls.fetch_add(1, std::memory_order_relaxed);
        if (prev + 1 > max_api_calls)
        {
            api_calls.fetch_sub(1, std::memory_order_relaxed);
            if (!isGracefulQuotaMode(on_quota_exceeded))
                throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                    "Limit for LLM API calls exceeded: {} calls made, maximum: {}. "
                    "This is controlled by the 'llm_max_api_calls_per_query' setting",
                    prev, max_api_calls);
            quota_exceeded.store(true, std::memory_order_relaxed);
            return false;
        }
    }

    return true;
}

void AIQuotaTracker::recordResponse(UInt64 in_tokens, UInt64 out_tokens)
{
    input_tokens.fetch_add(in_tokens, std::memory_order_relaxed);
    output_tokens.fetch_add(out_tokens, std::memory_order_relaxed);

    if (max_output_tokens > 0 && output_tokens.load(std::memory_order_relaxed) > max_output_tokens)
    {
        if (!isGracefulQuotaMode(on_quota_exceeded))
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "Limit for LLM output tokens exceeded: {} tokens generated, maximum: {}. "
                "This is controlled by the 'llm_max_output_tokens_per_query' setting",
                output_tokens.load(std::memory_order_relaxed), max_output_tokens);
        quota_exceeded.store(true, std::memory_order_relaxed);
    }
}

bool AIQuotaTracker::handleRowError()
{
    if (on_error == "null")
    {
        rows_skipped.fetch_add(1, std::memory_order_relaxed);
        return true;
    }
    return false;
}

}
