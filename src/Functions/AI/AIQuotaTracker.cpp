#include <Functions/AI/AIQuotaTracker.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LIMIT_EXCEEDED;
}

bool AIQuotaTracker::checkBeforeDispatch(size_t estimated_text_bytes)
{
    if (quota_exceeded.load(MEMORY_ORDER))
        return false;

    /// Attempt to estimate the tokens we will use for the next API call
    if (max_input_tokens > 0 && estimated_text_bytes > 0)
    {
        /// 1 token is approx 4 characters aka 4 bytes. https://help.openai.com/en/articles/4936856-what-are-tokens-and-how-to-count-them
        UInt64 estimated_tokens = estimated_text_bytes / 4;
        UInt64 current = input_tokens.load(MEMORY_ORDER);
        if (current + estimated_tokens > max_input_tokens)
        {
            if (throw_on_quota_exceeded)
                throw Exception(
                    ErrorCodes::LIMIT_EXCEEDED,
                    "Estimated input tokens for next API call ({}) would exceed limit: {} tokens used, maximum: {}. "
                    "This is controlled by the 'ai_function_max_input_tokens_per_query' setting",
                    estimated_tokens,
                    current,
                    max_input_tokens);

            quota_exceeded.store(true, MEMORY_ORDER);
            return false;
        }
    }

    /// We cannot know output tokens in advance, so this quota is only enforced retroactively
    if (max_output_tokens > 0 && output_tokens.load(MEMORY_ORDER) > max_output_tokens)
    {
        if (throw_on_quota_exceeded)
            throw Exception(ErrorCodes::LIMIT_EXCEEDED,
                "Limit for AI output tokens exceeded: {} tokens generated, maximum: {}. "
                "This is controlled by the 'ai_function_max_output_tokens_per_query' setting",
                output_tokens.load(MEMORY_ORDER), max_output_tokens);
        quota_exceeded.store(true, MEMORY_ORDER);
        return false;
    }

    if (max_api_calls > 0)
    {
        UInt64 prev = api_calls.fetch_add(1, MEMORY_ORDER);
        if (prev + 1 > max_api_calls)
        {
            api_calls.fetch_sub(1, MEMORY_ORDER);
            if (throw_on_quota_exceeded)
                throw Exception(
                    ErrorCodes::LIMIT_EXCEEDED,
                    "Limit for AI API calls will be exceeded by next API call: {} calls made, maximum: {}. "
                    "This is controlled by the 'ai_function_max_api_calls_per_query' setting",
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
}

bool AIQuotaTracker::handleRowError()
{
    if (!throw_on_error)
    {
        rows_skipped.fetch_add(1, MEMORY_ORDER);
        return true;
    }
    return false;
}

}
