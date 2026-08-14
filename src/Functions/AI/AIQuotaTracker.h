#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>

#include <atomic>

namespace DB
{

/// Tracks AI-function quota usage for one query. A single instance is shared by every AI function
/// call in the query (owned by the query `Context`), so the counters are updated concurrently from
/// the pipeline threads and are `atomic`. `checkQuotas` and `recordAttempt` are separate, so at the
/// limit a few concurrent calls can each pass the check before recording (overshoot bounded by the
/// number of in-flight calls).
class AIQuotaTracker
{
public:
    AIQuotaTracker(
        UInt64 max_input_tokens_, UInt64 max_output_tokens_,
        UInt64 max_api_calls_, bool throw_on_quota_exceeded_)
        : max_input_tokens(max_input_tokens_)
        , max_output_tokens(max_output_tokens_)
        , max_api_calls(max_api_calls_)
        , throw_on_quota_exceeded(throw_on_quota_exceeded_)
    {}

    /// Check all quotas, return true if any quota is met or exceeded, false otherwise. Should be called before issuing API call.
    bool checkQuotas();

    /// Count one outbound API call against the request quota. Should be called before each provider request
    /// (including retries), so a misbehaving endpoint can't bypass `ai_function_max_api_calls_per_query`.
    void recordAttempt();

    /// Record token usage on a successful response. Tokens are only billed by the provider when the call succeeds,
    /// so this is kept separate from `recordAttempt` and called only after the response is parsed.
    void recordTokens(UInt64 in_tokens, UInt64 out_tokens);


private:
    const UInt64 max_input_tokens;
    const UInt64 max_output_tokens;
    const UInt64 max_api_calls;
    const bool throw_on_quota_exceeded;

    std::atomic<bool> quota_exceeded = false;
    std::atomic<UInt64> input_tokens = 0;
    std::atomic<UInt64> output_tokens = 0;
    std::atomic<UInt64> api_calls = 0;
};

}
