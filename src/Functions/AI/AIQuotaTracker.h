#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>

#include <atomic>

namespace DB
{

/// Tracks AI-function quota usage for one query. A single instance is shared by every AI function
/// call in the query context (owned by the query `Context`) and updated concurrently from the
/// pipeline threads, so the counters are `atomic`. It is per query-execution context: a distributed
/// query has one per shard/fragment (each makes its own `Context`), so the limits bound each server
/// independently rather than the query globally.
///
/// The API-call limit is a hard cap within a context, while token limits are best effort (we only
/// know the usage after the call returns).
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

    /// Check the token quotas (and the sticky exceeded flag). Returns true if a limit is met or
    /// exceeded, false otherwise. Should be called before issuing an API call. The API-call limit is
    /// enforced separately by `tryReserveApiCall`.
    bool checkQuotas();

    /// Atomically reserve one outbound API call against the request quota. Should be called before
    /// each provider request (including retries), so a misbehaving endpoint can't bypass
    /// `ai_function_max_api_calls_per_query`. Returns true if a slot was reserved (the caller may
    /// dispatch), false when the per-query limit is already reached; throws when
    /// `throw_on_quota_exceeded`.
    bool tryReserveApiCall();

    /// Record token usage on a successful response. Tokens are only billed by the provider when the call succeeds,
    /// so this is kept separate and called only after the response is parsed.
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
