#pragma once

#include <Core/Types.h>
#include <Common/Exception.h>

#include <mutex>

namespace DB
{

/// Tracks AI-function quota usage for one query. A single instance is shared by every AI function
/// call in the query context (owned by the query `Context`) and updated concurrently from the
/// pipeline threads, so the counters are guarded by `mutex`. It is per query-execution context: a
/// distributed query has one per shard/fragment (each makes its own `Context`), so the limits bound
/// each server independently rather than the query globally.
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
    /// enforced separately by `recordApiCall`.
    bool checkQuotas();

    /// Count one outbound API call against the request quota, only while under the limit. Should be
    /// called before each provider request (including retries), so a misbehaving
    /// endpoint can't bypass `ai_function_max_api_calls_per_query`. Returns true if the call is within
    /// the limit (the caller may dispatch), false once the per-query API-call limit is reached or any
    /// quota is already exhausted (so no new request starts after the token budget is known-spent);
    /// throws when `throw_on_quota_exceeded`. Exact: `api_calls` never exceeds the limit.
    bool recordApiCall();

    /// Record token usage reported by a response, including one whose body was rejected as malformed: the
    /// provider billed for it either way. Kept separate from `recordApiCall`, which counts the request itself.
    void recordTokens(UInt64 in_tokens, UInt64 out_tokens);


private:
    const UInt64 max_input_tokens;
    const UInt64 max_output_tokens;
    const UInt64 max_api_calls;
    const bool throw_on_quota_exceeded;

    std::mutex mutex;
    bool quota_exceeded TSA_GUARDED_BY(mutex) = false;
    UInt64 input_tokens TSA_GUARDED_BY(mutex) = 0;
    UInt64 output_tokens TSA_GUARDED_BY(mutex) = 0;
    UInt64 api_calls TSA_GUARDED_BY(mutex) = 0;

    /// The sticky-flag + token-limit check, assuming `mutex` is held. Sets the sticky flag (or throws,
    /// per `throw_on_quota_exceeded`) when a token quota is met. Shared by `checkQuotas` and
    /// `recordApiCall` so a call is never started once a quota is known-exhausted.
    bool quotasExceededLocked() TSA_REQUIRES(mutex);
};

}
