#pragma once

#include <Functions/AI/IAIProvider.h>
#include <IO/ConnectionTimeouts.h>

#include <future>
#include <memory>
#include <optional>

namespace DB
{

class AIQuotaTracker;
using AIQuotaTrackerPtr = std::shared_ptr<AIQuotaTracker>;

/// How one AI provider request is executed. Resolved once per query from the `ai_function_*`
/// settings and shared by every request that query issues.
struct AIRequestPolicy
{
    ConnectionTimeouts timeouts;

    /// Number of extra attempts after a retriable failure, and the delay before the first of them.
    UInt64 max_retries = 0;
    UInt64 retry_initial_delay_ms = 0;

    /// `ai_function_throw_on_error`. When disabled, a request that failed for good produces no
    /// response instead of throwing, and the caller leaves the row at its default value.
    bool throw_on_error = true;
};

/// Issue one chat-completion request on the AI request thread pool (`getAIRequestThreadPool`) and
/// check that the model produced a complete answer.
///
/// Retries with backoff, API-call reservation and token accounting happen on the worker. The pool
/// size caps how many provider requests the whole server has in flight at once.
///
/// The future holds the response, or nothing when no usable response was produced: the per-query
/// API-call quota was exhausted, or the request failed and `throw_on_error` is disabled. `get`
/// rethrows when the request failed and `throw_on_error` is enabled, and always rethrows a quota
/// exception, which `ai_function_throw_on_quota_exceeded` governs instead.
std::future<std::optional<AIResponse>> submitAIRequest(
    std::shared_ptr<IAIProvider> provider, AIRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota);

/// The same, for one batch of texts sent to an embeddings endpoint.
std::future<std::optional<AIEmbeddingResponse>> submitAIEmbeddingRequest(
    std::shared_ptr<IAIProvider> provider, AIEmbeddingRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota);

}
