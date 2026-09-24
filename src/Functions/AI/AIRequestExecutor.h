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

    /// `ai_function_throw_on_error`. When disabled, a request that failed (after retries) does not
    /// throw, and the caller outputs a default value.
    bool throw_on_error = true;
};

/// Issue one chat-completion request on the AI request thread pool (`getAIRequestThreadPool`) and
/// check that the model produced a complete answer. Also handles retries with backoff, API-call
/// reservation, and token accounting.
std::future<std::optional<AIResponse>> submitAIRequest(
    std::shared_ptr<IAIProvider> provider, AIRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota);

/// The same, for one batch of texts sent to an embeddings endpoint.
std::future<std::optional<AIEmbeddingResponse>> submitAIEmbeddingRequest(
    std::shared_ptr<IAIProvider> provider, AIEmbeddingRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota);

}
