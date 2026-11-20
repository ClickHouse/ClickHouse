#pragma once

#include <Functions/AI/AIQuotaTracker.h>
#include <Functions/AI/IAIProvider.h>
#include <IO/ConnectionTimeouts.h>

#include <chrono>
#include <future>
#include <memory>
#include <optional>

namespace DB
{

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
    std::shared_ptr<IAIProvider> provider, AIChatRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota);

/// The same, for one batch of texts sent to an embeddings endpoint.
std::future<std::optional<AIEmbeddingResponse>> submitAIRequest(
    std::shared_ptr<IAIProvider> provider, AIEmbeddingRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota);

/// The same, for one batch of documents sent to a reranking endpoint.
std::future<std::optional<AIRerankResponse>> submitAIRequest(
    std::shared_ptr<IAIProvider> provider, AIRerankRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota);

/// How often a wait for a submitted request checks whether the query was cancelled.
constexpr auto ai_request_cancellation_check_interval = std::chrono::milliseconds(100);

/// Waits for a submitted request. Throws once the query is killed or exceeds its time limit, so a
/// request queued behind other queries' requests, or stalled at the provider, does not hold the query.
/// The task owns everything it uses, so it may outlive the caller after the throw.
template <typename Response>
std::optional<Response> waitForAIRequest(std::future<std::optional<Response>> & request, const AIQuotaTracker & quota)
{
    while (request.wait_for(ai_request_cancellation_check_interval) != std::future_status::ready)
        quota.throwIfCancelled();
    return request.get();
}

}
