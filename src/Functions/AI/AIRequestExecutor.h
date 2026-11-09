#pragma once

#include <Functions/AI/IAIProvider.h>
#include <IO/ConnectionTimeouts.h>

#include <chrono>
#include <condition_variable>
#include <future>
#include <memory>
#include <mutex>
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

/// Completions of the requests submitted by one caller, to wait for any of them.
class AIRequestCompletions
{
public:
    /// Called after the request's future is ready.
    void notifyCompleted();

    /// Called when a request throws, before its future is ready.
    void markFailed();

    /// Waits until more than `seen` requests have completed, or `timeout` passes. Returns the number of completed requests.
    size_t waitForMoreThan(size_t seen, std::chrono::milliseconds timeout);

    /// Whether a request has thrown.
    bool hasFailed();

private:
    std::mutex mutex;
    std::condition_variable completed_cv;
    size_t completed = 0;
    bool failed = false;
};

using AIRequestCompletionsPtr = std::shared_ptr<AIRequestCompletions>;

/// Issue one chat-completion request on the AI request thread pool (`getAIRequestThreadPool`) and
/// check that the model produced a complete answer. Also handles retries with backoff, API-call
/// reservation, and token accounting. `completions` is notified when the request completes.
std::future<std::optional<AIResponse>> submitAIRequest(
    std::shared_ptr<IAIProvider> provider,
    AIChatRequest request,
    AIRequestPolicy policy,
    AIQuotaTrackerPtr quota,
    AIRequestCompletionsPtr completions);

/// The same, for one batch of texts sent to an embeddings endpoint.
std::future<std::optional<AIEmbeddingResponse>> submitAIRequest(
    std::shared_ptr<IAIProvider> provider,
    AIEmbeddingRequest request,
    AIRequestPolicy policy,
    AIQuotaTrackerPtr quota,
    AIRequestCompletionsPtr completions);

}
