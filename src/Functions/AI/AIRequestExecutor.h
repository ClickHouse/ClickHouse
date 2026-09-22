#pragma once

#include <Common/ThreadPool_fwd.h>
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

/// Server-wide component that issues AI provider requests on its own worker threads.
///
/// AI functions submit requests and wait for the returned futures; they never create threads of
/// their own. Retries with backoff, API-call reservation and token accounting happen here, so they
/// are uniform across functions, and the pool size caps how many provider requests the whole server
/// has in flight at once. Request batching, a result cache and a server-wide quota belong here too
/// and can be added behind the same submission API.
class AIRequestExecutor
{
public:
    AIRequestExecutor(size_t pool_size, size_t queue_size);
    ~AIRequestExecutor();

    /// Issue one chat-completion request and check that the model produced a complete answer.
    ///
    /// The future holds the response, or nothing when no usable response was produced: the
    /// per-query API-call quota was exhausted, or the request failed and `throw_on_error` is
    /// disabled. `get` rethrows when the request failed and `throw_on_error` is enabled, and always
    /// rethrows a quota exception, which `ai_function_throw_on_quota_exceeded` governs instead.
    std::future<std::optional<AIResponse>> submit(
        std::shared_ptr<IAIProvider> provider, AIRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota);

    /// The same, for one batch of texts sent to an embeddings endpoint.
    std::future<std::optional<AIEmbeddingResponse>> submitEmbedding(
        std::shared_ptr<IAIProvider> provider, AIEmbeddingRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota);

    /// Wait for the already submitted requests to finish. Called on server shutdown.
    void wait();

private:
    std::unique_ptr<ThreadPool> pool;
};

}
