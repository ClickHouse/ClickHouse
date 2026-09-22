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

    /// `ai_function_throw_on_error`. When disabled, a request that failed (after retries) does not
    /// throw, and the caller outputs a default value.
    bool throw_on_error = true;
};

/// Server-wide component that issues AI provider requests on its own worker threads.
/// AI functions submit requests and wait for the returned futures. This class also handles
/// retries with backoff, API-call reservation, and token accounting.
class AIRequestExecutor
{
public:
    AIRequestExecutor(size_t pool_size, size_t queue_size);
    ~AIRequestExecutor();

    /// Issue one chat-completion request and check that the model produced a complete answer.
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
