#include <Functions/AI/AIRequestExecutor.h>

#include <Functions/AI/AIQuotaTracker.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/NetException.h>
#include <Common/ProfileEvents.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <IO/HTTPCommon.h>
#include <base/scope_guard.h>

#include <Poco/Net/NetException.h>

#include <algorithm>
#include <chrono>
#include <thread>

namespace CurrentMetrics
{
    extern const Metric AIRequestThreads;
    extern const Metric AIRequestThreadsActive;
    extern const Metric AIRequestThreadsScheduled;
}

namespace ProfileEvents
{
    extern const Event AIInputTokens;
    extern const Event AIOutputTokens;
    extern const Event AIAPICalls;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int AI_PROVIDER_RESPONSE_TRUNCATED;
    extern const int AI_PROVIDER_RESPONSE_INCOMPLETE;
}

namespace
{

/// Exponential backoff delay capped at one minute, so adversarial values of
/// `ai_function_retry_initial_delay_ms` or `ai_function_max_retries` cannot produce a multi-hour
/// sleep or overflow `std::chrono::milliseconds`.
UInt64 computeRetryBackoffMs(UInt64 initial_delay_ms, UInt64 attempt)
{
    constexpr UInt64 max_retry_delay_ms = 60'000;
    UInt64 delay_ms = std::min(initial_delay_ms, max_retry_delay_ms);
    for (UInt64 i = 0; i < attempt && delay_ms < max_retry_delay_ms; ++i)
        delay_ms = std::min(delay_ms * 2, max_retry_delay_ms);
    return delay_ms;
}

/// Whether a failed provider request should be retried: transient network failures and
/// transient/server-side HTTP responses are retriable, deterministic argument/usage errors are not.
bool isRetriableProviderError(std::exception_ptr exception)
{
    try
    {
        std::rethrow_exception(exception);
    }
    catch (const AIProviderHTTPException & http_exception)
    {
        return isRetriableHTTPError(http_exception.getHTTPStatus());
    }
    catch (const NetException &)
    {
        /// ClickHouse-level network error (e.g. a DNS failure raised by the HTTP connection pool).
        return true;
    }
    catch (const Poco::Net::NetException &)
    {
        /// Connection refused/reset, TLS connect failure, or an unreachable advertised address.
        return true;
    }
    catch (const Poco::TimeoutException &)
    {
        /// Connect or receive timeout.
        return true;
    }
    catch (const Poco::IOException & io_exception)
    {
        /// Write-side transient I/O failure, e.g. a broken pipe (`EPIPE`) when the peer resets the
        /// connection mid-request. Out-of-file-descriptors (`EMFILE`) is not retriable.
        return io_exception.code() != POCO_EMFILE;
    }
    catch (...)
    {
        /// Ok: any other exception is a deterministic and non-retrieable error, e.g. a malformed
        /// provider response, bad configuration, JSON parse failure, etc.
        return false;
    }
}

/// Reject a response the model did not finish. Throws a plain `Exception`, which
/// `isRetriableProviderError` classifies as non-retriable.
void checkResponseIsComplete(const AIResponse & response)
{
    /// `raw_finish_reason` is provider-controlled text; sanitize control characters before
    /// interpolating it into an exception message that reaches the logs and `system.query_log`.
    const String safe_finish_reason = sanitizeForLog(response.raw_finish_reason);

    switch (response.finish_reason)
    {
        case FinishReason::Complete:
        case FinishReason::Unknown: /// Don't throw on Unknown, could be new valid reason in new API version
            return;
        case FinishReason::Truncated:
            /// Differentiate between model hitting our output cap and exhausting its context window
            throw Exception(
                ErrorCodes::AI_PROVIDER_RESPONSE_TRUNCATED,
                "AI provider returned a truncated response (finish_reason='{}'): {}",
                safe_finish_reason,
                response.raw_finish_reason == "model_context_window_exceeded"
                    ? "the model ran out of context window before completing its answer. "
                      "Reduce the input or use a model with a larger context window."
                    : "the model hit the output token limit before completing its answer. "
                      "Increase max_tokens or reduce the input.");
        case FinishReason::ContentFilter:
            throw Exception(
                ErrorCodes::AI_PROVIDER_RESPONSE_INCOMPLETE,
                "AI provider withheld or filtered the response (finish_reason='{}'): the returned answer "
                "is incomplete.",
                safe_finish_reason);
        case FinishReason::RequiresAction:
            throw Exception(
                ErrorCodes::AI_PROVIDER_RESPONSE_INCOMPLETE,
                "AI provider stopped expecting further caller action (finish_reason='{}') instead of "
                "returning a completed answer.",
                safe_finish_reason);
    }
}

/// The attempt loop every request kind shares: reserve an API-call slot, run one attempt, retry a
/// transient failure after a backoff. Returns nothing when the API-call quota is exhausted, or when
/// the request failed (after all retries) and `throw_on_error` is disabled.
template <typename Response, typename Attempt>
std::optional<Response> runRequest(const AIRequestPolicy & policy, AIQuotaTracker & quota, Attempt && attempt_fn)
{
    for (UInt64 attempt = 0; attempt <= policy.max_retries; ++attempt)
    {
        /// Reserve an API-call slot before each request; this also performs a quota check.
        if (!quota.recordApiCall())
            return {};

        try
        {
            /// Count the call before issuing it, so a failed request is still counted.
            ProfileEvents::increment(ProfileEvents::AIAPICalls);
            return attempt_fn();
        }
        catch (...)
        {
            if (attempt < policy.max_retries && isRetriableProviderError(std::current_exception()))
            {
                std::this_thread::sleep_for(std::chrono::milliseconds(computeRetryBackoffMs(policy.retry_initial_delay_ms, attempt)));
                continue;
            }

            if (!policy.throw_on_error)
                return {};

            throw;
        }
    }

    return {};
}

}

AIRequestExecutor::AIRequestExecutor(size_t pool_size, size_t queue_size)
    : pool(std::make_unique<ThreadPool>(
        CurrentMetrics::AIRequestThreads,
        CurrentMetrics::AIRequestThreadsActive,
        CurrentMetrics::AIRequestThreadsScheduled,
        pool_size,
        /*max_free_threads=*/ 0,
        queue_size))
{
    /// A worker spends its life blocked on one HTTP request, so idle ones are released rather than
    /// parked: creating a thread costs nothing next to the request it is about to wait for, and AI
    /// traffic is bursty enough that keeping `pool_size` threads alive would hold that many global
    /// thread pool slots for the rest of the server's life.
}

AIRequestExecutor::~AIRequestExecutor() = default;

void AIRequestExecutor::wait()
{
    pool->wait();
}

std::future<std::optional<AIResponse>> AIRequestExecutor::submit(
    std::shared_ptr<IAIProvider> provider, AIRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota)
{
    /// Everything the worker touches is owned by the task, so a caller that stops waiting on the
    /// future (e.g. because an earlier request of the same block threw) leaves nothing dangling.
    auto task = [my_provider = std::move(provider),
                 my_request = std::move(request),
                 my_policy = std::move(policy),
                 my_quota = std::move(quota)]
    {
        return runRequest<AIResponse>(my_policy, *my_quota, [&]
        {
            AIResponse response;

            /// The provider fills the token counts before it validates the payload, so a request that
            /// ends in an exception still reports the usage the provider billed for.
            SCOPE_EXIT({
                my_quota->recordTokens(response.input_tokens, response.output_tokens);
                ProfileEvents::increment(ProfileEvents::AIInputTokens, response.input_tokens);
                ProfileEvents::increment(ProfileEvents::AIOutputTokens, response.output_tokens);
            });

            my_provider->call(my_request, my_policy.timeouts, response);
            checkResponseIsComplete(response);
            return response;
        });
    };

    return scheduleFromThreadPoolUnsafe<std::optional<AIResponse>>(std::move(task), *pool, ThreadName::AI_REQUEST);
}

std::future<std::optional<AIEmbeddingResponse>> AIRequestExecutor::submitEmbedding(
    std::shared_ptr<IAIProvider> provider, AIEmbeddingRequest request, AIRequestPolicy policy, AIQuotaTrackerPtr quota)
{
    auto task = [my_provider = std::move(provider),
                 my_request = std::move(request),
                 my_policy = std::move(policy),
                 my_quota = std::move(quota)]
    {
        return runRequest<AIEmbeddingResponse>(my_policy, *my_quota, [&]
        {
            AIEmbeddingResponse response;

            SCOPE_EXIT({
                my_quota->recordTokens(response.input_tokens, 0 /*output_tokens*/);
                ProfileEvents::increment(ProfileEvents::AIInputTokens, response.input_tokens);
            });

            my_provider->embed(my_request, my_policy.timeouts, response);
            return response;
        });
    };

    return scheduleFromThreadPoolUnsafe<std::optional<AIEmbeddingResponse>>(
        std::move(task), *pool, ThreadName::AI_REQUEST);
}

}
