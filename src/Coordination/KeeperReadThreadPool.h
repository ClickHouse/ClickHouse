#pragma once

#include <atomic>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <thread>

#include <Common/CacheLine.h>
#include <Common/ThreadPool.h>
#include <Coordination/CoordinationSettings.h>

namespace DB
{

/// Thread pool for executing (the parallelizable part of) read requests in keeper server.
/// Requests within the same batch can be executed in parallel.
/// At most one batch is being processed at any given time.
class KeeperReadThreadPool
{
public:
    using ExecuteRequestsFunction = std::function<void(size_t /*begin*/, size_t /*end*/)>;

    ~KeeperReadThreadPool();

    void execute(size_t count, const CoordinationSettings & coordination_settings, ExecuteRequestsFunction func);

    void shutdown();

private:
    /// Settings.
    size_t target_num_threads = 0;
    size_t chunk_size = 16;

    /// Threads.
    std::optional<ThreadPool> pool;
    std::mutex mutex;
    std::condition_variable wake_cv;
    std::condition_variable done_cv;
    size_t running_threads = 0;
    /// Whether the failure to start a worker was already logged (it is retried with every batch).
    bool logged_thread_start_failure = false;

    /// State of the current batch execution.
    /// Threads are woken up once at the start of the batch, then they can pick up requests
    /// lock-free by incrementing an atomic.
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> next_request_idx {};
    alignas(CH_CACHE_LINE_SIZE) std::atomic<size_t> busy_threads {};
    alignas(CH_CACHE_LINE_SIZE) size_t request_count = 0; // (alignment to add padding after previous atomic)
    size_t batch_idx = 0;
    std::exception_ptr first_exception;
    ExecuteRequestsFunction execute_requests;

    /// `seen_batch_idx` is the value of `batch_idx` when the worker was scheduled: the batches up to it are not for this worker.
    void threadFunction(size_t seen_batch_idx);
};

}
