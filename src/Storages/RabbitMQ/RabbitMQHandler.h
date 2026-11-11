#pragma once

#include <atomic>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <thread>
#include <vector>
#include <amqpcpp.h>
#include <amqpcpp/linux_tcp.h>
#include <base/types.h>
#include <amqpcpp/libuv.h>
#include <Common/ThreadPool_fwd.h>
#include <Poco/Logger.h>

namespace DB
{

/// Default timeout for any blocking AMQP wait that relies on broker callbacks to stop the loop
/// (table setup, queue cleanup, producer channel close). Bounds how long
/// DROP TABLE / INSERT shutdown / server shutdown can block when the broker connection is dead.
static const uint64_t BLOCKING_LOOP_TIMEOUT_MS = 30000;

/// Timeout for waiting on the consumer channels to close before a dropped table deletes its
/// queues. Far below BLOCKING_LOOP_TIMEOUT_MS because the wait needs a single channel.close round
/// trip, while a channel awaiting a reply that never comes queues its close and would make every
/// DROP TABLE pay the timeout in full. Giving up restores the behaviour of not waiting at all.
static const uint64_t CONSUMER_CHANNEL_CLOSE_TIMEOUT_MS = 1000;

using ChannelPtr = std::unique_ptr<AMQP::TcpChannel>;

/** Owns the thread that runs the libuv loop of one connection while a table streams. AMQP-CPP is not
  * thread safe: calls made through run() go to that thread while it runs and execute inline otherwise.
  * Table setup and cleanup, and the producer, drive the loop on their own thread while no loop thread runs.
  */
class RabbitMQHandler : public AMQP::LibUvHandler
{

public:
    RabbitMQHandler(uv_loop_t * loop_, LoggerPtr log_);
    ~RabbitMQHandler() override;

    void onError(AMQP::TcpConnection * connection, const char * message) override;
    void onReady(AMQP::TcpConnection * connection) override;

    /// Start a thread that blocks in uv_run() until stopLoop(). Idempotent; replaces a thread that died.
    void startLoop();
    /// Stop that thread and join it. Tasks still queued run before it exits.
    void stopLoop();
    bool loopRunning() const;

    /// Execute `f` on the loop thread and return its result (exceptions propagate). Runs inline, under
    /// `exec_mutex`, when no loop thread runs or when called on it.
    template <typename F>
    std::invoke_result_t<F> run(F && f)
    {
        if (loop_thread_id.load() == std::this_thread::get_id())
            return f();

        std::packaged_task<std::invoke_result_t<F>()> task(std::forward<F>(f));
        auto future = task.get_future();
        {
            std::lock_guard exec_lock(exec_mutex);
            if (!post([&task] { task(); }))
                task();
        }
        return future.get();
    }

    /// The methods below drive the loop on the calling thread.

    /// Process pending events without blocking. Does nothing while a loop thread runs.
    int iterateLoop();

    /// Run until stopBlockingLoop() or timeout_ms milliseconds. Requires that no loop thread runs.
    /// Returns true if the loop exited naturally (callbacks fired), false if it timed out.
    bool startBlockingLoopWithTimeout(uint64_t timeout_ms);
    void stopBlockingLoop();

    bool connectionRunning() const { return connection_running.load(); }

    uv_loop_t * getLoop() const { return loop; }

private:
    static void onAsync(uv_async_t * handle);
    /// Queue a task for the loop thread; false once the loop thread no longer accepts tasks.
    bool post(std::function<void()> task);
    void runLoop(std::promise<void> & started);
    void executeTasks();
    bool stopRequested() const;
    void assertNoLoopThread() const;

    uv_loop_t * loop;
    LoggerPtr log;
    uv_async_t async{};

    std::atomic<bool> connection_running{false};

    mutable std::mutex mutex;
    std::vector<std::function<void()>> tasks;
    /// Tasks are accepted while set: from startLoop() until the loop thread exits.
    bool loop_running = false;
    bool stop_requested = false;

    /// Held by whoever touches AMQP-CPP outside the loop thread, and by startLoop() and the exiting loop thread
    /// while they hand the connection over. Recursive: callbacks run inline call run() again.
    std::recursive_mutex exec_mutex;

    std::mutex thread_mutex;
    std::unique_ptr<ThreadFromGlobalPool> loop_thread;
    std::atomic<std::thread::id> loop_thread_id;
};

using RabbitMQHandlerPtr = std::shared_ptr<RabbitMQHandler>;

}
