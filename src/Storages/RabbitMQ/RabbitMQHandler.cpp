#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Storages/RabbitMQ/RabbitMQHandler.h>
#include <base/scope_guard.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int SYSTEM_ERROR;
    extern const int LOGICAL_ERROR;
}

/* The object of this class is shared between concurrent consumers (who share the same connection == share the same
 * event loop and handler).
 */
RabbitMQHandler::RabbitMQHandler(uv_loop_t * loop_, LoggerPtr log_) :
    AMQP::LibUvHandler(loop_),
    loop(loop_),
    log(log_)
{
    if (int error = uv_async_init(loop, &async, onAsync))
        throw Exception(ErrorCodes::SYSTEM_ERROR, "Cannot create event loop scheduler: {}", uv_strerror(error));
    async.data = this;
}

RabbitMQHandler::~RabbitMQHandler()
{
    stopLoop();
    /// Close the async handle and let its close callback run, otherwise uv_loop_close() reports a busy loop.
    uv_close(reinterpret_cast<uv_handle_t *>(&async), nullptr);
    uv_run(loop, UV_RUN_NOWAIT);
}

///Method that is called when the connection ends up in an error state.
void RabbitMQHandler::onError(AMQP::TcpConnection * /* connection */, const char * message)
{
    LOG_ERROR(log, "Library error report: {}", message);
    connection_running.store(false);
}

void RabbitMQHandler::onReady(AMQP::TcpConnection * /* connection */)
{
    LOG_TRACE(log, "Connection is ready");
    connection_running.store(true);
}

bool RabbitMQHandler::loopRunning() const
{
    std::lock_guard lock(mutex);
    return loop_running;
}

bool RabbitMQHandler::stopRequested() const
{
    std::lock_guard lock(mutex);
    return stop_requested;
}

void RabbitMQHandler::startLoop()
{
    std::lock_guard thread_lock(thread_mutex);
    if (loop_thread)
    {
        if (loopRunning())
            return;
        /// The thread died from an exception (see runLoop): replace it.
        loop_thread->join();
        loop_thread.reset();
    }

    /// Wait for inline callers to finish: from here on only the loop thread may touch AMQP-CPP.
    std::lock_guard exec_lock(exec_mutex);
    {
        std::lock_guard lock(mutex);
        loop_running = true;
        stop_requested = false;
    }
    try
    {
        /// The pool can fail to set up the thread before runLoop() is entered (e.g. on allocation); then the
        /// promise is destroyed unfulfilled and get() throws.
        auto started = std::make_shared<std::promise<void>>();
        auto started_future = started->get_future();
        loop_thread = std::make_unique<ThreadFromGlobalPool>([this, started] { runLoop(*started); });
        started_future.get();
    }
    catch (...)
    {
        if (loop_thread)
        {
            loop_thread->join();
            loop_thread.reset();
        }
        {
            std::lock_guard lock(mutex);
            loop_running = false;
        }
        /// Tasks posted meanwhile would otherwise wait for a thread that never comes.
        executeTasks();
        throw;
    }
}

void RabbitMQHandler::stopLoop()
{
    std::lock_guard thread_lock(thread_mutex);
    if (!loop_thread)
        return;

    LOG_DEBUG(log, "Stopping background loop.");
    {
        std::lock_guard lock(mutex);
        stop_requested = true;
        uv_async_send(&async);
    }
    loop_thread->join();
    loop_thread.reset();

    /// Otherwise a wake-up still pending in the loop would make the next inline uv_run() stop at once.
    std::lock_guard lock(mutex);
    stop_requested = false;
}

void RabbitMQHandler::runLoop(std::promise<void> & started)
{
    loop_thread_id = std::this_thread::get_id();
    SCOPE_EXIT({
        /// Hand over to inline callers only once the tasks posted before the flag flipped have run.
        std::lock_guard exec_lock(exec_mutex);
        {
            std::lock_guard lock(mutex);
            loop_running = false;
        }
        executeTasks();
        loop_thread_id = std::thread::id{};
        LOG_DEBUG(log, "Background loop ended");
    });
    started.set_value();

    try
    {
        /// An exception must not unwind through uv_run: libuv keeps pointers into the frames of a running poll. The
        /// allocations of AMQP-CPP in the callbacks would otherwise throw on the memory limit.
        LockMemoryExceptionInThread memory_tracker_lock(VariableContext::Global);
        setThreadName(ThreadName::RABBITMQ_LOOP);
        LOG_DEBUG(log, "Background loop started");
        /// The async handle keeps the loop alive. A uv_stop() from a callback (stopBlockingLoop) is
        /// not a reason to exit, only a stop request is.
        while (!stopRequested())
            uv_run(loop, UV_RUN_DEFAULT);
    }
    catch (...)
    {
        /// Last line of defence: the connection may be inconsistent, so treat it as broken.
        /// run() executes inline until startLoop() replaces this thread.
        connection_running.store(false);
        tryLogCurrentException(log, "Event loop thread terminated");
    }
}

void RabbitMQHandler::onAsync(uv_async_t * handle)
{
    auto * self = static_cast<RabbitMQHandler *>(handle->data);
    self->executeTasks();
    if (self->stopRequested())
        uv_stop(self->loop);
}

bool RabbitMQHandler::post(std::function<void()> task)
{
    std::lock_guard lock(mutex);
    if (!loop_running)
        return false;

    tasks.push_back(std::move(task));
    uv_async_send(&async);
    return true;
}

void RabbitMQHandler::executeTasks()
{
    std::vector<std::function<void()>> batch;
    {
        std::lock_guard lock(mutex);
        batch.swap(tasks);
    }
    for (auto & task : batch)
        task();
}

void RabbitMQHandler::assertNoLoopThread() const
{
    if (loopRunning())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The event loop is already driven by its own thread");
}

int RabbitMQHandler::iterateLoop()
{
    std::lock_guard exec_lock(exec_mutex);
    if (loopRunning())
        return 0;
    return uv_run(loop, UV_RUN_NOWAIT);
}

bool RabbitMQHandler::startBlockingLoopWithTimeout(uint64_t timeout_ms)
{
    std::lock_guard exec_lock(exec_mutex);
    assertNoLoopThread();
    LOG_DEBUG(log, "Started blocking loop with {}ms timeout.", timeout_ms);

    bool timed_out = false;

    /// uv_timer_start() derives the deadline from the loop's cached time, which is only refreshed
    /// while the loop runs. Between two blocking waits the loop is idle, so the cached value can be
    /// arbitrarily far in the past and the timer would then already be overdue when it is armed.
    uv_update_time(loop);

    uv_timer_t timer;
    uv_timer_init(loop, &timer);
    timer.data = &timed_out;

    uv_timer_start(
        &timer,
        [](uv_timer_t * t)
        {
            *static_cast<bool *>(t->data) = true;
            uv_stop(t->loop);
        },
        timeout_ms,
        /* repeat = */ 0);

    uv_run(loop, UV_RUN_DEFAULT);

    uv_timer_stop(&timer);
    /// Close the timer handle so libuv can release its resources.
    /// The close callback runs on the next NOWAIT tick below.
    uv_close(reinterpret_cast<uv_handle_t *>(&timer), nullptr);
    uv_run(loop, UV_RUN_NOWAIT);

    return !timed_out;
}

void RabbitMQHandler::stopBlockingLoop()
{
    LOG_DEBUG(log, "Stopping blocking loop.");
    uv_stop(loop);
}

}
