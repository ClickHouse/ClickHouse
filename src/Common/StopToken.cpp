#include <Common/StopToken.h>

#include <base/defines.h>
#include <exception>
#include <mutex>
#include <thread>

struct StopState
{
    /// A pretty inefficient implementation (mutex instead of spinlock, std::list instead of intrusive list,
    /// shared_ptr instead of custom refcounting), but this currently doesn't matter. If you want to use this in some
    /// performance-sensitive code, feel free to reimplement, probably similar to folly::CancellationToken implementation
    /// (but if it's actually performance-sensitive then maybe try to avoid using this at all: all this pointer chasing,
    /// reference conting, and callbacks can't be very fast.)

    std::mutex mutex;
    std::atomic<bool> stopped {false};
    std::list<StopCallback *> callbacks;
};

bool StopToken::stop_requested() const
{
    return state && state->stopped.load();
}

StopSource::StopSource() : state(std::make_shared<StopState>()) {}

bool StopSource::request_stop()
{
    std::list<StopCallback *> callbacks;
    {
        std::lock_guard lock(state->mutex);
        if (state->stopped.exchange(true))
        {
            chassert(state->callbacks.empty());
            return false;
        }
        callbacks = std::move(state->callbacks);
    }
    std::exception_ptr exception;
    for (StopCallback * cb : callbacks)
    {
        /// If one StopCallback's destroys another StopCallback, this may deadlock because the second
        /// StopCallback's destructor will wait for both callbacks to return (if it's later in the `callbacks` list).
        /// This can be prevented by allowing ~StopCallback() to set some cancellation flag that we'd check here,
        /// but this doesn't seem worth the trouble. Just don't have such complicated callbacks.

        try
        {
            cb->callback();
        }
        catch (...)
        {
            if (!exception)
                exception = std::current_exception();
        }
        cb->returned.store(true);
    }
    if (exception)
        std::rethrow_exception(exception);
    return true;
}

StopCallback::StopCallback(const StopToken & token, Callback cb) : state(token.state), callback(std::move(cb))
{
    if (state == nullptr)
        return;
    std::unique_lock lock(state->mutex);
    if (state->stopped.load())
    {
        lock.unlock();
        state = nullptr;
        callback();
    }
    else
    {
        state->callbacks.push_back(this);
        it = std::prev(state->callbacks.end());
    }
}

StopCallback::~StopCallback()
{
    if (state == nullptr)
        return;
    std::unique_lock lock(state->mutex);
    if (state->stopped.load())
    {
        lock.unlock();
        while (!returned.load())
            std::this_thread::yield();
    }
    else
    {
        state->callbacks.erase(it);
    }
}
