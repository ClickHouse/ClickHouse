#pragma once

#include <Common/Epoll.h>
#include <Common/Fiber.h>
#include <Common/FiberStack.h>
#include <Poco/Timespan.h>

#if defined(OS_LINUX)
#include <sys/epoll.h>
#endif


namespace DB
{

enum class AsyncEventTimeoutType : uint8_t
{
    CONNECT,
    RECEIVE,
    SEND,
    NONE,
};

using AsyncCallback = std::function<void(int, Poco::Timespan, AsyncEventTimeoutType, const std::string &, uint32_t)>;
using SuspendCallback = std::function<void()>;

struct FiberInfo
{
    const Fiber * fiber = nullptr;
    const FiberInfo * parent_fiber_info = nullptr;
};

/// Base class for a task that will be executed in a fiber.
/// It has only one method - run, that takes 2 callbacks:
/// 1) async_callback - callback that should be called when this task tries to perform
///    some operation on a file descriptor (e.g. reading from socket) that can block this task execution.
/// 2) suspend_callback - callback that can be called to suspend current fiber execution explicitly.
struct AsyncTask
{
public:
    virtual void run(AsyncCallback async_callback, SuspendCallback suspend_callback) = 0;
    virtual ~AsyncTask() = default;
};

/// Base class for executing tasks inside a fiber.
class AsyncTaskExecutor
{
public:
    explicit AsyncTaskExecutor(std::unique_ptr<AsyncTask> task_);

    /// Resume task execution. This method returns when task is completed or suspended.
    void resume();

    /// Cancel task execution. Fiber will be destroyed even if task wasn't finished.
    void cancel();

    /// Restart task execution. Current fiber will be destroyed
    /// and the new one will be created with the same task.
    /// The next resume() call will start the new task from the beginning
    void restart();

    bool isCancelled() const { return is_cancelled; }

    virtual ~AsyncTaskExecutor() = default;


#if defined(OS_LINUX)
    enum Event
    {
        READ = EPOLLIN,
        WRITE = EPOLLOUT,
        ERROR = EPOLLERR,
    };
#else
    enum Event
    {
        READ = 1,
        WRITE = 2,
        ERROR = 4,
    };
#endif

protected:
    /// Method that is called in resume() before actual fiber resuming.
    /// If it returns false, resume() will return immediately without actual fiber resuming.
    virtual bool checkBeforeTaskResume() = 0;

    /// Method that is called in resume() after fiber resuming (when it was finished or suspended).
    virtual void afterTaskResume() = 0;

    /// Method that is called on async event (when async callback is called) before fiber is suspended.
    virtual void processAsyncEvent(int fd, Poco::Timespan timeout, AsyncEventTimeoutType timeout_type, const std::string & fd_description, uint32_t async_events) = 0;

    /// Method that is called when task is resumed after it was suspended on async event.
    virtual void clearAsyncEvent() = 0;

    /// Process exception caught while task execution. It's called after fiber resume if exception happened.
    virtual void processException(std::exception_ptr e) { std::rethrow_exception(e); }

    /// Method that is called in cancel() before fiber destruction.
    virtual void cancelBefore() { }
    /// Method that is called in cancel() after fiber destruction.
    virtual void cancelAfter() { }

    /// Resume fiber explicitly without mutex locking.
    /// Can be called in cancelBefore().
    void resumeUnlocked();

private:
    struct Routine;

    void createFiber();
    void destroyFiber();

    FiberStack fiber_stack;
    Fiber fiber;
    std::mutex fiber_lock;
    std::exception_ptr exception;

    std::atomic_bool routine_is_finished = false;
    std::atomic_bool is_cancelled = false;

    std::unique_ptr<AsyncTask> task;
};

String getSocketTimeoutExceededMessageByTimeoutType(AsyncEventTimeoutType type, Poco::Timespan timeout, const String & socket_description);

}
