#pragma once

#include <atomic>
#include <mutex>
#include <base/types.h>
#include <Common/Epoll.h>
#include <Common/StackfulCoroutine.h>
#include <Common/CoroutineStack.h>
#include <Common/OpenTelemetryTraceContext.h>
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

struct CoroutineInfo
{
    const StackfulCoroutine * coroutine = nullptr;
    const CoroutineInfo * parent_coroutine_info = nullptr;
};

/// Base class for a task that will be executed in a coroutine.
/// It has only one method - run, that takes 2 callbacks:
/// 1) async_callback - callback that should be called when this task tries to perform
///    some operation on a file descriptor (e.g. reading from socket) that can block this task execution.
/// 2) suspend_callback - callback that can be called to suspend current coroutine execution explicitly.
struct AsyncTask
{
public:
    virtual void run(AsyncCallback async_callback, SuspendCallback suspend_callback) = 0;
    virtual ~AsyncTask() = default;
};

/// Base class for executing tasks inside a coroutine.
class AsyncTaskExecutor
{
public:
    /// operation_name_ is used as the name of the OpenTelemetry span covering one execution of the task
    AsyncTaskExecutor(std::unique_ptr<AsyncTask> task_, String operation_name_);

    /// Resume task execution. This method returns when task is completed or suspended.
    void resume();

    /// Cancel task execution. Coroutine will be destroyed even if task wasn't finished.
    void cancel();

    /// Restart task execution. Current coroutine will be destroyed
    /// and the new one will be created with the same task.
    /// The next resume() call will start the new task from the beginning
    void restart();

    bool isCancelled() const { return is_cancelled; }

    virtual ~AsyncTaskExecutor() = default;


#if defined(OS_LINUX) || defined(OS_DARWIN)
    /// EPOLLIN/EPOLLOUT/EPOLLERR come from <sys/epoll.h> on Linux and from the kqueue
    /// compatibility shim in <Common/Epoll.h> on macOS, so the values match the `Epoll` flags
    /// on both platforms.
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
    /// Method that is called in resume() before actual coroutine resuming.
    /// If it returns false, resume() will return immediately without actual coroutine resuming.
    virtual bool checkBeforeTaskResume() = 0;

    /// Method that is called in resume() after coroutine resuming (when it was finished or suspended).
    virtual void afterTaskResume() = 0;

    /// Method that is called on async event (when async callback is called) before coroutine is suspended.
    virtual void processAsyncEvent(int fd, Poco::Timespan timeout, AsyncEventTimeoutType timeout_type, const std::string & fd_description, uint32_t async_events) = 0;

    /// Method that is called when task is resumed after it was suspended on async event.
    virtual void clearAsyncEvent() = 0;

    /// Process exception caught while task execution. It's called after coroutine resume if exception happened.
    virtual void processException(std::exception_ptr e) { std::rethrow_exception(e); }

    /// Method that is called in cancel() before coroutine destruction.
    virtual void cancelBefore() { }
    /// Method that is called in cancel() after coroutine destruction.
    virtual void cancelAfter() { }

    /// Resume coroutine explicitly without mutex locking.
    /// Can be called in cancelBefore().
    void resumeUnlocked();

private:
    struct Routine;

    void createCoroutine();
    void destroyCoroutine();

    CoroutineStack coroutine_stack;
    StackfulCoroutine coroutine;
    std::mutex coroutine_lock;
    std::exception_ptr exception;

    std::atomic_bool routine_is_finished = false;
    std::atomic_bool is_cancelled = false;

    std::unique_ptr<AsyncTask> task;

    const String operation_name;

    /// Spans created inside the task belong to the query trace.
    const OpenTelemetry::TracingContextOnThread parent_trace_context;
};

String getSocketTimeoutExceededMessageByTimeoutType(AsyncEventTimeoutType type, Poco::Timespan timeout, const String & socket_description);

}
