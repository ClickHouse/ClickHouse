#pragma once

#include <Common/CoTask_fwd.h>
#include <Common/scope_guard_safe.h>
#include <Common/threadPoolCallbackRunner.h>
#include <boost/tti/has_member_function.hpp>
#include <coroutine>
#include <utility>


namespace DB::Co
{

/// This file declares utilities to deal with C++20 coroutines.
/// Example: (prints integers using three threads in parallel)
///
/// Co::Task<> print_numbers(size_t count)
/// {
///    for (size_t i = 0; i != count; ++i)
///    {
///        std::cout << "thread=" << std::this_thread::get_id() << ", i=" << i << std::endl;
///        sleepForMilliseconds(100);
///    }
///    co_return;
/// }
///
/// Co::Task<> print_numbers_in_parallel()
/// {
///    std::vector<Co::Task<>> tasks;
///    tasks.push_back(print_numbers(5));
///    tasks.push_back(print_numbers(10));
///    tasks.push_back(print_numbers(12));
///    co_await Co::parallel(std::move(tasks));
/// }
///
///
/// void main()
/// {
///     ThreadPool thread_pool{10};
///     Co::Scheduler scheduler{thread_pool};
///     auto main_task = print_numbers_in_parallel(); /// create a main task to execute but don't execute it yet
///     std::move(main_task).syncRun(scheduler); /// execute the main task and block the current thread until it finishes or throws an exception.
///  }

/// Scheduler is used to schedule tasks to run them on another thread.
class Scheduler
{
public:
    Scheduler() : schedule_function(&Scheduler::defaultScheduleFunction) { }

    explicit Scheduler(const std::function<void(std::function<void()>)> & schedule_function_) : Scheduler()
    {
        if (schedule_function_)
            schedule_function = schedule_function_;
    }

    explicit Scheduler(const ThreadPoolCallbackRunner<void> & thread_pool_callback_runner) : Scheduler()
    {
        if (thread_pool_callback_runner)
            schedule_function = [thread_pool_callback_runner](std::function<void()> callback)
            { thread_pool_callback_runner(std::move(callback), 0); };
    }

    explicit Scheduler(ThreadPool & thread_pool, const std::string & thread_name = "Co::Scheduler")
        : Scheduler(threadPoolCallbackRunner<void>(thread_pool, thread_name))
    {
    }

    void schedule(std::function<void()> callback) const { schedule_function(std::move(callback)); }

private:
    static void defaultScheduleFunction(std::function<void()> callback) { std::move(callback)(); }
    std::function<void(std::function<void()>)> schedule_function;
};

namespace details { class CancelStatus; }

/// Used to run a coroutine from another coroutine.
struct TaskRunParams
{
    Scheduler scheduler;
    std::shared_ptr<details::CancelStatus> cancel_status;
};


/// Keeps an asynchronous task, used as a return type in coroutines.
template <typename ResultType>
class [[nodiscard]] Task
{
    class AwaitControl;
    class Promise;
    class PromiseBase;

public:
    using promise_type = Promise;

    /// Creates a non-initialized task, it cannot be executed until assigned.
    Task() = default;

    /// Tasks are initialized by promise_type.
    explicit Task(const std::shared_ptr<AwaitControl> & await_control_) : await_control(await_control_) {}

    /// Task is a move-only class.
    Task(const Task &) = delete;
    Task & operator=(const Task &) = delete;

    Task(Task&& task_) noexcept : await_control(std::exchange(task_.await_control, {})) {}

    Task & operator=(Task && task_) noexcept
    {
        if (this == &task_)
            return *this;
        await_control = std::exchange(task_.await_control, {});
        return *this;
    }

    /// Whether this is an initialized task ready to execute.
    /// Returns false if the task is either created by the default constructor or has already run.
    operator bool() const { return static_cast<bool>(await_control); }

    class Awaiter;

    /// Runs a task on a passed scheduler, returns a waiter which can be checked to find out when it's done.
    /// Example of usage: `co_await task.run(scheduler)`.
    Awaiter run(const Scheduler & scheduler) &&
    {
        TaskRunParams params;
        params.scheduler = scheduler;
        return std::move(*this).run(params);

    }

    Awaiter run(const TaskRunParams & params) &&
    {
        chassert(await_control);
        auto awaiter = await_control->run(params);
        await_control.reset();
        return awaiter;
    }

    /// Runs a task and blocks the current thread until it's done.
    /// Prefer run() to this function if possible.
    ResultType syncRun(const Scheduler & scheduler) &&;

private:
    std::shared_ptr<AwaitControl> await_control;
};


/// Represents an asynchronous waiter for a task to be finished.
/// This class has functions await_ready(), await_suspend(), await_resume() so it can an argument of the `co_await` operator.
template <typename ResultType>
class [[nodiscard]] Task<ResultType>::Awaiter
{
public:
    Awaiter(const std::shared_ptr<AwaitControl> & await_control_) : await_control(await_control_) {}

    bool await_ready() { return await_control->isContinuationProcessed(); }
    bool await_suspend(std::coroutine_handle<> handle_) { return await_control->addContinuation(handle_); }

    ResultType await_resume()
    {
        if (auto exception = await_control->getException())
            std::rethrow_exception(exception);
        if constexpr (!std::is_void_v<ResultType>)
            return await_control->getResult();
    }

    /// Blocks the current thread until the coroutine finishes or throws an exception.
    ResultType syncWait();

    /// Tries to cancel the coroutine. If succeeded the coroutine stops with a specified exception.
    /// That doesn't act immediately because the coroutine decides by itself where to stop (by calling coawait Co::StopIfCancelled{}).
    /// After calling tryCancelTask() the coroutine can still finish successfully if it decides so.
    void tryCancelTask(std::exception_ptr cancel_exception_) { await_control->getCancelStatus()->setCancelException(cancel_exception_); }
    void tryCancelTask(const String & message_ = {}) { await_control->getCancelStatus()->setCancelException(message_); }

private:
    std::shared_ptr<AwaitControl> await_control;
};


/// Runs multiple tasks one after another.
template <typename Result>
Task<std::vector<Result>> sequential(std::vector<Task<Result>> && tasks);

Task<> sequential(std::vector<Task<>> && tasks);


/// Runs multiple tasks in parallel.
template <typename Result>
Task<std::vector<Result>> parallel(std::vector<Task<Result>> && tasks);

Task<> parallel(std::vector<Task<>> && tasks);


/// Usage: `co_await Co::IsCancelled{}` inside a coroutine returns a boolean meaning whether the coroutine is ordered to cancel and .
struct IsCancelled
{
    bool value = false;
    operator bool() const { return value; }
};

/// Usage: `co_await Co::StopIfCancelled{}` inside a coroutine stops the coroutine by throwing the exception specified in Awaiter::tryCancelTask().
struct StopIfCancelled {};


/// The following are implementation details.
namespace details
{
    /// ContinuationList is used to run resume waiting coroutines after the task has finished.
    class ContinuationList
    {
    public:
        /// Adds a new item if the list has not been processed yet, otherwise the function returns false.
        bool add(std::coroutine_handle<> new_item);

        /// Process all items in the list. The function returns false if they were already processed or there were no items in the list.
        void process();

        bool isProcessed() const;

    private:
        std::list<std::coroutine_handle<>> TSA_GUARDED_BY(mutex) items;
        bool TSA_GUARDED_BY(mutex) processed = false;
        mutable std::mutex mutex;
    };

    /// Keeps a result for the promise type.
    template <typename ResultType>
    class ResultHolder
    {
    public:
        void setResult(ResultType result_) { result = std::move(result_); }
        ResultType getResult() const { return result; }
        ResultType * getResultPtr() { return &result; }

    private:
        ResultType result;
    };

    template <>
    class ResultHolder<void>
    {
    public:
        void * getResultPtr() { return nullptr; }
        void return_void() {}
    };

    BOOST_TTI_HAS_MEMBER_FUNCTION(await_ready)

    /// Checks whether a specified type is an awaiter, i.e. has the member function await_ready().
    template <typename T>
    inline constexpr bool is_awaiter_type_v = has_member_function_await_ready<bool (T::*)()>::value;

    /// Holds a cancel exception to helps implementing Awaiter::tryCancelTask().
    class CancelStatus
    {
    public:
        CancelStatus() = default;
        virtual ~CancelStatus() = default;

        void setCancelException(const String & message_ = {});
        void setCancelException(std::exception_ptr cancel_exception_);
        std::exception_ptr getCancelException() const;
        bool isCancelled() const { return static_cast<bool>(getCancelException()); }

        /// Registers this cancel status as a child of another cancel status, so when setCancelException() is called on parent it's also called on child.
        /// This function is used only in complex scenarious, like parallel().
        void addChild(std::shared_ptr<CancelStatus> child_);

    protected:
        /// Inhetrited classes can do extra work here.
        virtual void onCancelled(std::exception_ptr /* cancel_exception_ */) {}

    private:
        mutable std::mutex mutex;
        std::exception_ptr TSA_GUARDED_BY(mutex) cancel_exception;
        std::list<std::shared_ptr<CancelStatus>> TSA_GUARDED_BY(mutex) children;
    };

    /// A helper task, part of the implementation of syncWait() and syncRun().
    class SyncWaitTask
    {
    public:
        template <typename ResultType>
        static SyncWaitTask run(typename Task<ResultType>::Awaiter & awaiter, ResultHolder<ResultType> & result, std::exception_ptr & exception, Poco::Event & event)
        {
            try
            {
                if constexpr (std::is_void_v<ResultType>)
                    co_await awaiter;
                else
                    result.setResult(co_await awaiter);
            }
            catch (...)
            {
                exception = std::current_exception();
            }
            event.set();
            co_return;
        }

        struct promise_type
        {
            auto get_return_object() { return SyncWaitTask{}; }
            std::suspend_never initial_suspend() { return {}; }
            std::suspend_never final_suspend() noexcept { return {}; }
            void unhandled_exception() { }
            void return_void() {}
        };
    };
}

/// Waits for the result in synchronous (blocking) manner.
template <typename ResultType>
ResultType Task<ResultType>::Awaiter::syncWait()
{
    Poco::Event event;
    std::exception_ptr exception;
    details::ResultHolder<ResultType> result_holder;
    details::SyncWaitTask::run(*this, result_holder, exception, event);
    event.wait();
    if (exception)
        std::rethrow_exception(exception);
    if constexpr (!std::is_void_v<ResultType>)
        return std::move(*result_holder.getResultPtr());
}

template <typename ResultType>
ResultType Task<ResultType>::syncRun(const Scheduler & scheduler) &&
{
    if constexpr (std::is_void_v<ResultType>)
        std::move(*this).run(scheduler).syncWait();
    else
        return std::move(*this).run(scheduler).syncWait();
}

template <typename ResultType>
class Task<ResultType>::PromiseBase
{
public:
    void return_value(ResultType result_) { await_control->setResult(std::move(result_)); }
protected:
    std::shared_ptr<typename Task<ResultType>::AwaitControl> await_control;
};

template <>
class Task<void>::PromiseBase
{
public:
    void return_void() {}
protected:
    std::shared_ptr<typename Task<>::AwaitControl> await_control;
};

/// Represents the `promise_type` for coroutine tasks.
template <typename ResultType>
class Task<ResultType>::Promise : public PromiseBase
{
public:
    Promise() { this->await_control = std::make_shared<AwaitControl>(std::coroutine_handle<Promise>::from_promise(*this)); }

    Task get_return_object() { return Task(getAwaitControl()); }

    Awaiter run(const Scheduler & scheduler_) { return getAwaitControl()->run(scheduler_); }

    auto initial_suspend() { return std::suspend_always{}; }

    class FinalSuspend;
    auto final_suspend() noexcept { return FinalSuspend{getAwaitControl()}; }

    void unhandled_exception() { getAwaitControl()->setException(std::current_exception()); }

    auto await_transform(TaskRunParams)
    {
        struct Getter : std::suspend_never
        {
            TaskRunParams params;
            TaskRunParams await_resume() const noexcept { return params; }
        };
        return Getter{{}, getAwaitControl()->getRunParams()};
    }

    auto await_transform(IsCancelled)
    {
        struct Getter : std::suspend_never
        {
            bool is_cancelled;
            IsCancelled await_resume() const noexcept { return {is_cancelled}; }
        };
        return Getter{{}, static_cast<bool>(getAwaitControl()->getCancelStatus()->isCancelled())};
    }

    auto await_transform(StopIfCancelled)
    {
        struct Getter : std::suspend_never
        {
            std::exception_ptr cancel_exception;
            void await_resume() const
            {
                if (cancel_exception)
                    std::rethrow_exception(cancel_exception);
            }
        };
        return Getter{{}, getAwaitControl()->getCancelStatus()->getCancelException()};
    }

    template <typename AwaiterType>
    auto await_transform(AwaiterType && awaiter, typename std::enable_if_t<details::is_awaiter_type_v<std::remove_cvref_t<AwaiterType>>> * = nullptr)
    {
        return std::forward<AwaiterType>(awaiter);
    }

    template <typename SubTaskResultType>
    typename Task<SubTaskResultType>::Awaiter await_transform(Task<SubTaskResultType> && sub_task)
    {
        return std::move(sub_task).run(getAwaitControl()->getRunParams());
    }

private:
    std::shared_ptr<typename Task<ResultType>::AwaitControl> getAwaitControl() { return this->await_control; }
};

/// FinalSuspend is used to resume waiting coroutines stored in the contunuation list after the task has finished.
template <typename ResultType>
class Task<ResultType>::Promise::FinalSuspend : public std::suspend_never
{
public:
    FinalSuspend(std::shared_ptr<AwaitControl> await_control_) : await_control(await_control_) {}

    bool await_ready() noexcept
    {
        /// The coroutine has finished normally, maybe with an exception, but it wasn't cancelled,
        /// so ~AwaitControl must not destroy it.
        await_control->setIsFinalized();

        /// Give control to the code waiting for this coroutine to be finished.
        await_control->processContinuation();
        return true;
    }

private:
    std::shared_ptr<AwaitControl> await_control;
};

/// Keeps std::coroutine_handle<> and destroys it in the destructor if the coroutine has not finished
/// (if it has finished it will be destroyed automatically).
template <typename ResultType>
class Task<ResultType>::AwaitControl : public details::ResultHolder<ResultType>, public std::enable_shared_from_this<AwaitControl>
{
public:
    AwaitControl(std::coroutine_handle<Promise> handle_) : handle(handle_) { }
    AwaitControl(const AwaitControl &) = delete;
    AwaitControl & operator=(const AwaitControl &) = delete;

    Awaiter run(const TaskRunParams & params_)
    {
        auto awaiter = Awaiter(this->shared_from_this());
        params = params_;

        if (!params.cancel_status)
            params.cancel_status = std::make_shared<details::CancelStatus>();

        params.scheduler.schedule([this, keep_this_from_destruction = this->shared_from_this()]
        {
            if (auto cancel_exception = getCancelStatus()->getCancelException())
            {
                /// Go straight to the continuation list without resuming this task.
                handle.destroy();
                setException(cancel_exception);
                processContinuation();
                return;
            }

            handle.resume();
        });

        return awaiter;
    }

    bool isContinuationProcessed() const { return continuation_list.isProcessed(); }
    bool addContinuation(std::coroutine_handle<> handle_) { return continuation_list.add(handle_); }
    void processContinuation() { continuation_list.process(); }

    void setIsFinalized() { is_finalized = true; }
    void setException(std::exception_ptr exception_) { exception = exception_; }
    std::exception_ptr getException() const { return exception; }

    TaskRunParams getRunParams() const { return params; }
    std::shared_ptr<details::CancelStatus> getCancelStatus() const { return params.cancel_status; }

private:
    std::coroutine_handle<Promise> handle;
    TaskRunParams params;
    details::ContinuationList continuation_list;

    /// Mutex is not needed to access `is_finalized` because it's accessed in separate moments of time (so no concurrent access possible):
    /// FinalSuspend::await_ready(), ~AwaitControl()
    bool is_finalized = false;

    /// Mutex is not needed to access `exception` because it's accessed in separate moments of time (so no concurrent access possible):
    /// AwaitControl::run(), Promise::unhandled_exception(), Awaiter::await_resume().
    std::exception_ptr exception;
};

template <typename ResultType>
Task<std::vector<ResultType>> sequential(std::vector<Task<ResultType>> && tasks)
{
    std::vector<ResultType> results;
    if (tasks.empty())
        co_return results;

    SCOPE_EXIT_SAFE({ tasks.clear(); });
    results.reserve(tasks.size());

    auto params = co_await TaskRunParams{};

    for (auto & task : tasks)
    {
        co_await StopIfCancelled{};
        results.emplace_back(co_await std::move(task).run(params));
    }

    co_return results;
}

template <typename ResultType>
Task<std::vector<ResultType>> parallel(std::vector<Task<ResultType>> && tasks)
{
    std::vector<ResultType> results;
    if (tasks.empty())
        co_return results;

    SCOPE_EXIT_SAFE({ tasks.clear(); });
    results.reserve(tasks.size());

    auto params = co_await TaskRunParams{};
    auto cancel_status = std::make_shared<details::CancelStatus>();
    params.cancel_status->addChild(cancel_status);
    params.cancel_status = cancel_status;

    struct AwaitInfo
    {
        typename Task<ResultType>::Awaiter awaiter;
        bool ready = false;
        size_t index_in_results = static_cast<size_t>(-1);
    };
    std::vector<AwaitInfo> await_infos;
    await_infos.reserve(tasks.size());

    std::exception_ptr exception;

    try
    {
        for (auto & task : tasks)
        {
            await_infos.emplace_back(AwaitInfo{std::move(task).run(params)});
            auto & info = await_infos.back();

            /// After `task.run()` the `task` can be already finished.
            if (info.awaiter.await_ready())
            {
                info.ready = true;
                info.index_in_results = results.size();
                results.emplace_back(co_await info.awaiter);
            }
        }

        for (auto & info : await_infos)
        {
            if (!info.ready)
            {
                co_await StopIfCancelled{};
                info.ready = true;
                info.index_in_results = results.size();
                results.emplace_back(co_await info.awaiter);
            }
        }

        for (size_t i = 0; i != tasks.size(); ++i)
        {
            std::swap(results[i], results[await_infos[i].index_in_results]);
        }
    }
    catch (...)
    {
        exception = std::current_exception();
    }

    if (exception)
    {
        try
        {
            cancel_status->setCancelException("Task was cancelled because a parallel task failed");
        }
        catch (...)
        {
        }

        for (auto & info : await_infos)
        {
            if (!info.ready)
            {
                try
                {
                    co_await info.awaiter;
                }
                catch (...)
                {
                }
            }
        }

        std::rethrow_exception(exception);
    }

    co_return results;
}

}
