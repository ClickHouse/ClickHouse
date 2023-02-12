#include <Common/CoTask.h>


namespace DB::ErrorCodes
{
    extern const int TASK_CANCELLED;
}


namespace DB::Co
{

void details::CancelStatus::setCancelException(std::exception_ptr cancel_exception_)
{
    std::lock_guard lock{mutex};
    if (cancel_exception || !cancel_exception_)
        return;
    cancel_exception = cancel_exception_;
    onCancelled(cancel_exception_);
    for (const auto & child : children)
        child->setCancelException(cancel_exception_);
}

void details::CancelStatus::setCancelException(const String & message_)
{
    auto exception = std::make_exception_ptr(Exception{ErrorCodes::TASK_CANCELLED, "{}", message_.empty() ? "Task cancelled" : message_});
    setCancelException(exception);
}

std::exception_ptr details::CancelStatus::getCancelException() const
{
    std::lock_guard lock{mutex};
    return cancel_exception;
}

/// Registers this cancel status as a child of another cancel status. This function is used only in complex scenarious, like parallel().
void details::CancelStatus::addChild(std::shared_ptr<CancelStatus> child_)
{
    std::lock_guard lock{mutex};
    children.emplace_back(child_);
}

bool details::ContinuationList::add(std::coroutine_handle<> new_item)
{
    std::lock_guard lock{mutex};
    if (!processed)
    {
        items.push_back(std::move(new_item));
        return true;
    }
    return false;
}

void details::ContinuationList::process()
{
    std::list<std::coroutine_handle<>> items_to_process;
    {
        std::lock_guard lock{mutex};
        items_to_process = std::exchange(items, {});
        processed = true;
    }
    for (auto & item : items_to_process)
        item.resume();
}

bool details::ContinuationList::isProcessed() const
{
    std::lock_guard lock{mutex};
    return processed;
}

Task<> sequential(std::vector<Task<>> && tasks)
{
    if (tasks.empty())
        co_return;

    SCOPE_EXIT_SAFE({ tasks.clear(); });
    auto params = co_await TaskRunParams{};

    for (auto & task : tasks)
    {
        co_await StopIfCancelled{};
        co_await std::move(task).run(params);
    }
}

Task<> parallel(std::vector<Task<>> && tasks)
{
    if (tasks.empty())
        co_return;

    SCOPE_EXIT_SAFE({ tasks.clear(); });

    auto params = co_await TaskRunParams{};
    auto cancel_status = std::make_shared<details::CancelStatus>();
    params.cancel_status->addChild(cancel_status);
    params.cancel_status = cancel_status;

    struct AwaitInfo
    {
        Task<>::Awaiter awaiter;
        bool ready = false;
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
                co_await info.awaiter; /// Call co_await anyway because it can throw an exception.
            }
        }

        for (auto & info : await_infos)
        {
            if (!info.ready)
            {
                co_await StopIfCancelled{};
                info.ready = true;
                co_await info.awaiter;
            }
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
}

}
