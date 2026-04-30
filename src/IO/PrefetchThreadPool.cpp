#include <IO/PrefetchThreadPool.h>
#include <IO/Rope.h>

namespace DB
{

std::future<Rope> PrefetchThreadPool::submit(std::function<Rope()> task)
{
    auto shared_promise = std::make_shared<std::promise<Rope>>();
    auto future = shared_promise->get_future();

    pool.scheduleOrThrowOnError([shared_promise, shared_task = std::move(task)]()
    {
        try
        {
            shared_promise->set_value(shared_task());
        }
        catch (...)
        {
            shared_promise->set_exception(std::current_exception());
        }
    });

    return future;
}

}
