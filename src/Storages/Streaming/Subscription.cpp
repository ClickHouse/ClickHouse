#include <mutex>
#include <utility>

#include <Storages/Streaming/Subscription.h>

namespace DB
{

#if defined(OS_LINUX)

void StreamSubscription::push(Block block)
{
    {
        std::unique_lock guard(mutex);
        ready_blocks.emplace_back(std::move(block));
    }
    new_blocks_event.write(1);
}

BlocksList StreamSubscription::extractAll()
{
    if (is_disabled.load())
        return {};

    new_blocks_event.read();

    std::unique_lock guard(mutex);
    return std::exchange(ready_blocks, {});
}

std::optional<int> StreamSubscription::fd() const
{
    return new_blocks_event.fd;
}

void StreamSubscription::disable()
{
    is_disabled.store(true);
    new_blocks_event.write(1);
}

#else

void StreamSubscription::push(Block block)
{
    std::unique_lock guard(mutex);
    ready_blocks.emplace_back(std::move(block));
    empty_blocks.notify_one();
}

BlocksList StreamSubscription::extractAll()
{
    std::unique_lock guard(mutex);

    while (ready_blocks.empty() && !is_disabled.load())
        empty_blocks.wait(guard);

    return std::exchange(ready_blocks, {});
}

std::optional<int> StreamSubscription::fd() const
{
    return std::nullopt;
}

void StreamSubscription::disable()
{
    std::unique_lock guard(mutex);
    is_disabled.store(true);
    empty_blocks.notify_one();
}

#endif

}
