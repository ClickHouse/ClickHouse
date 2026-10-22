#pragma once

#include <deque>
#include <future>
#include <type_traits>

#include <base/scope_guard.h>
#include <Common/threadPoolCallbackRunner.h>

namespace DB::Iceberg
{

template <typename Items, typename Decode, typename Consume>
void decodeManifestsInOrder(
    const Items & items, size_t max_in_flight, ThreadPool & pool, ThreadName thread_name, Decode && decode, Consume && consume)
{
    using Result = std::invoke_result_t<Decode &, const typename Items::value_type &>;

    auto runner = threadPoolCallbackRunnerUnsafe<Result>(pool, thread_name);

    std::deque<std::future<Result>> in_flight;
    SCOPE_EXIT({
        for (auto & future : in_flight)
        {
            if (future.valid())
                future.wait();
        }
    });

    size_t next_to_decode = 0;
    while (next_to_decode < items.size() || !in_flight.empty())
    {
        while (in_flight.size() < max_in_flight && next_to_decode < items.size())
        {
            const auto & item = items[next_to_decode++];
            in_flight.push_back(runner([&decode, &item] { return decode(item); }, Priority{}));
        }

        auto pending = std::move(in_flight.front());
        in_flight.pop_front();
        if (!consume(pending.get()))
            return;
    }
}

}
