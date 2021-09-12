#include <Common/Scheduler.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <iostream>
#include <fmt/format.h>


using namespace DB;


struct Payload
{
    size_t idx;
    double seconds;
    size_t priority;
    size_t weight;

    void run()
    {
        std::cerr << fmt::format("Task {}, sleeping for {} seconds, priority {}, weight {}\n", idx, seconds, priority, weight);
        std::this_thread::sleep_for(std::chrono::duration<double>(seconds));
    }
};


int main(int, char **)
{
    using Sched = Scheduler<Payload>;
    Sched scheduler;

    std::mutex mutex;
    std::condition_variable condvar;

    std::thread scheduling_thread([&]
    {
        for (size_t i = 0; i < 1000; ++i)
        {
            Sched::Task task;
            task.priority = i % 3;
            task.weight = 1 + i / 3 % 2;
            task.resource_keys.emplace_back(Sched::ResourceConstraint{"all", 4});
            task.resource_keys.emplace_back(Sched::ResourceConstraint{fmt::format("user{}", task.weight), 4});

            task.data.idx = i;
            task.data.seconds = 1;
            task.data.priority = task.priority;
            task.data.weight = task.weight;

            std::unique_lock lock(mutex);
            scheduler.push(std::move(task));
            condvar.notify_one();
        }
    });

    std::vector<std::thread> threads;

    Sched::HandlePtr handle;
    std::unique_lock lock(mutex);
    while (true)
    {
        condvar.wait(lock, [&] { handle = scheduler.pop(); return handle != nullptr; });
        threads.emplace_back([&, handle = std::move(handle)]() mutable
        {
            handle->task.data.run();
            handle.reset();
            condvar.notify_one();
        });
    }
}
