#include <Processors/Executors/Runtime/Wyrd/Testing/WaitGroup.h>
#include <Processors/Executors/Runtime/Wyrd/Facade/Futex.h>

namespace DB::Runtime::Wyrd
{

WaitGroup & WaitGroup::add(size_t count, std::function<void(size_t)> body)
{
    for (size_t i = 0; i < count; ++i)
    {
        threads.emplace_back(
            [this, index = threads.size(), body]
            {
                UInt32 value = started.load();
                while (value == 0)
                    futexWaitFetch(started, value);
                body(index);
            });
    }
    return *this;
}

void WaitGroup::join()
{
    started.store(1);
    futexWakeAll(started);

    for (auto & thread : threads)
        thread.join();
    threads.clear();
}

}
