#include <Processors/Executors/Runtime/Wyrd/Strategies/Simulation/FutexTable.h>
#include <Processors/Executors/Runtime/Wyrd/Random.h>

namespace DB::Runtime::Wyrd
{

void FutexTable::park(void * address, SimulatedThread * thread)
{
    queues[address].push_back(thread);
}

VectorWithMemoryTracking<SimulatedThread *> FutexTable::wake(void * address, size_t count)
{
    VectorWithMemoryTracking<SimulatedThread *> woken;

    auto it = queues.find(address);
    if (it == queues.end())
        return woken;

    VectorWithMemoryTracking<SimulatedThread *> & waiters = it->second;
    while (count > 0 && !waiters.empty())
    {
        woken.push_back(popRandom(waiters));
        --count;
    }

    if (waiters.empty())
        queues.erase(it);

    return woken;
}

}
