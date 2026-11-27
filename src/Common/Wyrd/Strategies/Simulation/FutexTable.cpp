#include <Common/Wyrd/Strategies/Simulation/FutexTable.h>
#include <Common/Wyrd/Strategies/Random.h>

namespace DB::Wyrd::SimulationMode
{

namespace
{

SimulatedThread * popRandom(VectorWithMemoryTracking<SimulatedThread *> & threads)
{
    size_t index = randomInt(0, threads.size() - 1);
    SimulatedThread * thread = threads[index];
    threads[index] = threads.back();
    threads.pop_back();
    return thread;
}

}

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
