#include <Common/MemoryTrackerFaultInjectorInThread.h>
#include <Common/thread_local_rng.h>
#include <random>

thread_local uint64_t MemoryTrackerFaultInjectorInThread::counter = 0;
thread_local double MemoryTrackerFaultInjectorInThread::probability = 0.;

MemoryTrackerFaultInjectorInThread::MemoryTrackerFaultInjectorInThread(double probability_)
{
    ++counter;
    previous_probability = probability;
    probability = probability_;
}
MemoryTrackerFaultInjectorInThread::~MemoryTrackerFaultInjectorInThread()
{
    --counter;
    probability = previous_probability;
}

bool MemoryTrackerFaultInjectorInThread::faulty()
{
    if (counter)
    {
        std::bernoulli_distribution fault(probability);
        return fault(thread_local_rng);
    }
    return false;
}
