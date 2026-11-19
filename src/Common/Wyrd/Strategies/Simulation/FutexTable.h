#pragma once

#include <Common/MapWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB::Runtime::Wyrd
{

struct SimulatedThread;

class FutexTable
{
public:
    void park(void * address, SimulatedThread * thread);
    VectorWithMemoryTracking<SimulatedThread *> wake(void * address, size_t count);

private:
    MapWithMemoryTracking<void *, VectorWithMemoryTracking<SimulatedThread *>> queues;
};

}
