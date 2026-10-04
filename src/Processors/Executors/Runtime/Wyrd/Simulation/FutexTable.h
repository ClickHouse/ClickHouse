#pragma once

#include <map>
#include <vector>

namespace DB::Runtime::Wyrd
{

struct SimulatedThread;

class FutexTable
{
public:
    void park(void * address, SimulatedThread * thread);
    std::vector<SimulatedThread *> wake(void * address, size_t count);

private:
    std::map<void *, std::vector<SimulatedThread *>> queues;
};

}
