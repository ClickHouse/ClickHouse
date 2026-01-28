#pragma once

#include <mutex>
#include <list>
#include <cstdlib>

namespace DB
{

class UntrackedMemoryHolder
{
    std::mutex mutex;
    std::list<void *> memory;

public:
    void alloc(size_t size)
    {
        void * ptr = malloc(size);
        std::lock_guard lock(mutex);
        memory.push_back(ptr);
    }

    void free()
    {
        std::lock_guard lock(mutex);
        for (void * ptr : memory)
            ::free(ptr);
        memory.clear();
    }
};

}
