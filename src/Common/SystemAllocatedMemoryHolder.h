#pragma once

#include <mutex>
#include <list>
#include <cstdlib>
#include <Common/AllocationInterceptors.h>
#include <Common/CurrentMemoryTracker.h>

namespace DB
{

/// This is a helper class for SYSTEM ALLOCATE MEMORY / SYSTEM FREE MEMORY commands
class SystemAllocatedMemoryHolder
{
    std::mutex mutex;

    class Memory
    {
        void * data = nullptr;
        size_t size;

    public:
        explicit Memory(size_t size_) : size(size_)
        {
            [[maybe_unused]] auto res = CurrentMemoryTracker::allocNoThrow(size);
            data = __real_malloc(size);
        }

        Memory(Memory && other) noexcept
        {
            std::swap(data, other.data);
            std::swap(size, other.size);
        }

        ~Memory()
        {
            if (!data)
                return;

            __real_free(data);
            [[maybe_unused]] auto res = CurrentMemoryTracker::free(size);
        }
    };

    std::list<Memory> memory_list;

public:

    void alloc(size_t size)
    {
        Memory memory(size);
        std::lock_guard lock(mutex);
        memory_list.emplace_back(std::move(memory));
    }

    void free()
    {
        std::lock_guard lock(mutex);
        memory_list.clear();
    }
};

}
