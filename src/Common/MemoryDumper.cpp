#include "MemoryDumper.h"

#include <Common/MemoryTrackerBlockerInThread.h>

MemoryDumper & MemoryDumper::instance()
{
    static MemoryDumper detector;
    return detector;
};

bool MemoryDumper::isEnabled() const
{
    return enabled.load(std::memory_order_acquire);
}

void MemoryDumper::enable()
{
    enabled.store(true, std::memory_order_release);
}

void MemoryDumper::disable()
{
    enabled.store(false, std::memory_order_release);
}

void MemoryDumper::onAllocation(void * ptr, size_t size)
{
    if (!enabled.load(std::memory_order_acquire))
        return;

    MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);

    String query_id;
    UInt64 thread_id;

    if (DB::CurrentThread::isInitialized())
    {
        query_id = DB::CurrentThread::getQueryId();
        thread_id = DB::CurrentThread::get().thread_id;
    }
    else
    {
        thread_id = DB::MainThreadStatus::get()->thread_id;
    }

    StackTrace trace;

    uint64_t raw_ptr = reinterpret_cast<uint64_t>(ptr);
    size_t hash_map_index = intHash64(raw_ptr) % TwoLevelHashMapSize;
    auto & allocation_hash_map = allocation_two_level_hash_map[hash_map_index];
    std::lock_guard<std::mutex> lock(allocation_hash_map.mutex);
    if (!allocation_hash_map.ptr_to_allocation)
        allocation_hash_map.ptr_to_allocation.emplace();

    auto memory_allocation_entry = makePairNoInit(ptr, MemoryAllocation{thread_id, std::move(query_id), raw_ptr, size, std::move(trace)});
    allocation_hash_map.ptr_to_allocation->insert(memory_allocation_entry);
}

void MemoryDumper::onDeallocation(void * ptr, size_t)
{
    if (!enabled.load(std::memory_order_acquire))
        return;

    size_t hash_map_index = intHash64(reinterpret_cast<uint64_t>(ptr)) % TwoLevelHashMapSize;
    auto & allocation_hash_map = allocation_two_level_hash_map[hash_map_index];
    std::lock_guard<std::mutex> lock(allocation_hash_map.mutex);
    if (!allocation_hash_map.ptr_to_allocation)
        return;

    allocation_hash_map.ptr_to_allocation->erase(ptr);
}

void MemoryDumper::dump(std::vector<MemoryAllocation> & result_memory_allocations)
{
    MemoryTrackerBlockerInThread untrack_lock(VariableContext::Global);
    for (auto & allocation_hash_map : allocation_two_level_hash_map)
    {
        std::lock_guard<std::mutex> lock(allocation_hash_map.mutex);
        if (!allocation_hash_map.ptr_to_allocation)
            allocation_hash_map.ptr_to_allocation.emplace();

        for (auto & [key, value] : *allocation_hash_map.ptr_to_allocation)
            result_memory_allocations.push_back(value);
    }
}
