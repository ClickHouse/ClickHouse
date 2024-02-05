#pragma once

#include <atomic>
#include <mutex>

#include <base/types.h>
#include <Common/StackTrace.h>
#include <Common/HashTable/HashMap.h>

struct MemoryAllocation
{
    UInt64 thread_id;
    String query_id;
    UInt64 ptr;
    size_t size = 0;
    StackTrace stack_trace;
};

class MemoryDumper
{
public:
    static MemoryDumper & instance();

    bool isEnabled() const;

    void enable();

    void disable();

    void onAllocation(void * ptr, size_t size);

    void onDeallocation(void * ptr, size_t size);

    void dump(std::vector<MemoryAllocation> & result_memory_allocations);

    MemoryDumper() = default;

private:
    std::atomic<bool> enabled = false;

    std::string dump_file_name;

    struct AllocationHashMap
    {
        std::mutex mutex;
        /// Initialize hash map lazily to avoid abort during recursive initialization
        std::optional<HashMap<void *, MemoryAllocation>> ptr_to_allocation;
    };

    static constexpr size_t TwoLevelHashMapSize = 512;
    std::array<AllocationHashMap, TwoLevelHashMapSize> allocation_two_level_hash_map;
};
