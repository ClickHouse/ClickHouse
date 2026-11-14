#include <GPU/GPUMemoryTracking.h>

#include <Common/CurrentThread.h>
#include <Common/HashTable/Hash.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/ThreadStatus.h>

#include <array>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

namespace DB::GPU
{

namespace
{

/// The query an allocation is accounted to, by the group of the thread that made it.
struct Allocation
{
    std::weak_ptr<ThreadGroup> group;
    Int64 bytes = 0;
};

/// Allocations by address, in shards, so that the threads of a query on the device do not wait for each other.
struct Shard
{
    std::mutex mutex;
    std::unordered_map<const void *, Allocation> allocations;
};

constexpr size_t num_shards = 64;

std::array<Shard, num_shards> & shards()
{
    static std::array<Shard, num_shards> instance;
    return instance;
}

Shard & shardOf(const void * memory)
{
    return shards()[intHash64(reinterpret_cast<UInt64>(memory)) % num_shards];
}

void account(const ThreadGroupPtr & group, Int64 bytes)
{
    for (ThreadGroupPtr current = group; current; current = current->getParent())
    {
        const Int64 usage = current->gpu_memory_usage.fetch_add(bytes, std::memory_order_relaxed) + bytes;
        Int64 peak = current->gpu_peak_memory_usage.load(std::memory_order_relaxed);
        while (usage > peak && !current->gpu_peak_memory_usage.compare_exchange_weak(peak, usage, std::memory_order_relaxed))
        {
        }
    }
}

}

void trackDeviceAllocation(const void * memory, size_t bytes) noexcept
{
    if (memory == nullptr || bytes == 0)
        return;
    auto group = CurrentThread::getGroup();
    if (!group)
        return;

    /// The entry is memory of the accounting itself: a limit of the query must not fail an allocation already made.
    [[maybe_unused]] MemoryTrackerBlockerInThread blocker;
    auto & shard = shardOf(memory);
    try
    {
        std::lock_guard lock(shard.mutex);
        shard.allocations.insert_or_assign(memory, Allocation{group, static_cast<Int64>(bytes)});
    }
    catch (...)
    {
        /// No memory for the entry: neither the allocation nor its deallocation is accounted.
        return;
    }
    account(group, static_cast<Int64>(bytes));
}

void trackDeviceDeallocation(const void * memory) noexcept
{
    if (memory == nullptr)
        return;

    Allocation allocation;
    {
        auto & shard = shardOf(memory);
        std::lock_guard lock(shard.mutex);
        auto it = shard.allocations.find(memory);
        if (it == shard.allocations.end())
            return;
        allocation = std::move(it->second);
        shard.allocations.erase(it);
    }
    if (auto group = allocation.group.lock())
        account(group, -allocation.bytes);
}

std::vector<ThreadGroupPtr> groupsHoldingDeviceMemory()
{
    std::vector<ThreadGroupPtr> groups;
    std::unordered_set<const ThreadGroup *> seen;
    for (size_t shard = 0; shard < num_shards; ++shard)
    {
        std::vector<ThreadGroupPtr> found;
        {
            std::lock_guard lock(shards()[shard].mutex);
            for (const auto & [memory, allocation] : shards()[shard].allocations)
                if (auto group = allocation.group.lock())
                    found.push_back(std::move(group));
        }
        for (auto & group : found)
        {
            while (auto parent = group->getParent())
                group = std::move(parent);
            if (seen.insert(group.get()).second)
                groups.push_back(std::move(group));
        }
    }
    return groups;
}

}
