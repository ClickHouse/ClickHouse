#include <gtest/gtest.h>

#include <Common/MemoryTracker.h>
#include <Common/VariableContext.h>

#include <atomic>
#include <memory>
#include <thread>

TEST(MemoryTrackerParent, ParentAttachedConcurrentlyWithAlloc)
{
    MemoryTracker child(/*parent_=*/nullptr, VariableContext::Thread);
    std::atomic<bool> attached = false;

    std::thread allocator([&]
    {
        while (!attached.load(std::memory_order_relaxed))
            ;
        child.adjustWithUntrackedMemory(4096);
    });

    auto parent = std::make_unique<MemoryTracker>(
        &total_memory_tracker, VariableContext::User, /*log_peak_memory_usage_in_destructor_=*/false);
    child.setParent(parent.get());
    attached.store(true, std::memory_order_release);

    allocator.join();
}
