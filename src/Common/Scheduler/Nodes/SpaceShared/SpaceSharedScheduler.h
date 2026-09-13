#pragma once

#include <base/defines.h>

#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/Scheduler/ISpaceSharedNode.h>

#include <atomic>
#include <limits>
#include <map>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

/// Resource scheduler root node with a dedicated thread.
/// May have multiple children, each corresponding to a root of the managed scheduling hierarchy
/// (a WorkloadNode for a root workload). Multiple roots form an independent forest of workload
/// trees. The scheduler itself never enforces a limit; it simply processes pending increase and
/// decrease requests coming from its children in a round-robin fashion to avoid starving any tree.
class SpaceSharedScheduler final : public ISpaceSharedNode
{
public:
    explicit SpaceSharedScheduler()
        : ISpaceSharedNode(events)
    {}

    ~SpaceSharedScheduler() override
    {
        stop();
        while (!children.empty())
            removeChild(children.begin()->second.get());
    }

    /// Runs separate scheduler thread
    void start(ThreadName name)
    {
        if (!scheduler.joinable())
            scheduler = ThreadFromGlobalPool([this, name] { schedulerThread(name); });
    }

    /// Joins scheduler threads and execute every pending request iff graceful
    void stop(bool graceful = true)
    {
        if (scheduler.joinable())
        {
            stop_flag.store(true);
            events.enqueue([]{}); // just to wake up thread
            scheduler.join();
            if (graceful)
            {
                // Do the same cycle as schedulerThread() but never block or wait postponed events
                while (true)
                {
                    if (events.forceProcess()) // Priority 0: process events
                        continue;
                    else if (decrease) // Priority 1: process decrease requests
                        approveDecrease();
                    else if (increase) // Priority 2: process increase requests
                        approveIncrease();
                    else
                        break; // No more work to do
                }
            }
        }
    }

    std::string_view getTypeName() const override { return "scheduler"; }

    void attachChild(const std::shared_ptr<ISchedulerNode> & child_) override
    {
        SpaceSharedNodePtr child = std::static_pointer_cast<ISpaceSharedNode>(child_);
        if (auto [it, inserted] = children.emplace(child->basename, child); !inserted)
            throw Exception(
                ErrorCodes::INVALID_SCHEDULER_NODE,
                "Can't add another child with the same path: {}",
                it->second->getPath());
        child->setParentNode(this);
        child->updateMinMaxAllocated(min_max_allocated);
        Update update = Update().setAttached(child.get());
        apply(update);
        refreshRequests();
    }

    void removeChild(ISchedulerNode * child_) override
    {
        if (auto iter = children.find(child_->basename); iter != children.end())
        {
            SpaceSharedNodePtr child = iter->second;
            Update update = Update().setDetached(child.get());
            apply(update);
            child->setParentNode(nullptr);
            child->updateMinMaxAllocated(std::numeric_limits<ResourceCost>::max());
            children.erase(iter);
            refreshRequests();
        }
    }

    ISchedulerNode * getChild(const String & child_name) override
    {
        if (auto iter = children.find(child_name); iter != children.end())
            return iter->second.get();
        return nullptr;
    }

    void propagateUpdate(ISpaceSharedNode &, Update && update) override
    {
        // A child's pending requests changed. Aggregate the attach/detach effect and re-pick the
        // next requests to process. The scheduler is the root and has no parent to propagate to.
        apply(update);
        refreshRequests();
    }

    void approveIncrease() override
    {
        chassert(increase && increase_child);
        apply(*increase);
        increase = nullptr;
        ISpaceSharedNode * child = increase_child;
        increase_child = nullptr;
        increase_cursor = child->basename; // advance round-robin past the served child
        child->approveIncrease();
        refreshRequests();
    }

    void approveDecrease() override
    {
        chassert(decrease && decrease_child);
        apply(*decrease);
        decrease = nullptr;
        ISpaceSharedNode * child = decrease_child;
        decrease_child = nullptr;
        decrease_cursor = child->basename; // advance round-robin past the served child
        child->approveDecrease();
        refreshRequests();
    }

    ResourceAllocation * selectAllocationToKill(IncreaseRequest &, ResourceCost, String &) override
    {
        // The scheduler has no limit of its own, so it never initiates a kill: limits (and thus
        // kills) live inside the trees at AllocationLimit nodes.
        chassert(false);
        return nullptr;
    }

    void updateMinMaxAllocated(ResourceCost new_value) override
    {
        min_max_allocated = new_value;
        for (auto & [name, child] : children)
            child->updateMinMaxAllocated(min_max_allocated);
    }

private:
    void schedulerThread(ThreadName name)
    {
        setThreadName(name);
        EventQueue::SchedulerThread scheduler_thread(&events);
        while (!stop_flag.load())
        {
            if (events.tryProcess()) // Priority 0: process events
                continue;
            else if (decrease) // Priority 1: process decrease requests
                approveDecrease();
            else if (increase) // Priority 2: process increase requests
                approveIncrease();
            else // Block until any event happens
                events.process();
        }
    }

    // Re-pick the next decrease and increase requests to process from the children, round-robin.
    // Decrease requests (freeing resources) have priority over increase requests, matching
    // schedulerThread(). Children are name-ordered; scanning starts right after the last served
    // child (the cursor) and wraps around, so no tree is starved.
    void refreshRequests()
    {
        decrease_child = pickRoundRobin(decrease_cursor, /*want_increase=*/ false);
        decrease = decrease_child ? decrease_child->decrease : nullptr;
        increase_child = pickRoundRobin(increase_cursor, /*want_increase=*/ true);
        increase = increase_child ? increase_child->increase : nullptr;
    }

    ISpaceSharedNode * pickRoundRobin(const String & cursor, bool want_increase)
    {
        ISpaceSharedNode * first = nullptr; // first child with a pending request (wrap-around target)
        for (auto & [name, child] : children) // std::map iterates in ascending name order
        {
            if (want_increase ? (child->increase == nullptr) : (child->decrease == nullptr))
                continue;
            if (!first)
                first = child.get();
            if (name > cursor)
                return child.get(); // first match strictly after the cursor
        }
        return first; // wrap around (or nullptr if no child has a pending request)
    }

    ISpaceSharedNode * increase_child = nullptr; // child owning the current `increase`
    ISpaceSharedNode * decrease_child = nullptr; // child owning the current `decrease`
    String increase_cursor; // basename of the last child served an increase (round-robin position)
    String decrease_cursor; // basename of the last child served a decrease (round-robin position)

    std::atomic<bool> stop_flag = false;
    EventQueue events;
    /// Children by name (name-ordered for round-robin). Must be destroyed before `events` because
    /// the destructor of ISchedulerNode might access the mutex in that queue.
    std::map<String, SpaceSharedNodePtr> children;
    ThreadFromGlobalPool scheduler;
};

}
