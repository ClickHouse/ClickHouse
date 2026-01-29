#pragma once

#include <base/defines.h>

#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/Scheduler/ISpaceSharedNode.h>

#include <atomic>


namespace DB
{

/// Resource scheduler root node with a dedicated thread.
/// May have the only child corresponding to the managed scheduling hierarchy root
/// (usually, WorkloadNode for workload `all`).
class SpaceSharedScheduler final : public ISpaceSharedNode
{
public:
    explicit SpaceSharedScheduler()
        : ISpaceSharedNode(events)
    {}

    ~SpaceSharedScheduler() override
    {
        stop();
        if (child)
            removeChild(child.get());
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

    const String & getTypeName() const override
    {
        static String type_name("scheduler");
        return type_name;
    }

    bool equals(ISchedulerNode *) override
    {
        chassert(false);
        return false;
    }

    void attachChild(const std::shared_ptr<ISchedulerNode> & child_) override
    {
        child = std::static_pointer_cast<ISpaceSharedNode>(child_);
        child->setParentNode(this);
        propagateUpdate(*child, Update()
            .setAttached(child.get())
            .setIncrease(child->increase)
            .setDecrease(child->decrease));
    }

    void removeChild(ISchedulerNode * child_) override
    {
        if (child.get() != child_)
            return;
        propagateUpdate(*child, Update()
            .setDetached(child.get())
            .setIncrease(nullptr)
            .setDecrease(nullptr));
        child->setParentNode(nullptr);
        child.reset();
    }

    ISchedulerNode * getChild(const String & child_name) override
    {
        if (child->basename == child_name)
            return child.get();
        return nullptr;
    }

    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override
    {
        chassert(&from_child == child.get());
        apply(update);
        if (update.increase)
            increase = *update.increase;
        if (update.decrease)
            decrease = *update.decrease;
    }

    void approveIncrease() override
    {
        chassert(increase);
        apply(*increase);
        increase = nullptr;
        child->approveIncrease();
        increase = child->increase;
    }

    void approveDecrease() override
    {
        chassert(decrease);
        apply(*decrease);
        decrease = nullptr;
        child->approveDecrease();
        decrease = child->decrease;
    }

    ResourceAllocation * selectAllocationToKill(IncreaseRequest &, ResourceCost, String &) override
    {
        chassert(false);
        return nullptr;
    }

private:
    void schedulerThread(ThreadName name)
    {
        setThreadName(name);
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

    SpaceSharedNodePtr child;

    std::atomic<bool> stop_flag = false;
    EventQueue events;
    ThreadFromGlobalPool scheduler;
};

}
