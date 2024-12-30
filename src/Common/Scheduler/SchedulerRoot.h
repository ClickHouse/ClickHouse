#pragma once

#include <base/defines.h>

#include <Common/Stopwatch.h>
#include <Common/ThreadPool.h>

#include <Common/Scheduler/ISchedulerNode.h>
#include <Common/Scheduler/ISchedulerConstraint.h>

#include <Poco/Util/XMLConfiguration.h>

#include <unordered_map>
#include <map>
#include <memory>
#include <atomic>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

/*
 * Resource scheduler root node with a dedicated thread.
 * Immediate children correspond to different resources.
 */
class SchedulerRoot final : public ISchedulerNode
{
private:
    struct Resource
    {
        SchedulerNodePtr root;

        // Intrusive cyclic list of active resources
        Resource * next = nullptr;
        Resource * prev = nullptr;

        explicit Resource(const SchedulerNodePtr & root_)
            : root(root_)
        {
            root->info.parent.ptr = this;
        }

        // Get pointer stored by ctor in info
        static Resource * get(SchedulerNodeInfo & info)
        {
            return reinterpret_cast<Resource *>(info.parent.ptr);
        }
    };

public:
    SchedulerRoot()
        : ISchedulerNode(&events)
    {}

    ~SchedulerRoot() override
    {
        stop();
        while (!children.empty())
            removeChild(children.begin()->first);
    }

    /// Runs separate scheduler thread
    void start()
    {
        if (!scheduler.joinable())
            scheduler = ThreadFromGlobalPool([this] { schedulerThread(); });
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
                bool has_work = true;
                while (has_work)
                {
                    auto [request, _] = dequeueRequest();
                    if (request)
                        execute(request);
                    else
                        has_work = false;
                    while (events.forceProcess())
                        has_work = true;
                }
            }
        }
    }

    const String & getTypeName() const override
    {
        static String type_name("scheduler");
        return type_name;
    }

    bool equals(ISchedulerNode * other) override
    {
        if (!ISchedulerNode::equals(other))
            return false;
        if (auto * _ = dynamic_cast<SchedulerRoot *>(other))
            return true;
        return false;
    }

    void attachChild(const SchedulerNodePtr & child) override
    {
        // Take ownership
        assert(child->parent == nullptr);
        if (auto [it, inserted] = children.emplace(child.get(), child); !inserted)
            throw Exception(
                ErrorCodes::INVALID_SCHEDULER_NODE,
                "Can't add the same scheduler node twice");

        // Attach
        child->setParent(this);

        // Activate child if required
        if (child->isActive())
            activateChild(child.get());
    }

    void removeChild(ISchedulerNode * child) override
    {
        if (auto iter = children.find(child); iter != children.end())
        {
            SchedulerNodePtr removed = iter->second.root;

            // Deactivate if required
            deactivate(&iter->second);

            // Detach
            removed->setParent(nullptr);

            // Remove ownership
            children.erase(iter);
        }
    }

    ISchedulerNode * getChild(const String &) override
    {
        abort(); // scheduler is allowed to have multiple children with the same name
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        while (true)
        {
            if (current == nullptr) // No active resources
                return {nullptr, false};

            // Dequeue request from current resource
            auto [request, resource_active] = current->root->dequeueRequest();

            // Deactivate resource if required
            if (!resource_active)
                deactivate(current);
            else
                current = current->next; // Just move round-robin pointer

            if (request == nullptr) // Possible in case of request cancel, just retry
                continue;

            incrementDequeued(request->cost);
            return {request, current != nullptr};
        }
    }

    bool isActive() override
    {
        return current != nullptr;
    }

    size_t activeChildren() override
    {
        return 0;
    }

    void activateChild(ISchedulerNode * child) override
    {
        activate(Resource::get(child->info));
    }

private:
    void activate(Resource * value)
    {
        assert(value->next == nullptr && value->prev == nullptr);
        if (current == nullptr) // No active children
        {
            current = value;
            value->prev = value;
            value->next = value;
        }
        else
        {
            current->prev->next = value;
            value->prev = current->prev;
            current->prev = value;
            value->next = current;
        }
    }

    void deactivate(Resource * value)
    {
        if (value->next == nullptr)
            return; // Already deactivated
        assert(current != nullptr);
        if (current == value)
        {
            if (current->next == current) // We are going to remove the last active child
            {
                value->next = nullptr;
                value->prev = nullptr;
                current = nullptr;
                busy_periods++;
                return;
            }
            // Just move current to next to avoid invalidation
            current = current->next;
        }
        value->prev->next = value->next;
        value->next->prev = value->prev;
        value->prev = nullptr;
        value->next = nullptr;
    }

    void schedulerThread()
    {
        while (!stop_flag.load())
        {
            // Dequeue and execute single request
            auto [request, _] = dequeueRequest();
            if (request)
                execute(request);
            else // No more requests -- block until any event happens
                events.process();

            // Process all events before dequeuing to ensure fair competition
            while (events.tryProcess()) {}
        }
    }

    void execute(ResourceRequest * request)
    {
        request->execute();
    }

    Resource * current = nullptr; // round-robin pointer
    std::unordered_map<ISchedulerNode *, Resource> children; // resources by pointer
    std::atomic<bool> stop_flag = false;
    EventQueue events;
    ThreadFromGlobalPool scheduler;
};

}
