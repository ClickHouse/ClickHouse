#pragma once

#include <Common/Scheduler/ISchedulerNode.h>

#include <algorithm>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

/*
 * Scheduler node that implements priority scheduling policy.
 * Requests are scheduled in order of priorities.
 */
class PriorityPolicy : public ISchedulerNode
{
    /// Scheduling state of a child
    struct Item
    {
        ISchedulerNode * child = nullptr;
        Priority priority; // lower value means higher priority

        /// For max-heap by priority
        bool operator<(const Item& rhs) const noexcept
        {
            return priority > rhs.priority; // Reversed for heap top to yield highest priority (lowest value) child first
        }
    };

public:
    explicit PriorityPolicy(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    bool equals(ISchedulerNode * other) override
    {
        if (!ISchedulerNode::equals(other))
            return false;
        if (auto * _ = dynamic_cast<PriorityPolicy *>(other))
            return true;
        return false;
    }

    void attachChild(const SchedulerNodePtr & child) override
    {
        // Take ownership
        chassert(child->parent == nullptr);
        if (auto [it, inserted] = children.emplace(child->basename, child); !inserted)
            throw Exception(
                ErrorCodes::INVALID_SCHEDULER_NODE,
                "Can't add another child with the same path: {}",
                it->second->getPath());

        // Attach
        child->setParent(this);

        // Activate child if it is not empty
        if (child->isActive())
            activateChild(child.get());
    }

    void removeChild(ISchedulerNode * child) override
    {
        if (auto iter = children.find(child->basename); iter != children.end())
        {
            SchedulerNodePtr removed = iter->second;

            // Deactivate: detach is not very common operation, so we can afford O(N) here
            for (auto i = items.begin(), e = items.end(); i != e; ++i)
            {
                if (i->child == removed.get())
                {
                    items.erase(i);
                    // Element was removed from inside of heap -- heap must be rebuilt
                    std::make_heap(items.begin(), items.end());
                    break;
                }
            }

            // Detach
            removed->setParent(nullptr);

            // Get rid of ownership
            children.erase(iter);
        }
    }

    ISchedulerNode * getChild(const String & child_name) override
    {
        if (auto iter = children.find(child_name); iter != children.end())
            return iter->second.get();
        return nullptr;
    }

    std::pair<ResourceRequest *, bool> dequeueRequest() override
    {
        // Cycle is required to do deactivations in the case of canceled requests, when dequeueRequest returns `nullptr`
        while (true)
        {
            if (items.empty())
                return {nullptr, false};

            // Recursively pull request from child
            auto [request, child_active] = items.front().child->dequeueRequest();

            // Deactivate child if it is empty
            if (!child_active)
            {
                std::pop_heap(items.begin(), items.end());
                items.pop_back();
                if (items.empty())
                    busy_periods++;
            }

            if (request)
            {
                incrementDequeued(request->cost);
                return {request, !items.empty()};
            }
        }
    }

    bool isActive() override
    {
        return !items.empty();
    }

    size_t activeChildren() override
    {
        return items.size();
    }

    void activateChild(ISchedulerNode * child) override
    {
        bool activate_parent = items.empty();
        items.emplace_back(Item{child, child->info.priority});
        std::push_heap(items.begin(), items.end());
        if (activate_parent && parent)
            parent->activateChild(this);
    }

private:
    /// Heap of active children
    std::vector<Item> items;

    /// All children with ownership
    std::unordered_map<String, SchedulerNodePtr> children; // basename -> child
};

}
