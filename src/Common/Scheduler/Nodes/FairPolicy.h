#pragma once

#include <Common/Scheduler/ISchedulerNode.h>

#include <Common/Stopwatch.h>

#include <algorithm>
#include <optional>
#include <unordered_map>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_SCHEDULER_NODE;
}

/*
 * Scheduler node that implements weight-based fair scheduling policy.
 * Based on Start-time Fair Queueing (SFQ) algorithm.
 *
 * Algorithm description.
 * Virtual runtime (total consumed cost divided by child weight) is tracked for every child.
 * Active child with minimum vruntime is selected to be dequeued next. On activation, initial vruntime
 * of a child is set to vruntime of "start" of the last request. This guarantees immediate processing
 * of at least single request of newly activated children and thus best isolation and scheduling latency.
 */
class FairPolicy : public ISchedulerNode
{
    /// Scheduling state of a child
    struct Item
    {
        ISchedulerNode * child = nullptr;
        double vruntime = 0; /// total consumed cost divided by child weight

        /// For min-heap by vruntime
        bool operator<(const Item & rhs) const noexcept
        {
            return vruntime > rhs.vruntime;
        }
    };

public:
    explicit FairPolicy(EventQueue * event_queue_, const Poco::Util::AbstractConfiguration & config = emptyConfig(), const String & config_prefix = {})
        : ISchedulerNode(event_queue_, config, config_prefix)
    {}

    bool equals(ISchedulerNode * other) override
    {
        if (!ISchedulerNode::equals(other))
            return false;
        if (auto * _ = dynamic_cast<FairPolicy *>(other))
            return true;
        return false;
    }

    void attachChild(const SchedulerNodePtr & child) override
    {
        // Take ownership
        if (auto [it, inserted] = children.emplace(child->basename, child); !inserted)
            throw Exception(
                ErrorCodes::INVALID_SCHEDULER_NODE,
                "Can't add another child with the same path: {}",
                it->second->getPath());

        // Attach
        child->setParent(this);

        // At first attach as inactive child.
        // Inactive attached child must have `info.parent.idx` equal it's index inside `items` array.
        // This is needed to avoid later scanning through inactive `items` in O(N). Important optimization.
        // NOTE: vruntime must be equal to `system_vruntime` for fairness.
        child->info.parent.idx = items.size();
        items.emplace_back(Item{child.get(), system_vruntime});

        // Activate child if it is not empty
        if (child->isActive())
            activateChildImpl(items.size() - 1);
    }

    void removeChild(ISchedulerNode * child) override
    {
        if (auto iter = children.find(child->basename); iter != children.end())
        {
            SchedulerNodePtr removed = iter->second;

            // Deactivate: detach is not very common operation, so we can afford O(N) here
            size_t child_idx = 0;
            [[ maybe_unused ]] bool found = false;
            for (; child_idx != items.size(); child_idx++)
            {
                if (items[child_idx].child == removed.get())
                {
                    found = true;
                    break;
                }
            }
            assert(found);
            if (child_idx < heap_size) // Detach of active child requires deactivation at first
            {
                heap_size--;
                std::swap(items[child_idx], items[heap_size]);
                // Element was removed from inside of heap -- heap must be rebuilt
                std::make_heap(items.begin(), items.begin() + heap_size);
                child_idx = heap_size;
            }

            // Now detach inactive child
            if (child_idx != items.size() - 1)
            {
                std::swap(items[child_idx], items.back());
                items[child_idx].child->info.parent.idx = child_idx;
            }
            items.pop_back();

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
            if (heap_size == 0)
                return {nullptr, false};

            // Recursively pull request from child
            auto [request, child_active] = items.front().child->dequeueRequest();
            std::pop_heap(items.begin(), items.begin() + heap_size);
            Item & current = items[heap_size - 1];

            if (request)
            {
                // SFQ fairness invariant: system vruntime equals last served request start-time
                assert(current.vruntime >= system_vruntime);
                system_vruntime = current.vruntime;

                // By definition vruntime is amount of consumed resource (cost) divided by weight
                current.vruntime += double(request->cost) / current.child->info.weight;
                max_vruntime = std::max(max_vruntime, current.vruntime);
            }

            if (child_active) // Put active child back in heap after vruntime update
            {
                std::push_heap(items.begin(), items.begin() + heap_size);
            }
            else // Deactivate child if it is empty, but remember it's vruntime for latter activations
            {
                heap_size--;

                // Store index of this inactive child in `parent.idx`
                // This enables O(1) search of inactive children instead of O(n)
                current.child->info.parent.idx = heap_size;
            }

            // Reset any difference between children on busy period end
            if (heap_size == 0)
            {
                // Reset vtime to zero to avoid floating-point error accumulation,
                // but do not reset too often, because it's O(N)
                UInt64 ns = clock_gettime_ns();
                if (last_reset_ns + 1000000000 < ns)
                {
                    last_reset_ns = ns;
                    for (Item & item : items)
                        item.vruntime = 0;
                    max_vruntime = 0;
                }
                system_vruntime = max_vruntime;
                busy_periods++;
            }

            if (request)
            {
                incrementDequeued(request->cost);
                return {request, heap_size > 0};
            }
        }
    }

    bool isActive() override
    {
        return heap_size > 0;
    }

    size_t activeChildren() override
    {
        return heap_size;
    }

    void activateChild(ISchedulerNode * child) override
    {
        // Find this child; this is O(1), thanks to inactive index we hold in `parent.idx`
        activateChildImpl(child->info.parent.idx);
    }

    // For introspection
    double getSystemVRuntime() const
    {
        return system_vruntime;
    }

    std::optional<double> getChildVRuntime(ISchedulerNode * child) const
    {
        for (const auto & item : items)
        {
            if (child == item.child)
                return item.vruntime;
        }
        return std::nullopt;
    }

private:
    void activateChildImpl(size_t inactive_idx)
    {
        bool activate_parent = heap_size == 0;

        if (heap_size != inactive_idx)
        {
            std::swap(items[heap_size], items[inactive_idx]);
            items[inactive_idx].child->info.parent.idx = inactive_idx;
        }

        // Newly activated child should have at least `system_vruntime` to keep fairness
        items[heap_size].vruntime = std::max(system_vruntime, items[heap_size].vruntime);
        heap_size++;
        std::push_heap(items.begin(), items.begin() + heap_size);

        // Recursive activation
        if (activate_parent && parent)
            parent->activateChild(this);
    }

    /// Beginning of `items` vector is heap of active children: [0; `heap_size`).
    /// Next go inactive children in unsorted order.
    /// NOTE: we have to track vruntime of inactive children for max-min fairness.
    std::vector<Item> items;
    size_t heap_size = 0;

    /// Last request vruntime
    double system_vruntime = 0;
    double max_vruntime = 0;
    UInt64 last_reset_ns = 0;

    /// All children with ownership
    std::unordered_map<String, SchedulerNodePtr> children; // basename -> child
};

}
