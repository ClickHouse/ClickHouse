#pragma once

#include <memory>
#include <Common/Scheduler/ISpaceSharedNode.h>
#include <Common/Scheduler/IAllocationQueue.h>
#include <Common/Exception.h>
#include <Common/ErrorCodes.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int RESOURCE_LIMIT_EXCEEDED;
}

/// Limits the total amount of allocated resource by all the children nodes.
class AllocationLimit final : public ISpaceSharedNode
{
    static constexpr ResourceCost default_max_allocated = std::numeric_limits<ResourceCost>::max();
public:
    AllocationLimit(EventQueue & event_queue_, const SchedulerNodeInfo & info_, ResourceCost max_allocated_)
        : ISpaceSharedNode(event_queue_, info_)
        , max_allocated(max_allocated_)
    {}

    ~AllocationLimit() override
    {
        // We need to clear `parent` in child to avoid dangling references
        if (child)
            removeChild(child.get());
    }

    void updateLimit(UInt64 new_max_allocated)
    {
        /// TODO(serxa): Update limit.
        UNUSED(new_max_allocated);
    }

    const String & getTypeName() const override
    {
        static String type_name("allocation_limit");
        return type_name;
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

    ResourceAllocation * selectAllocationToKill() override
    {
        return child->selectAllocationToKill();
    }

    void approveIncrease() override
    {
        applyIncrease();
        child->approveIncrease();
        setIncrease(child->increase, false);
    }

    void approveDecrease() override
    {
        applyDecrease();
        child->approveDecrease();
        setDecrease(child->decrease);
    }

    ResourceCost getLimit() const
    {
        return max_allocated;
    }

    void propagateUpdate(ISpaceSharedNode & from_child, Update update) override
    {
        chassert(&from_child == child.get());
        chassert(parent);
        bool reapply_constraint = false;
        if (update.attached)
        {
            allocated += update.attached->allocated;
            reapply_constraint = true;
        }
        if (update.detached)
        {
            allocated -= update.detached->allocated;
            // In case of a queue purge we may need to clear allocation_to_kill
            if (allocation_to_kill && update.detached == static_cast<ISpaceSharedNode *>(&allocation_to_kill->queue))
                allocation_to_kill = nullptr;
            reapply_constraint = true;
        }
        if (update.increase || reapply_constraint)
        {
            if (setIncrease(update.increase ? *update.increase : increase, reapply_constraint))
                update.setIncrease(increase);
            else
                update.resetIncrease();
        }
        if (update.decrease)
        {
            if (setDecrease(*update.decrease))
                update.setDecrease(decrease);
            else
                update.resetDecrease();
        }
        if (update)
            castParent().propagateUpdate(*this, update);
    }

private:
    bool setIncrease(IncreaseRequest * new_increase, bool reapply_constraint)
    {
        if (!reapply_constraint && increase == new_increase)
            return false;
        IncreaseRequest * old_increase = increase;
        if (new_increase)
        {
            if (allocated + new_increase->size > max_allocated)
            {
                // Limit would be violated, so we have to reclaim resource
                if (!allocation_to_kill)
                {
                    if (ResourceAllocation * candidate_to_kill = selectAllocationToKill())
                    {
                        // It is important not to kill allocation due to pending allocation in the same queue
                        if (!new_increase->pending_allocation // Kill due to running allocation increase
                            || &candidate_to_kill->queue != &new_increase->allocation->queue) // Or kills allocation from a different queue
                        {
                            allocation_to_kill = candidate_to_kill;
                            allocation_to_kill->killAllocation(std::make_exception_ptr(
                                Exception(ErrorCodes::RESOURCE_LIMIT_EXCEEDED,
                                    "Resource limit exceeded"))); // TODO(serxa): add limit details, resource name, path
                        }
                    }
                }
                // Block until there is enough resource to process child's increase request
                increase = nullptr;
            }
            else
                increase = child->increase; // Can safely process child's increase request
        }
        else
            increase = nullptr; // No more increase requests

        return increase != old_increase;
    }

    bool setDecrease(DecreaseRequest * new_decrease)
    {
        if (decrease == new_decrease)
            return false;
        decrease = new_decrease;
        return true;
    }

    void applyIncrease()
    {
        chassert(increase);
        allocated += increase->size;
        increase = nullptr;
    }

    void applyDecrease()
    {
        chassert(decrease);
        allocated -= decrease->size;

        // Check if allocation being killed released all its resources
        if (decrease->allocation == allocation_to_kill && decrease->removing_allocation)
            allocation_to_kill = nullptr;

        // Check if we can now process pending increase request
        bool increase_set = setIncrease(child->increase, true);
        if (increase_set)
            castParent().propagateUpdate(*this, Update().setIncrease(increase));

        decrease = nullptr;
    }

    ResourceCost max_allocated = default_max_allocated;

    /// Allocation that is being killed (if any)
    ResourceAllocation * allocation_to_kill = nullptr;

    SpaceSharedNodePtr child;
};

}
