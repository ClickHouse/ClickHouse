#include <Common/Scheduler/Nodes/SpaceShared/AllocationRateLimit.h>
#include <Common/Scheduler/Debug.h>

#include <algorithm>
#include <cmath>


namespace DB
{

AllocationRateLimit::AllocationRateLimit(EventQueue & event_queue_, const SchedulerNodeInfo & info_, double max_speed_, double max_burst_)
    : ISpaceSharedNode(event_queue_, info_)
    , max_speed(max_speed_)
    , max_burst(max_burst_)
    , last_update(event_queue_.now())
    , tokens(max_burst_)
{}

AllocationRateLimit::~AllocationRateLimit()
{
    // Cancel the postponed event to avoid a dangling reference from the event queue
    event_queue.cancelPostponed(postponed);

    // We need to clear `parent` in child to avoid dangling references
    if (child)
        removeChild(child.get());
}

std::string_view AllocationRateLimit::getTypeName() const { return "allocation_rate_limit"; }

void AllocationRateLimit::updateConstraints(double new_max_speed, double new_max_burst)
{
    event_queue.cancelPostponed(postponed);
    postponed = EventQueue::not_postponed;
    updateBucket(); // Apply previous parameters for the duration since `last_update`
    max_speed = new_max_speed;
    max_burst = new_max_burst;
    tokens = std::min(tokens, max_burst);
    if (setIncrease(child ? child->increase : nullptr) && parent)
        propagate(Update().setIncrease(increase));
}

double AllocationRateLimit::getTokens() const
{
    auto now = event_queue.now();
    double elapsed = static_cast<double>(std::chrono::nanoseconds(now - last_update).count()) / 1e9;
    return std::min(tokens + max_speed * elapsed, max_burst);
}

std::chrono::nanoseconds AllocationRateLimit::getThrottlingDuration() const
{
    return throttling_duration;
}

std::pair<double, double> AllocationRateLimit::getParams() const
{
    return {max_speed, max_burst};
}

void AllocationRateLimit::attachChild(const std::shared_ptr<ISchedulerNode> & child_)
{
    child = std::static_pointer_cast<ISpaceSharedNode>(child_);
    child->setParentNode(this);
    child->updateMinMaxAllocated(min_max_allocated);
    propagateUpdate(*child, Update()
        .setAttached(child.get())
        .setIncrease(child->increase)
        .setDecrease(child->decrease));
}

void AllocationRateLimit::removeChild(ISchedulerNode * child_)
{
    if (child.get() != child_)
        return;
    propagateUpdate(*child, Update()
        .setDetached(child.get())
        .setIncrease(nullptr)
        .setDecrease(nullptr));
    child->setParentNode(nullptr);
    child->updateMinMaxAllocated(std::numeric_limits<ResourceCost>::max());
    child.reset();
}

ISchedulerNode * AllocationRateLimit::getChild(const String & child_name)
{
    if (child && child->basename == child_name)
        return child.get();
    return nullptr;
}

ResourceAllocation * AllocationRateLimit::selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details)
{
    if (!child)
        return nullptr;
    return child->selectAllocationToKill(killer, limit, details);
}

ResourceAllocation * AllocationRateLimit::selectAllocationToSpill(ResourceCost at_least, String & details)
{
    if (!child)
        return nullptr;
    return child->selectAllocationToSpill(at_least, details);
}

void AllocationRateLimit::approveIncrease()
{
    chassert(increase);
    SCHED_DBG("{} -- approveIncrease(id={}, size={}, tokens={:.2f})", getPath(), increase->allocation.id, increase->size, tokens);
    apply(*increase);
    updateBucket(increase->size);
    increase = nullptr;
    child->approveIncrease();
    // The parent reads `increase` after this call returns, so no propagation is needed here
    setIncrease(child->increase);
}

void AllocationRateLimit::approveDecrease()
{
    chassert(decrease);
    SCHED_DBG("{} -- approveDecrease(id={}, size={})", getPath(), decrease->allocation.id, decrease->size);
    apply(*decrease);
    decrease = nullptr;
    child->approveDecrease();
    decrease = child->decrease;
    // Child's increase request might have changed without propagation (e.g. reordering of pending allocations)
    if (setIncrease(child->increase) && parent)
        propagate(Update().setIncrease(increase));
}

void AllocationRateLimit::propagateUpdate(ISpaceSharedNode & from_child, Update && update)
{
    SCHED_DBG("{} -- propagateUpdate(from_child={}, update={})", getPath(), from_child.basename, update.toString());
    chassert(&from_child == child.get());
    apply(update);
    if (update.decrease)
    {
        if (decrease != *update.decrease)
            decrease = *update.decrease;
        else
            update.resetDecrease();
    }
    if (update.increase)
    {
        if (setIncrease(*update.increase))
            update.setIncrease(increase);
        else
            update.resetIncrease();
    }
    if (parent && update)
        propagate(std::move(update));
}

void AllocationRateLimit::updateMinMaxAllocated(ResourceCost new_value)
{
    min_max_allocated = new_value;
    if (child)
        child->updateMinMaxAllocated(min_max_allocated);
}

bool AllocationRateLimit::setIncrease(IncreaseRequest * new_increase)
{
    IncreaseRequest * old_increase = increase;
    if (new_increase)
    {
        updateBucket();
        if (satisfied())
            increase = new_increase;
        else
        {
            increase = nullptr; // Block until the debt is repaid
            postponeIfNeeded();
        }
    }
    else
        increase = nullptr;
    return increase != old_increase;
}

void AllocationRateLimit::updateBucket(ResourceCost use)
{
    auto now = event_queue.now();
    if (max_speed > 0.0)
    {
        double elapsed = static_cast<double>(std::chrono::nanoseconds(now - last_update).count()) / 1e9;
        tokens = std::min(tokens + max_speed * elapsed, max_burst);
        // Consume outside of min() to avoid passing large increases without consuming tokens after a long idle period
        tokens -= static_cast<double>(use);
    }
    last_update = now;
}

void AllocationRateLimit::postponeIfNeeded()
{
    if (postponed != EventQueue::not_postponed || satisfied() || max_speed <= 0.0)
        return;
    // Round up, so the event never fires while the bucket is still (slightly) in debt
    auto delay_ns = saturatingDelay(std::ceil(-tokens / max_speed * 1e9));
    postponed = event_queue.postpone(
        std::chrono::time_point_cast<EventQueue::Duration>(event_queue.now() + delay_ns),
        [this] { onPostponed(); });
    // The total accumulates for the node's lifetime, so it saturates as well
    throttling_duration += std::min(delay_ns, std::chrono::nanoseconds::max() - throttling_duration);
}

std::chrono::nanoseconds AllocationRateLimit::saturatingDelay(double delay_ns)
{
    if (delay_ns < static_cast<double>(max_delay.count()))
        return std::chrono::nanoseconds(static_cast<Int64>(delay_ns));
    return max_delay;
}

void AllocationRateLimit::onPostponed()
{
    postponed = EventQueue::not_postponed;
    if (setIncrease(child ? child->increase : nullptr) && parent)
        propagate(Update().setIncrease(increase));
}

}
