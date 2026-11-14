#pragma once

#include <Common/Scheduler/ISpaceSharedNode.h>

#include <chrono>


namespace DB
{

/// Limits the rate at which allocations under this node grow, using a token bucket.
/// Every approved increase consumes `increase.size` tokens; tokens are refilled at `max_speed` per second
/// up to `max_burst`. While the bucket is in debt (negative tokens), increase requests are blocked and
/// a postponed event re-evaluates the child's request once the debt is repaid.
/// Decreases are never throttled.
/// For the `QUERY` resource every allocation is a single slot, so this limits the rate of query admissions.
class AllocationRateLimit final : public ISpaceSharedNode
{
public:
    AllocationRateLimit(EventQueue & event_queue_, const SchedulerNodeInfo & info_, double max_speed_, double max_burst_);
    ~AllocationRateLimit() override;

    /// Updates the token bucket parameters. Must be called from the scheduler thread.
    void updateConstraints(double new_max_speed, double new_max_burst);

    /// Introspection
    double getTokens() const;
    std::chrono::nanoseconds getThrottlingDuration() const;
    std::pair<double, double> getParams() const;

    // ISchedulerNode
    std::string_view getTypeName() const override;
    void attachChild(const std::shared_ptr<ISchedulerNode> & child_) override;
    void removeChild(ISchedulerNode * child_) override;
    ISchedulerNode * getChild(const String & child_name) override;

    // ISpaceSharedNode
    ResourceAllocation * selectAllocationToKill(IncreaseRequest & killer, ResourceCost limit, String & details) override;
    ResourceAllocation * selectAllocationToSpill(ResourceCost at_least, String & details) override;
    void approveIncrease() override;
    void approveDecrease() override;
    void propagateUpdate(ISpaceSharedNode & from_child, Update && update) override;
    void updateMinMaxAllocated(ResourceCost new_value) override;

private:
    /// Passes the child's increase request through if the bucket is not in debt, otherwise blocks it.
    /// Returns true if `increase` was changed.
    bool setIncrease(IncreaseRequest * new_increase);

    /// Refills the bucket for the time elapsed since the last update and consumes `use` tokens.
    void updateBucket(ResourceCost use = 0);

    /// Schedules a postponed re-evaluation for the moment the debt is repaid (if not scheduled yet).
    void postponeIfNeeded();
    void onPostponed();

    bool satisfied() const { return tokens >= 0.0; }

    /// Converts a delay in nanoseconds to a duration, saturating at `max_delay`.
    /// A tiny `max_speed` may lead to a delay that does not fit into `Int64` nanoseconds.
    static std::chrono::nanoseconds saturatingDelay(double delay_ns);

    /// Postponing an admission for longer than this is meaningless; when the event fires early the delay is recomputed.
    static constexpr auto max_delay = std::chrono::nanoseconds(std::chrono::hours(24 * 365));

    double max_speed = 0; /// in tokens per second
    double max_burst = 0; /// in tokens

    EventQueue::TimePoint last_update;
    double tokens = 0; /// in ResourceCost units
    EventId postponed = EventQueue::not_postponed;
    std::chrono::nanoseconds throttling_duration{0};

    SpaceSharedNodePtr child;
};

}
