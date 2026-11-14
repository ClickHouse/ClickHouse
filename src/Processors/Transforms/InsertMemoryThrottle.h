#pragma once

#include <atomic>
#include <memory>

#include <base/types.h>
#include <Common/MemoryTracker.h>


namespace DB
{

/// Memory snapshot to avoid passing MemoryTracker everywhere
struct MemorySnapshot
{
    Int64 used_bytes = 0;
    Int64 hard_limit_bytes = 0;   /// 0 -> unlimited
};

/// query-level memory information
class IQueryMemoryProvider
{
public:
    virtual MemorySnapshot get() const = 0;
    virtual ~IQueryMemoryProvider() = default;
};

/// default implementation using MemoryTracker hierarchy
class QueryMemoryProvider : public IQueryMemoryProvider
{
public:
    explicit QueryMemoryProvider(MemoryTracker * tracker_) : tracker(tracker_) {}

    MemorySnapshot get() const override;

private:
    MemoryTracker * tracker = nullptr;
};


/**
 * Implements memory-based throttling for INSERT pipelines
 *
 * uses high/low thresholds to determine throttling state:
 * - when memory usage crosses high_threshold → enable throttling
 * - when memory usage drops below low_threshold → disable throttling
 *
 * When throttled, BalancingTransform creates backpressure by not pulling
 * from upstream, reducing the number of active threads
 */
class InsertMemoryThrottle
{
public:
    struct Settings
    {
        bool enabled = false;

        double high_threshold = 0.85; /// enable throttling
        double low_threshold  = 0.75; /// disable throttling
    };

    InsertMemoryThrottle(Settings settings_, std::shared_ptr<IQueryMemoryProvider> mem_provider_)
        : settings(std::move(settings_)), mem_provider(std::move(mem_provider_))
    {}

    bool isThrottled();

    bool isEnabled() const { return settings.enabled; }

    /// diagnostics/metrics
    struct Snapshot
    {
        MemorySnapshot memory;
        bool throttled = false;
    };

    Snapshot getSnapshot() const;

private:
    Settings settings;
    std::shared_ptr<IQueryMemoryProvider> mem_provider;
    std::atomic<bool> throttled{false};
};

using InsertMemoryThrottlePtr = std::shared_ptr<InsertMemoryThrottle>;

}
