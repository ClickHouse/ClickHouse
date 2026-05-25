#include <Common/MemoryPressureMonitor.h>
#include <Common/MemoryTracker.h>

#include <algorithm>
#include <chrono>

namespace DB
{

namespace
{

double defaultPressure()
{
    const Int64 limit = total_memory_tracker.getHardLimit();
    if (limit <= 0)
        return 0.0;
    const Int64 used = total_memory_tracker.get();
    if (used <= 0)
        return 0.0;
    return static_cast<double>(used) / static_cast<double>(limit);
}

uint64_t defaultNow()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}

MemoryPressureMonitor::MemoryPressureMonitor()
    : read_pressure(defaultPressure)
    , read_now_ns(defaultNow)
{
}

MemoryPressureMonitor & MemoryPressureMonitor::instance()
{
    static MemoryPressureMonitor monitor;
    return monitor;
}

void MemoryPressureMonitor::setThresholds(uint8_t l1_pct, uint8_t l2_pct, uint8_t l3_pct)
{
    /// Clamp to (0, 100] and re-sort, so a misconfigured server still gets a
    /// monotonic threshold ladder rather than silently degenerating to "always
    /// at L0" or "always at L3".
    uint8_t v[3] = {
        static_cast<uint8_t>(std::clamp<int>(l1_pct, 1, 100)),
        static_cast<uint8_t>(std::clamp<int>(l2_pct, 1, 100)),
        static_cast<uint8_t>(std::clamp<int>(l3_pct, 1, 100)),
    };
    std::sort(std::begin(v), std::end(v));
    threshold_l1.store(v[0], std::memory_order_relaxed);
    threshold_l2.store(v[1], std::memory_order_relaxed);
    threshold_l3.store(v[2], std::memory_order_relaxed);
}

MemoryPressureMonitor::LevelConfig MemoryPressureMonitor::effective()
{
    const double pressure = read_pressure();
    const double t1 = threshold_l1.load(std::memory_order_relaxed) / 100.0;
    const double t2 = threshold_l2.load(std::memory_order_relaxed) / 100.0;
    const double t3 = threshold_l3.load(std::memory_order_relaxed) / 100.0;

    int raw_level = 0;
    if (pressure >= t3)
        raw_level = 3;
    else if (pressure >= t2)
        raw_level = 2;
    else if (pressure >= t1)
        raw_level = 1;

    const int cur = level.load(std::memory_order_relaxed);
    const uint64_t now = read_now_ns();

    if (raw_level >= cur)
    {
        /// Snap up immediately; refresh the "still elevated" timestamp.
        if (raw_level > cur)
            level.store(raw_level, std::memory_order_relaxed);
        last_at_or_above_ns.store(now, std::memory_order_relaxed);
    }
    else if (cur > 0)
    {
        /// Step down by ONE level per cooldown — a CRITICAL → NORMAL recovery
        /// therefore needs ≥ 3 × COOLDOWN_NS of sustained low pressure. That's
        /// intentional: bursty allocators that briefly drop below the threshold
        /// should not immediately get full window back.
        const uint64_t last = last_at_or_above_ns.load(std::memory_order_relaxed);
        if (now >= last + COOLDOWN_NS)
        {
            level.store(cur - 1, std::memory_order_relaxed);
            last_at_or_above_ns.store(now, std::memory_order_relaxed);
        }
    }

    return CONFIGS[level.load(std::memory_order_relaxed)];
}

void MemoryPressureMonitor::resetForTesting(PressureFn pressure, ClockFn clock)
{
    read_pressure = std::move(pressure);
    read_now_ns = std::move(clock);
    level.store(0, std::memory_order_relaxed);
    last_at_or_above_ns.store(0, std::memory_order_relaxed);
    threshold_l1.store(75, std::memory_order_relaxed);
    threshold_l2.store(90, std::memory_order_relaxed);
    threshold_l3.store(95, std::memory_order_relaxed);
}

}
