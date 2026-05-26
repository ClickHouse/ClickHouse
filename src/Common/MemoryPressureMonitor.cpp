#include <Common/MemoryPressureMonitor.h>
#include <Common/MemoryTracker.h>

#include <algorithm>
#include <chrono>

namespace DB
{

MemoryPressureLevel PressureLevelMachine::sample(double pressure, uint64_t now_ns)
{
    const double t1 = threshold_l1.load(std::memory_order_relaxed) / 100.0;
    const double t2 = threshold_l2.load(std::memory_order_relaxed) / 100.0;
    const double t3 = threshold_l3.load(std::memory_order_relaxed) / 100.0;

    uint8_t raw_level = 0;
    if (pressure >= t3)
        raw_level = 3;
    else if (pressure >= t2)
        raw_level = 2;
    else if (pressure >= t1)
        raw_level = 1;

    const uint8_t cur = level.load(std::memory_order_relaxed);

    if (raw_level >= cur)
    {
        /// Snap up immediately; refresh the "still elevated" timestamp.
        if (raw_level > cur)
            level.store(raw_level, std::memory_order_relaxed);
        last_at_or_above_ns.store(now_ns, std::memory_order_relaxed);
    }
    else if (cur > 0)
    {
        /// Step down by ONE level per cooldown — a CRITICAL → NORMAL recovery
        /// therefore needs ≥ 3 × COOLDOWN_NS of sustained low pressure.
        const uint64_t last = last_at_or_above_ns.load(std::memory_order_relaxed);
        if (now_ns >= last + COOLDOWN_NS)
        {
            level.store(cur - 1, std::memory_order_relaxed);
            last_at_or_above_ns.store(now_ns, std::memory_order_relaxed);
        }
    }

    return static_cast<MemoryPressureLevel>(level.load(std::memory_order_relaxed));
}

void PressureLevelMachine::setThresholds(uint8_t l1_pct, uint8_t l2_pct, uint8_t l3_pct)
{
    /// Clamp to (0, 100] and re-sort so a misconfigured server still gets a
    /// monotonic threshold ladder rather than silently degenerating to "always
    /// at Normal" or "always at Critical".
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

namespace
{

double readTotalPressure()
{
    const Int64 limit = total_memory_tracker.getHardLimit();
    if (limit <= 0)
        return 0.0;
    const Int64 used = total_memory_tracker.get();
    if (used <= 0)
        return 0.0;
    return static_cast<double>(used) / static_cast<double>(limit);
}

uint64_t steadyNowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}

MemoryPressureLevel MemoryPressureMonitor::currentLevel()
{
    return machine.sample(readTotalPressure(), steadyNowNs());
}

MemoryPressureLevel FakeMemoryPressureMonitor::currentLevel()
{
    return machine.sample(
        pressure.load(std::memory_order_relaxed),
        now_ns.load(std::memory_order_relaxed));
}

namespace
{

/// Atomic pointer to the active monitor. Production never writes after
/// first init; tests swap it via `ScopedMemoryPressureMonitor`.
std::atomic<IMemoryPressureMonitor *> & activeMonitorSlot()
{
    static std::atomic<IMemoryPressureMonitor *> slot{nullptr};
    return slot;
}

MemoryPressureMonitor & productionInstance()
{
    static MemoryPressureMonitor instance;
    return instance;
}

}

IMemoryPressureMonitor & memoryPressureMonitor()
{
    auto & slot = activeMonitorSlot();
    auto * current = slot.load(std::memory_order_acquire);
    if (current)
        return *current;

    /// First access: install the production singleton. Concurrent first
    /// callers all reference the same Meyers singleton; `compare_exchange`
    /// ensures only one wins the slot, the rest pick up that value.
    auto * production = &productionInstance();
    IMemoryPressureMonitor * expected = nullptr;
    slot.compare_exchange_strong(expected, production, std::memory_order_acq_rel);
    return *slot.load(std::memory_order_acquire);
}

ScopedMemoryPressureMonitor::ScopedMemoryPressureMonitor(IMemoryPressureMonitor & override_monitor)
{
    /// Force the production singleton to be constructed and slotted so the
    /// dtor can restore it even if nothing called `memoryPressureMonitor()`
    /// before this scope was entered.
    (void)memoryPressureMonitor();
    prior = activeMonitorSlot().exchange(&override_monitor, std::memory_order_acq_rel);
}

ScopedMemoryPressureMonitor::~ScopedMemoryPressureMonitor()
{
    activeMonitorSlot().store(prior, std::memory_order_release);
}

}
