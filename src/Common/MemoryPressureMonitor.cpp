#include <Common/MemoryPressureMonitor.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <base/defines.h>

#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

MemoryPressureLevel PressureLevelMachine::sample(double pressure, uint64_t now_ns)
{
    std::lock_guard lk(mutex);

    const double t1 = threshold_l1 / 100.0;
    const double t2 = threshold_l2 / 100.0;
    const double t3 = threshold_l3 / 100.0;

    uint8_t raw_level = 0;
    if (pressure >= t3)
        raw_level = 3;
    else if (pressure >= t2)
        raw_level = 2;
    else if (pressure >= t1)
        raw_level = 1;

    if (raw_level >= level)
    {
        /// Snap up immediately; refresh the "still elevated" timestamp.
        level = raw_level;
        last_at_or_above_ns = now_ns;
    }
    else if (level > 0 && now_ns >= last_at_or_above_ns + COOLDOWN_NS)
    {
        /// Step down by ONE level per cooldown — a CRITICAL → NORMAL recovery
        /// needs ≥ 3 × COOLDOWN_NS of sustained low pressure.
        level -= 1;
        last_at_or_above_ns = now_ns;
    }

    chassert(level <= 3);
    /// NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
    return static_cast<MemoryPressureLevel>(level);
}

void PressureLevelMachine::setThresholds(UInt64 l1_pct, UInt64 l2_pct, UInt64 l3_pct)
{
    /// Validate range and ordering loudly. Silent clamping/sorting (the
    /// previous behavior) hid config typos — e.g. `300` would wrap to `44`
    /// through `uint8_t` at the call site and then look valid here, so the
    /// server would silently apply a wrong threshold ladder.
    if (l1_pct > 100 || l2_pct > 100 || l3_pct > 100)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Memory pressure thresholds must be in [0, 100], got "
            "level_1={}, level_2={}, level_3={}",
            l1_pct, l2_pct, l3_pct);
    if (!(l1_pct <= l2_pct && l2_pct <= l3_pct))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Memory pressure thresholds must satisfy level_1 <= level_2 <= level_3, got "
            "level_1={}, level_2={}, level_3={}",
            l1_pct, l2_pct, l3_pct);

    std::lock_guard lk(mutex);
    threshold_l1 = static_cast<uint8_t>(l1_pct);
    threshold_l2 = static_cast<uint8_t>(l2_pct);
    threshold_l3 = static_cast<uint8_t>(l3_pct);
}

MemoryPressureThresholds PressureLevelMachine::getThresholds() const
{
    std::lock_guard lk(mutex);
    return {threshold_l1, threshold_l2, threshold_l3};
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
