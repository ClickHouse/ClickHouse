#include <Common/MemoryPressureMonitor.h>
#include <Common/CurrentThread.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/VariableContext.h>
#include <base/defines.h>

#include <algorithm>
#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

uint8_t PressureLevelMachine::rawLevel(double pressure) const
{
    const uint32_t packed = thresholds_packed.load(std::memory_order_relaxed);
    const double t1 = ((packed >> 16) & 0xFFu) / 100.0;
    const double t2 = ((packed >> 8) & 0xFFu) / 100.0;
    const double t3 = (packed & 0xFFu) / 100.0;

    if (pressure >= t3)
        return 3;
    if (pressure >= t2)
        return 2;
    if (pressure >= t1)
        return 1;
    return 0;
}

MemoryPressureLevel PressureLevelMachine::levelForPressure(double pressure) const
{
    /// NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
    return static_cast<MemoryPressureLevel>(rawLevel(pressure));
}

MemoryPressureLevel PressureLevelMachine::sample(double pressure, uint64_t now_ns)
{
    std::lock_guard lk(mutex);
    /// Classify under the lock so a concurrent `setThresholds` can't swap the ladder mid-update.
    return stickUnlocked(rawLevel(pressure), now_ns);
}

MemoryPressureLevel PressureLevelMachine::stick(uint8_t raw_level, uint64_t now_ns)
{
    std::lock_guard lk(mutex);
    return stickUnlocked(raw_level, now_ns);
}

MemoryPressureLevel PressureLevelMachine::stickUnlocked(uint8_t raw_level, uint64_t now_ns)
{
    if (raw_level >= level)
    {
        /// Snap up immediately; refresh the "still elevated" timestamp.
        level = raw_level;
        last_at_or_above_ns = now_ns;
    }
    else if (level > 0 && now_ns >= last_at_or_above_ns + cooldown_ns)
    {
        /// Step down one level per cooldown, so a `Critical` -> `Normal` recovery needs >= 3 cooldowns.
        level -= 1;
        last_at_or_above_ns = now_ns;
    }

    chassert(level <= 3);
    /// NOLINTNEXTLINE(clang-analyzer-optin.core.EnumCastOutOfRange)
    return static_cast<MemoryPressureLevel>(level);
}

void validateMemoryPressureThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct)
{
    if (elevated_pct > 100 || high_pct > 100 || critical_pct > 100)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Memory pressure thresholds must be in [0, 100], got "
            "elevated={}, high={}, critical={}",
            elevated_pct, high_pct, critical_pct);
    if (!(elevated_pct <= high_pct && high_pct <= critical_pct))
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "Memory pressure thresholds must satisfy elevated <= high <= critical, got "
            "elevated={}, high={}, critical={}",
            elevated_pct, high_pct, critical_pct);
}

void PressureLevelMachine::setThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct)
{
    validateMemoryPressureThresholds(elevated_pct, high_pct, critical_pct);

    /// Pack all three so `rawLevel` never sees a half-applied ladder.
    const uint32_t packed = (static_cast<uint32_t>(elevated_pct) << 16)
                          | (static_cast<uint32_t>(high_pct) << 8)
                          | static_cast<uint32_t>(critical_pct);

    std::lock_guard lk(mutex);
    thresholds_packed.store(packed, std::memory_order_relaxed);
    /// Reset the cooldown so a level classified under the old ladder can't stick.
    level = 0;
    last_at_or_above_ns = 0;
}

MemoryPressureThresholds PressureLevelMachine::getThresholds() const
{
    const uint32_t packed = thresholds_packed.load(std::memory_order_relaxed);
    return {(packed >> 16) & 0xFFu, (packed >> 8) & 0xFFu, packed & 0xFFu};
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

double localMemoryPressureFromChain(MemoryTracker * start)
{
    double worst = 0.0;
    for (MemoryTracker * t = start; t != nullptr; t = t->getParent())
    {
        /// `Global` is handled by the total-pressure path; react only to `Process`/`User` here.
        if (t->level == VariableContext::Global)
            continue;
        worst = std::max(worst, t->getPressure());
    }
    return worst;
}

MemoryPressureLevel MemoryPressureMonitor::currentLevel()
{
    const uint64_t now_ns = steadyNowNs();
    const MemoryPressureLevel total_level = machine.sample(readTotalPressure(), now_ns);

    /// Per-query/per-user pressure: classified on the global ladder but cooled down on the group's own
    /// machine, so the sticky state is scoped to the query. A group-less thread classifies transiently.
    const double local_pressure = localMemoryPressureFromChain(CurrentThread::getMemoryTracker());
    const MemoryPressureLevel local_raw = machine.levelForPressure(local_pressure);
    MemoryPressureLevel local_level = local_raw;
    if (auto group = CurrentThread::getGroup())
        local_level = group->memory_pressure_machine.stick(static_cast<uint8_t>(local_raw), now_ns);

    return std::max(total_level, local_level);
}

MemoryPressureLevel FakeMemoryPressureMonitor::currentLevel()
{
    return machine.sample(
        pressure.load(std::memory_order_relaxed),
        now_ns.load(std::memory_order_relaxed));
}

namespace
{

/// The active monitor; tests swap it via `ScopedMemoryPressureMonitor`.
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

    /// First access installs the production singleton; `compare_exchange` picks one winner.
    auto * production = &productionInstance();
    IMemoryPressureMonitor * expected = nullptr;
    slot.compare_exchange_strong(expected, production, std::memory_order_acq_rel);
    return *slot.load(std::memory_order_acquire);
}

ScopedMemoryPressureMonitor::ScopedMemoryPressureMonitor(IMemoryPressureMonitor & override_monitor)
{
    /// Ensure the production singleton is slotted so the dtor can restore it.
    (void)memoryPressureMonitor();
    prior = activeMonitorSlot().exchange(&override_monitor, std::memory_order_acq_rel);
}

ScopedMemoryPressureMonitor::~ScopedMemoryPressureMonitor()
{
    activeMonitorSlot().store(prior, std::memory_order_release);
}

}
