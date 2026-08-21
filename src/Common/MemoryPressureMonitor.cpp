#include <Common/MemoryPressureMonitor.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>
#include <base/defines.h>

#include <algorithm>
#include <chrono>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// The shared server-wide ladder in its published form. Relaxed ordering is enough: the word publishes
/// no other memory alongside itself.
std::atomic<uint64_t> & thresholdsState()
{
    static std::atomic<uint64_t> state{MemoryPressureThresholds{75, 90, 95}.pack()};
    return state;
}

uint64_t steadyNowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

}

MemoryPressureLevel MemoryPressureThresholds::classify(double pressure) const
{
    const double elevated = static_cast<double>(elevated_pct) / 100.0;
    const double high = static_cast<double>(high_pct) / 100.0;
    const double critical = static_cast<double>(critical_pct) / 100.0;

    if (pressure >= critical)
        return MemoryPressureLevel::Critical;
    if (pressure >= high)
        return MemoryPressureLevel::High;
    if (pressure >= elevated)
        return MemoryPressureLevel::Elevated;
    return MemoryPressureLevel::Normal;
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

MemoryPressureLevel PressureCooldown::apply(MemoryPressureLevel raw_level, uint64_t now_ns, uint32_t thresholds_generation)
{
    const uint8_t raw = static_cast<uint8_t>(raw_level);
    std::lock_guard lk(mutex);

    const bool ladder_changed = thresholds_generation != last_generation;
    last_generation = thresholds_generation;

    if (raw >= level)
    {
        /// Snap up immediately; refresh the "still elevated" timestamp.
        level = raw;
        last_at_or_above_ns = now_ns;
    }
    else if (ladder_changed)
    {
        /// The held level was classified against a ladder that no longer exists - accept the new
        /// classification at once instead of holding the old one until the cooldown expires.
        level = raw;
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

void PressureCooldown::reset()
{
    std::lock_guard lk(mutex);
    level = 0;
    last_at_or_above_ns = 0;
}

void setMemoryPressureThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct)
{
    validateMemoryPressureThresholds(elevated_pct, high_pct, critical_pct);

    MemoryPressureThresholds next{elevated_pct, high_pct, critical_pct};
    auto & state = thresholdsState();
    uint64_t current = state.load(std::memory_order_relaxed);

    while (true)
    {
        const MemoryPressureThresholds live = MemoryPressureThresholds::unpack(current);

        /// A reload that leaves the values alone must not move the generation: that would flush every
        /// sticky cooldown, so any `SYSTEM RELOAD CONFIG` would clear the pressure state of a server
        /// that is still under pressure.
        if (live.packValues() == next.packValues())
            return;

        /// A real change, so the levels classified against the old values are stale. The new generation
        /// tells every cooldown to take its next classification at once instead of decaying to it.
        next.generation = live.generation + 1;
        if (state.compare_exchange_weak(current, next.pack(), std::memory_order_relaxed))
            return;
    }
}

MemoryPressureThresholds getMemoryPressureThresholds()
{
    return MemoryPressureThresholds::unpack(thresholdsState().load(std::memory_order_relaxed));
}

MemoryPressureLevel classifyMemoryPressure(double pressure)
{
    return getMemoryPressureThresholds().classify(pressure);
}

MemoryPressureMonitor::MemoryPressureMonitor()
    : scope(&total_memory_tracker)
    , parent(nullptr)
    , cooldown(PressureCooldown::COOLDOWN_NS)
{
}

MemoryPressureMonitor::MemoryPressureMonitor(MemoryTracker & scope_, MemoryPressureMonitor & parent_)
    : scope(&scope_)
    , parent(&parent_)
    , cooldown(PressureCooldown::QUERY_COOLDOWN_NS)
{
}

void MemoryPressureMonitor::setParent(MemoryPressureMonitor & new_parent)
{
    parent.store(&new_parent, std::memory_order_relaxed);
}

void MemoryPressureMonitor::reset()
{
    cooldown.reset();
}

double MemoryPressureMonitor::samplePressure() const
{
    return scope->getPressure();
}

MemoryPressureLevel MemoryPressureMonitor::currentLevel()
{
    /// One read gives both the values and the generation that identifies them, so the classification
    /// and the cooldown's staleness check always refer to the same published ladder.
    const MemoryPressureThresholds thresholds = getMemoryPressureThresholds();
    const MemoryPressureLevel own
        = cooldown.apply(thresholds.classify(samplePressure()), steadyNowNs(), thresholds.generation);

    /// Escalate up the chain: a monitor never reads below any level above it.
    if (auto * p = parent.load(std::memory_order_relaxed))
        return std::max(own, p->currentLevel());
    return own;
}

MemoryPressureMonitor & memoryPressureMonitor()
{
    static MemoryPressureMonitor instance;
    return instance;
}

}
