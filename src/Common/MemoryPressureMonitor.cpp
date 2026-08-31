#include <Common/MemoryPressureMonitor.h>
#include <Common/Exception.h>
#include <Common/MemoryTracker.h>

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

uint64_t steadyNowMs()
{
    return std::chrono::duration_cast<std::chrono::milliseconds>(
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

MemoryPressureLevel PressureCooldown::apply(MemoryPressureLevel raw, uint16_t thresholds_generation)
{
    if (raw == MemoryPressureLevel::Normal
        && State::unpack(state.load(std::memory_order_relaxed)).level == MemoryPressureLevel::Normal)
        return MemoryPressureLevel::Normal;

    return apply(raw, steadyNowMs(), thresholds_generation);
}

MemoryPressureLevel PressureCooldown::apply(MemoryPressureLevel raw, uint64_t now_ms, uint16_t thresholds_generation)
{
    uint64_t current = state.load(std::memory_order_relaxed);
    while (true)
    {
        const State held = State::unpack(current);
        State next = held;

        if (raw >= held.level)                                  /// snap up, restart the cooldown
            next = {raw, thresholds_generation, now_ms};
        else if (thresholds_generation != held.generation)       /// stale ladder, take the new level now
            next = {raw, thresholds_generation, now_ms};
        else if (held.elapsedTo(now_ms) >= cooldown_ms)          /// one step per cooldown
            next = {stepDown(held.level), thresholds_generation, now_ms};

        /// A `Normal` level records nothing beside itself.
        if (next.level == MemoryPressureLevel::Normal)
        {
            next.generation = held.generation;
            next.cooldown_since_ms = held.cooldown_since_ms;
        }

        if (next.pack() == current)
            return next.level;

        /// A retry recomputes against the winner's word, so a step-down never happens twice per cooldown.
        if (state.compare_exchange_weak(current, next.pack(), std::memory_order_relaxed))
            return next.level;
    }
}

void PressureCooldown::reset()
{
    state.store(0, std::memory_order_relaxed);
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
        next.generation = static_cast<uint16_t>(live.generation + 1);
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
    , cooldown(PressureCooldown::COOLDOWN_MS)
{
}

MemoryPressureMonitor::MemoryPressureMonitor(MemoryTracker & scope_, MemoryPressureMonitor & parent_)
    : scope(&scope_)
    , parent(&parent_)
    , cooldown(PressureCooldown::SCOPE_COOLDOWN_MS)
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
        = cooldown.apply(thresholds.classify(samplePressure()), thresholds.generation);

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
