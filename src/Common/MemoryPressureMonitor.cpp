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

constexpr uint32_t packThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct)
{
    return (static_cast<uint32_t>(elevated_pct) << 16)
         | (static_cast<uint32_t>(high_pct) << 8)
         | static_cast<uint32_t>(critical_pct);
}

/// The shared server-wide ladder, packed as bytes `(elevated << 16) | (high << 8) | critical`.
std::atomic<uint32_t> & thresholdsSingleton()
{
    static std::atomic<uint32_t> packed{packThresholds(75, 90, 95)};
    return packed;
}

/// Steady-clock time the ladder was last (re)set; lets a cooldown tell a stale level from a fresh one.
std::atomic<uint64_t> & thresholdsSetNs()
{
    static std::atomic<uint64_t> set_ns{0};
    return set_ns;
}

uint64_t steadyNowNs()
{
    return std::chrono::duration_cast<std::chrono::nanoseconds>(
               std::chrono::steady_clock::now().time_since_epoch())
        .count();
}

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

MemoryPressureLevel PressureCooldown::apply(MemoryPressureLevel raw_level, uint64_t now_ns, uint64_t thresholds_set_ns)
{
    const uint8_t raw = static_cast<uint8_t>(raw_level);
    std::lock_guard lk(mutex);

    if (raw >= level)
    {
        /// Snap up immediately; refresh the "still elevated" timestamp.
        level = raw;
        last_at_or_above_ns = now_ns;
    }
    else if (thresholds_set_ns > last_at_or_above_ns)
    {
        /// The ladder was reloaded after this level was set, so the level is stale - accept the new
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
    thresholdsSingleton().store(packThresholds(elevated_pct, high_pct, critical_pct), std::memory_order_relaxed);
    /// Stamp the change so cooldowns holding a level from the old ladder accept the new one at once.
    thresholdsSetNs().store(steadyNowNs(), std::memory_order_relaxed);
}

MemoryPressureThresholds getMemoryPressureThresholds()
{
    const uint32_t packed = thresholdsSingleton().load(std::memory_order_relaxed);
    return {(packed >> 16) & 0xFFu, (packed >> 8) & 0xFFu, packed & 0xFFu};
}

MemoryPressureLevel classifyMemoryPressure(double pressure)
{
    const MemoryPressureThresholds th = getMemoryPressureThresholds();
    const double elevated = static_cast<double>(th.elevated_pct) / 100.0;
    const double high = static_cast<double>(th.high_pct) / 100.0;
    const double critical = static_cast<double>(th.critical_pct) / 100.0;

    if (pressure >= critical)
        return MemoryPressureLevel::Critical;
    if (pressure >= high)
        return MemoryPressureLevel::High;
    if (pressure >= elevated)
        return MemoryPressureLevel::Elevated;
    return MemoryPressureLevel::Normal;
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
    const MemoryPressureLevel own = cooldown.apply(
        classifyMemoryPressure(samplePressure()), steadyNowNs(), thresholdsSetNs().load(std::memory_order_relaxed));

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
