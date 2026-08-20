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

MemoryPressureLevel PressureCooldown::apply(MemoryPressureLevel raw_level, uint64_t now_ns)
{
    const uint8_t raw = static_cast<uint8_t>(raw_level);
    std::lock_guard lk(mutex);

    if (raw >= level)
    {
        /// Snap up immediately; refresh the "still elevated" timestamp.
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

MemoryPressureMonitor::MemoryPressureMonitor()
    : scope(&total_memory_tracker)
    , parent(nullptr)
    , thresholds_packed(packThresholds(75, 90, 95))
    , cooldown(PressureCooldown::COOLDOWN_NS)
{
}

MemoryPressureMonitor::MemoryPressureMonitor(MemoryTracker & scope_, MemoryPressureMonitor & parent_)
    : scope(&scope_)
    , parent(&parent_)
    , thresholds_packed(packThresholds(75, 90, 95))   /// unused: a scoped monitor reads the root's
    , cooldown(PressureCooldown::QUERY_COOLDOWN_NS)
{
}

void MemoryPressureMonitor::setParent(MemoryPressureMonitor & new_parent)
{
    parent.store(&new_parent, std::memory_order_relaxed);
}

void MemoryPressureMonitor::setThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct)
{
    validateMemoryPressureThresholds(elevated_pct, high_pct, critical_pct);
    thresholds_packed.store(packThresholds(elevated_pct, high_pct, critical_pct), std::memory_order_relaxed);
}

MemoryPressureThresholds MemoryPressureMonitor::getThresholds() const
{
    /// Thresholds live at the root, so every monitor classifies against the live server-wide ladder
    /// and a reload reaches long-lived scoped monitors (e.g. the per-user one) at once.
    if (auto * p = parent.load(std::memory_order_relaxed))
        return p->getThresholds();
    const uint32_t packed = thresholds_packed.load(std::memory_order_relaxed);
    return {(packed >> 16) & 0xFFu, (packed >> 8) & 0xFFu, packed & 0xFFu};
}

double MemoryPressureMonitor::samplePressure() const
{
    return scope->getPressure();
}

MemoryPressureLevel MemoryPressureMonitor::classify(double pressure) const
{
    const MemoryPressureThresholds th = getThresholds();
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

MemoryPressureLevel MemoryPressureMonitor::currentLevel()
{
    const MemoryPressureLevel own = cooldown.apply(classify(samplePressure()), steadyNowNs());

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
