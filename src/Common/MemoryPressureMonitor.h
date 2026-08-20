#pragma once

#include <base/types.h>

#include <atomic>
#include <cstdint>
#include <mutex>

class MemoryTracker;

namespace DB
{

enum class MemoryPressureLevel : uint8_t
{
    Normal = 0,
    Elevated = 1,
    High = 2,
    Critical = 3,
};

inline constexpr int memoryPressureLevelCount() { return 4; }

/// The three thresholds as percent of a tracker's hard limit.
struct MemoryPressureThresholds
{
    UInt64 elevated_pct;
    UInt64 high_pct;
    UInt64 critical_pct;
};

/// Each threshold must be in [0, 100] with `elevated <= high <= critical`, else throws `BAD_ARGUMENTS`.
void validateMemoryPressureThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct);

/// The thresholds are server-wide: one shared ladder every monitor classifies against. `set` validates
/// and publishes atomically (config reload); `get` reads the live values (observability).
void setMemoryPressureThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct);
MemoryPressureThresholds getMemoryPressureThresholds();

/// Classify a pressure ratio against the shared thresholds. No cooldown, lock-free.
MemoryPressureLevel classifyMemoryPressure(double pressure);

/// Sticky cooldown over an already-classified level: snap up immediately, step down one level per
/// `cooldown_ns` of sustained lower pressure. One instance per monitor holds its own timestamp.
class PressureCooldown
{
public:
    /// Server-total cooldown: freed RAM may be re-taken by any tenant, so de-escalate slowly.
    static constexpr uint64_t COOLDOWN_NS = 60ULL * 1000ULL * 1000ULL * 1000ULL;
    /// Query-scoped cooldown: no other tenant competes for the freed memory, so recover fast.
    static constexpr uint64_t QUERY_COOLDOWN_NS = 10ULL * 1000ULL * 1000ULL * 1000ULL;

    explicit PressureCooldown(uint64_t cooldown_ns_ = COOLDOWN_NS) : cooldown_ns(cooldown_ns_) {}

    MemoryPressureLevel apply(MemoryPressureLevel raw, uint64_t now_ns);

    /// Clear the sticky level and timestamp.
    void reset();

private:
    const uint64_t cooldown_ns;
    mutable std::mutex mutex;
    uint8_t level{0};
    uint64_t last_at_or_above_ns{0};
};

/// Watches one memory tracker and classifies its pressure with a sticky cooldown. Levels compose
/// through the parent chain: a monitor escalates against its parent (`max`), so a level never reads
/// below any level above it. The chain mirrors the tracker hierarchy - the global monitor watches
/// `total_memory_tracker`, a per-user monitor watches the user tracker with the global as parent, and
/// a per-query monitor watches the query tracker with the user monitor as parent. Classification uses
/// the shared server-wide thresholds (`getMemoryPressureThresholds`).
class MemoryPressureMonitor
{
public:
    /// Global (root) monitor: watches `total_memory_tracker`, server cooldown.
    MemoryPressureMonitor();
    /// Scoped monitor: watches `scope`, query cooldown, escalated against `parent`.
    MemoryPressureMonitor(MemoryTracker & scope, MemoryPressureMonitor & parent);

    /// Advances the cooldown, so non-const.
    MemoryPressureLevel currentLevel();

    /// Repoint the escalation parent - e.g. a query monitor onto its user monitor once the query joins
    /// the user (until then it escalates straight against the global monitor).
    void setParent(MemoryPressureMonitor & new_parent);

    /// Clear the sticky cooldown, e.g. when a shared scope (the per-user monitor) goes idle, so the
    /// next query does not inherit the previous one's level.
    void reset();

private:
    double samplePressure() const;

    MemoryTracker * scope;                          /// the tracker this monitor samples
    std::atomic<MemoryPressureMonitor *> parent;    /// null = root (global)
    PressureCooldown cooldown;
};

/// The global (root) monitor.
MemoryPressureMonitor & memoryPressureMonitor();

}
