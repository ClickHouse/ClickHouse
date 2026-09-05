#pragma once

#include <base/types.h>

#include <atomic>
#include <cstdint>

class MemoryTracker;

namespace DB
{

enum class MemoryPressureLevel : uint8_t
{
    Normal = 0,
    Elevated,
    High,
    Critical,
    Count,
};

constexpr MemoryPressureLevel stepDown(MemoryPressureLevel level)
{
    return level == MemoryPressureLevel::Normal
        ? MemoryPressureLevel::Normal
        : static_cast<MemoryPressureLevel>(static_cast<uint8_t>(level) - 1);
}

/// The three thresholds as percent of a tracker's hard limit, with the generation that identifies this
/// publication of them. The generation is assigned by the store, so a caller never supplies one, and it
/// moves only when the values change - that is what lets a cooldown tell one publication from another.
struct MemoryPressureThresholds
{
    /// Width of a publication generation, shared by both packed words in this file. A wrap needs 65536
    /// reloads between two samples of one monitor.
    static constexpr int GENERATION_BITS = 16;

    UInt64 elevated_pct = 0;
    UInt64 high_pct = 0;
    UInt64 critical_pct = 0;
    uint16_t generation = 0;

    /// The values alone, as one word. Two publications hold the same thresholds when these are equal.
    constexpr uint32_t packValues() const
    {
        return (static_cast<uint32_t>(elevated_pct) << 16)
             | (static_cast<uint32_t>(high_pct) << 8)
             | static_cast<uint32_t>(critical_pct);
    }

    /// The published form, `(generation << 32) | packValues()`. Values and generation share one word, so
    /// a reader can never pair a new generation with old values.
    constexpr uint64_t pack() const { return (static_cast<uint64_t>(generation) << 32) | packValues(); }

    static constexpr MemoryPressureThresholds unpack(uint64_t packed)
    {
        return {(packed >> 16) & 0xFFu, (packed >> 8) & 0xFFu, packed & 0xFFu, static_cast<uint16_t>(packed >> 32)};
    }

    MemoryPressureLevel classify(double pressure) const;
};

/// Each threshold must be in [1, 100] with `elevated <= high <= critical`, else throws `BAD_ARGUMENTS`.
/// An `elevated` of 0 would classify every scope as `Elevated`, including one with no hard limit.
void validateMemoryPressureThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct);

/// The thresholds are server-wide: one shared triple every monitor classifies against. `set` validates
/// and publishes atomically (config reload), assigning the generation itself; `get` reads the live
/// values together with the generation they were published under.
void setMemoryPressureThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct);
MemoryPressureThresholds getMemoryPressureThresholds();

MemoryPressureLevel classifyMemoryPressure(double pressure);

/// Sticky cooldown over an already-classified level: snap up immediately, step down one level per
/// `cooldown_ms` of sustained lower pressure. One instance per monitor.
///
/// Lock-free. The level, the timestamp its cooldown runs from, and the thresholds generation it was
/// classified against share one atomic word, because they must move as a unit: separate atomics pair a
/// level with another level's timestamp and step down twice in one cooldown. A sample that changes
/// nothing does not write, so an idle monitor's word stays read-only on every reader thread.
class PressureCooldown
{
public:
    /// Server-total cooldown: freed RAM may be re-taken by any tenant, so de-escalate slowly.
    static constexpr uint64_t COOLDOWN_MS = 60 * 1000;       /// 1 minute
    /// Scope cooldown (user, query): no other tenant competes for the freed memory, so recover fast.
    static constexpr uint64_t SCOPE_COOLDOWN_MS = 10 * 1000; /// 10 seconds

    explicit PressureCooldown(uint64_t cooldown_ms_ = COOLDOWN_MS) : cooldown_ms(cooldown_ms_) {}

    /// `thresholds_generation` identifies the thresholds `raw_level` was classified against. A generation
    /// other than the held level's means that level came from thresholds that no longer exist, so it is
    /// replaced at once rather than decaying over a cooldown, and a reload needs no per-monitor flag.
    ///
    /// Reads the clock only when there is state to update.
    MemoryPressureLevel apply(MemoryPressureLevel raw_level, uint16_t thresholds_generation);
    /// Time-injecting form, for tests.
    MemoryPressureLevel apply(MemoryPressureLevel raw_level, uint64_t now_ms, uint16_t thresholds_generation);

    void reset();

private:
    /// The published word, `[ level: 2 | generation: 16 | cooldown_since_ms: 46 ]`. The fields tile it,
    /// so `pack` and `unpack` are exact inverses - that is what lets `apply` spot a no-op by comparing
    /// packed words. 46 bits of milliseconds is ~2200 years; `elapsedTo` masks the subtraction anyway.
    struct State
    {
        static constexpr int MS_BITS = 46;
        static constexpr uint64_t MS_MASK = (1ULL << MS_BITS) - 1;
        static constexpr int LEVEL_SHIFT = MS_BITS + MemoryPressureThresholds::GENERATION_BITS;

        MemoryPressureLevel level = MemoryPressureLevel::Normal;
        uint16_t generation = 0;
        uint64_t cooldown_since_ms = 0;

        constexpr uint64_t pack() const
        {
            return (static_cast<uint64_t>(level) << LEVEL_SHIFT)
                 | (static_cast<uint64_t>(generation) << MS_BITS)
                 | (cooldown_since_ms & MS_MASK);
        }

        static constexpr State unpack(uint64_t packed)
        {
            return {
                static_cast<MemoryPressureLevel>(packed >> LEVEL_SHIFT),
                static_cast<uint16_t>(packed >> MS_BITS),
                packed & MS_MASK};
        }

        constexpr uint64_t elapsedTo(uint64_t now_ms) const { return (now_ms - cooldown_since_ms) & MS_MASK; }
    };

    static_assert(State::LEVEL_SHIFT + 2 == 64, "the packed state must fill exactly one word");
    static_assert(static_cast<size_t>(MemoryPressureLevel::Count) <= 4, "the level field holds 2 bits");

    const uint64_t cooldown_ms;
    std::atomic<uint64_t> state{0};
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
    /// Scoped monitor: watches `scope`, scope cooldown, escalated against `parent`.
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

    MemoryTracker * scope;
    std::atomic<MemoryPressureMonitor *> parent;    /// null = root (global)
    PressureCooldown cooldown;
};

MemoryPressureMonitor & getGlobalMemoryPressureMonitor();

}
