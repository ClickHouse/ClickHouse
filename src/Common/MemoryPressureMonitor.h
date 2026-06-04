#pragma once

#include <base/types.h>

#include <atomic>
#include <cstdint>
#include <mutex>

/// `MemoryTracker` lives in the global namespace (see Common/MemoryTracker.h).
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

/// Snapshot of the three `Elevated` / `High` / `Critical` thresholds as
/// percent of `total_memory_tracker.getHardLimit()`. Returned by
/// `IMemoryPressureMonitor::getThresholds` so observability surfaces
/// (e.g. `system.server_settings`) can report the live values, which can
/// drift from the on-disk config after `SYSTEM RELOAD CONFIG`.
struct MemoryPressureThresholds
{
    UInt64 l1_pct;
    UInt64 l2_pct;
    UInt64 l3_pct;
};

/// Pressure → level mapping is the monitor's job. The mapping of level →
/// concrete sizes (read window, block) lives with the consumer (e.g.
/// `ReaderExecutor`). The interface intentionally exposes only the level
/// and the configurable thresholds.

/// Validate the three thresholds: each in [0, 100] and `l1 <= l2 <= l3`;
/// throws `BAD_ARGUMENTS` otherwise. Exposed so the config-reload path can
/// reject an invalid triple BEFORE applying any other live setting, instead of
/// leaving earlier settings from the same rejected reload partially applied.
void validateMemoryPressureThresholds(UInt64 l1_pct, UInt64 l2_pct, UInt64 l3_pct);

class IMemoryPressureMonitor
{
public:
    virtual ~IMemoryPressureMonitor() = default;

    /// Sample current pressure, apply the sticky-downward 60 s cooldown,
    /// return the level the monitor settles on. Non-const because each
    /// call advances the level state machine.
    virtual MemoryPressureLevel currentLevel() = 0;

    /// Thresholds for `Elevated` / `High` / `Critical` as percent of
    /// `total_memory_tracker.getHardLimit()`. Each value must be in [0, 100]
    /// and `l1 <= l2 <= l3`; violation throws `BAD_ARGUMENTS`. Takes
    /// `UInt64` (the server-settings type) so out-of-range inputs reach the
    /// validator instead of silently wrapping through `uint8_t`.
    virtual void setThresholds(UInt64 l1_pct, UInt64 l2_pct, UInt64 l3_pct) = 0;

    /// Snapshot of the currently-active thresholds. Used by
    /// `system.server_settings` to report the live (post-reload) values
    /// rather than the originally configured ones.
    virtual MemoryPressureThresholds getThresholds() const = 0;
};

/// Internal state machine reused by both impls. Owns the level + cooldown
/// timestamp + threshold atomics; given a fresh `(pressure, now_ns)`
/// sample, applies the snap-up-immediate / step-down-on-cooldown rule.
class PressureLevelMachine
{
public:
    static constexpr uint64_t COOLDOWN_NS = 60ULL * 1000ULL * 1000ULL * 1000ULL;

    MemoryPressureLevel sample(double pressure, uint64_t now_ns);

    /// Map a pressure ratio to a level using the current thresholds, WITHOUT
    /// the cooldown state machine — for transient (per-query / per-user)
    /// pressure that must react immediately and leave no sticky state.
    /// Lock-free: read on the executor's per-window hot path.
    MemoryPressureLevel levelForPressure(double pressure) const;

    void setThresholds(UInt64 l1_pct, UInt64 l2_pct, UInt64 l3_pct);
    MemoryPressureThresholds getThresholds() const;

private:
    /// Raw level for `pressure` against the (atomic) threshold ladder. Lock-free.
    uint8_t rawLevel(double pressure) const;

    /// Thresholds packed as bytes `(l1 << 16) | (l2 << 8) | l3`, published as one
    /// atomic so `levelForPressure` / `rawLevel` need no lock. The mutex below
    /// guards only the cooldown state (`level`, `last_at_or_above_ns`).
    std::atomic<uint32_t> thresholds_packed{(75u << 16) | (90u << 8) | 95u};
    mutable std::mutex mutex;
    uint8_t level{0};
    uint64_t last_at_or_above_ns{0};
};

/// Production implementation — reads `total_memory_tracker` (used / hard
/// limit) and `std::chrono::steady_clock` for the cooldown timer.
class MemoryPressureMonitor final : public IMemoryPressureMonitor
{
public:
    MemoryPressureLevel currentLevel() override;
    void setThresholds(UInt64 l1_pct, UInt64 l2_pct, UInt64 l3_pct) override { machine.setThresholds(l1_pct, l2_pct, l3_pct); }
    MemoryPressureThresholds getThresholds() const override { return machine.getThresholds(); }

private:
    PressureLevelMachine machine;
};

/// Test implementation — pressure and time are controllable atomics. Tests
/// own an instance on the stack and install it via `ScopedMemoryPressureMonitor`
/// for the duration of a test case. No global state, no captured lambdas,
/// no stack-use-after-return surface.
class FakeMemoryPressureMonitor final : public IMemoryPressureMonitor
{
public:
    explicit FakeMemoryPressureMonitor(double initial_pressure = 0.0, uint64_t initial_now_ns = 0)
        : pressure(initial_pressure)
        , now_ns(initial_now_ns)
    {
    }

    void setPressure(double p) { pressure.store(p, std::memory_order_relaxed); }
    void setNowNs(uint64_t t) { now_ns.store(t, std::memory_order_relaxed); }

    MemoryPressureLevel currentLevel() override;
    void setThresholds(UInt64 l1_pct, UInt64 l2_pct, UInt64 l3_pct) override { machine.setThresholds(l1_pct, l2_pct, l3_pct); }
    MemoryPressureThresholds getThresholds() const override { return machine.getThresholds(); }

private:
    std::atomic<double> pressure;
    std::atomic<uint64_t> now_ns;
    PressureLevelMachine machine;
};

/// Most-constraining transient memory pressure in `start`'s tracker chain:
/// `used / hard_limit` walked over `start` and its parents, taking the max
/// across the per-query (`Process`) and per-user (`User`) levels. `Global`
/// (the server total) is skipped — it is handled, with cooldown smoothing, by
/// the total-pressure path. Trackers without a hard limit are skipped.
/// `start == nullptr` (no current thread) yields 0. Pure; used by the
/// production monitor and unit-tested directly with a hand-built chain.
double localMemoryPressureFromChain(MemoryTracker * start);

/// Active monitor accessor. Production code (`ReaderExecutor`, `Server.cpp`)
/// always uses this. Defaults to a Meyers `MemoryPressureMonitor` singleton.
IMemoryPressureMonitor & memoryPressureMonitor();

/// RAII swap of the active monitor for the duration of a test. The
/// destructor restores the prior monitor — so production code that fires
/// `memoryPressureMonitor().currentLevel()` between cases or at teardown
/// always sees the real `MemoryPressureMonitor`, never a dangling test
/// instance.
class ScopedMemoryPressureMonitor
{
public:
    explicit ScopedMemoryPressureMonitor(IMemoryPressureMonitor & override_monitor);
    ~ScopedMemoryPressureMonitor();

    ScopedMemoryPressureMonitor(const ScopedMemoryPressureMonitor &) = delete;
    ScopedMemoryPressureMonitor & operator=(const ScopedMemoryPressureMonitor &) = delete;

private:
    IMemoryPressureMonitor * prior;
};

}
