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

/// The three thresholds as percent of `total_memory_tracker.getHardLimit`.
struct MemoryPressureThresholds
{
    UInt64 elevated_pct;
    UInt64 high_pct;
    UInt64 critical_pct;
};

/// Each threshold must be in [0, 100] with `elevated <= high <= critical`, else throws `BAD_ARGUMENTS`.
/// Called before a reload applies so an invalid triple rejects the whole reload.
void validateMemoryPressureThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct);

/// Classifies memory pressure into a `MemoryPressureLevel`, sticky on de-escalation.
class IMemoryPressureMonitor
{
public:
    virtual ~IMemoryPressureMonitor() = default;

    /// Advances the cooldown state machine, so non-const.
    virtual MemoryPressureLevel currentLevel() = 0;

    virtual void setThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct) = 0;
    virtual MemoryPressureThresholds getThresholds() const = 0;
};

/// Level state machine shared by the impls: snaps up immediately, steps down one level per
/// `cooldown_ns` of sustained lower pressure.
class PressureLevelMachine
{
public:
    /// Server-total cooldown: freed RAM may be re-taken by any tenant, so de-escalate slowly.
    static constexpr uint64_t COOLDOWN_NS = 60ULL * 1000ULL * 1000ULL * 1000ULL;
    /// Query-scoped cooldown: no other tenant competes for the freed memory, so recover fast.
    static constexpr uint64_t QUERY_COOLDOWN_NS = 10ULL * 1000ULL * 1000ULL * 1000ULL;

    explicit PressureLevelMachine(uint64_t cooldown_ns_ = COOLDOWN_NS) : cooldown_ns(cooldown_ns_) {}

    MemoryPressureLevel sample(double pressure, uint64_t now_ns);

    /// Apply the cooldown to an already-classified level, so a scope-local machine can keep its own
    /// sticky state while classification stays in one place.
    MemoryPressureLevel stick(uint8_t raw_level, uint64_t now_ns);

    /// Classify `pressure` against the thresholds without the cooldown. Lock-free.
    MemoryPressureLevel levelForPressure(double pressure) const;

    void setThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct);
    MemoryPressureThresholds getThresholds() const;

private:
    uint8_t rawLevel(double pressure) const;

    /// Callers hold `mutex`.
    MemoryPressureLevel stickUnlocked(uint8_t raw_level, uint64_t now_ns);

    /// Thresholds packed into one atomic so classification needs no lock; `mutex` guards the cooldown state.
    std::atomic<uint32_t> thresholds_packed{(75u << 16) | (90u << 8) | 95u};
    const uint64_t cooldown_ns;
    mutable std::mutex mutex;
    uint8_t level{0};
    uint64_t last_at_or_above_ns{0};
};

/// Reads `total_memory_tracker` and `steady_clock`.
class MemoryPressureMonitor final : public IMemoryPressureMonitor
{
public:
    MemoryPressureLevel currentLevel() override;
    void setThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct) override { machine.setThresholds(elevated_pct, high_pct, critical_pct); }
    MemoryPressureThresholds getThresholds() const override { return machine.getThresholds(); }

private:
    PressureLevelMachine machine;
};

/// Pressure and time are controllable atomics, for tests.
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
    void setThresholds(UInt64 elevated_pct, UInt64 high_pct, UInt64 critical_pct) override { machine.setThresholds(elevated_pct, high_pct, critical_pct); }
    MemoryPressureThresholds getThresholds() const override { return machine.getThresholds(); }

private:
    std::atomic<double> pressure;
    std::atomic<uint64_t> now_ns;
    PressureLevelMachine machine;
};

/// Worst `used / hard_limit` over `start` and its parents across the `Process` and `User` levels;
/// `Global` and limit-less trackers are skipped. Null `start` yields 0.
double localMemoryPressureFromChain(MemoryTracker * start);

/// The active monitor; defaults to a `MemoryPressureMonitor` singleton.
IMemoryPressureMonitor & memoryPressureMonitor();

/// RAII swap of the active monitor for a test; the destructor restores the prior one.
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
