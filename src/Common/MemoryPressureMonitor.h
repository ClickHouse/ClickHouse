#pragma once

#include <atomic>
#include <cstddef>
#include <cstdint>
#include <functional>

namespace DB
{

/// Singleton that maps `total_memory_tracker` pressure (used / hard_limit)
/// onto a small set of hard-coded levels. Each level pins the
/// `ReaderExecutor` read window and per-block allocation to progressively
/// smaller sizes so steady-state allocations shrink before the hard limit
/// fires.
///
/// Going UP (more pressure) is immediate. Going DOWN is sticky: after the
/// pressure drops below the current level's threshold, the monitor waits at
/// least `COOLDOWN_NS` (60 s) before stepping down by one level — and only
/// one level per cooldown window. This prevents thrashing on bursty
/// allocations and gives heap fragmentation time to settle.
///
/// Thresholds are tunable per server (`reader_executor_memory_pressure_level_*_pct`),
/// configured once at server start via `setThresholds`. Sizes and number of
/// levels are intentionally hard-coded — the small fixed table keeps the
/// state machine trivial to reason about and avoids combinatorial tuning.
class MemoryPressureMonitor
{
public:
    struct LevelConfig
    {
        size_t window_bytes;
        size_t block_bytes;
    };

    static constexpr int LEVELS = 4;

    static MemoryPressureMonitor & instance();

    /// Apply thresholds from `ServerSettings`. `l1 < l2 < l3` must hold (values in
    /// 1..100); out-of-range or non-monotonic inputs are clamped/ordered before
    /// being stored so the monitor stays in a workable state.
    void setThresholds(uint8_t l1_pct, uint8_t l2_pct, uint8_t l3_pct);

    /// Sample pressure, apply the sticky-downward rule, return the (window,
    /// block) sizes for the current level.
    LevelConfig effective();

    int currentLevel() const { return level.load(std::memory_order_relaxed); }

    /// Test hook. Replaces the pressure and clock sources, resets state to L0
    /// and timestamp 0. Not safe to call concurrently with `effective()`.
    using PressureFn = std::function<double()>;
    using ClockFn = std::function<uint64_t()>;
    void resetForTesting(PressureFn pressure, ClockFn clock);

private:
    MemoryPressureMonitor();

    std::atomic<int> level{0};
    std::atomic<uint64_t> last_at_or_above_ns{0};

    std::atomic<uint8_t> threshold_l1{75};
    std::atomic<uint8_t> threshold_l2{90};
    std::atomic<uint8_t> threshold_l3{95};

    PressureFn read_pressure;
    ClockFn read_now_ns;

    static constexpr LevelConfig CONFIGS[LEVELS] = {
        {8ULL << 20,   1ULL << 20  },  // L0 NORMAL
        {2ULL << 20,   512ULL << 10},  // L1 ELEVATED
        {512ULL << 10, 512ULL << 10},  // L2 HIGH
        {128ULL << 10, 128ULL << 10},  // L3 CRITICAL
    };
    static constexpr uint64_t COOLDOWN_NS = 60ULL * 1000ULL * 1000ULL * 1000ULL;
};

}
