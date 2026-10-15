#pragma once

#include <Common/Stopwatch.h>
#include <base/types.h>

#include <mutex>
#include <optional>


namespace DB
{

/// Chooses for `ReplaceRegexpImpl` whether to cache results per distinct haystack or run the regexp on every
/// row. Whether the cache pays depends on the cost of the regexp relative to hashing the haystack, so the time
/// per byte of each window of rows is measured, the cheaper mode is kept, and the other mode is probed again
/// at exponentially growing intervals. The result does not depend on the choice.
/// Also enables the capture-free existence check when almost no row of the last window matched.
/// The state is carried from block to block through `Hint`, so short blocks still reach a decision.
class ReplaceRegexpModeController
{
public:
    /// The counters of a window, kept by the caller in its row loop.
    struct Window
    {
        size_t units = 0;
        size_t rows = 0;
        size_t matched = 0;
    };

    static constexpr size_t window_rows_count = 32;
    /// The per-row cost in bytes, for normalizing the time of a window.
    static constexpr size_t row_cost_in_bytes = 64;

    struct State
    {
        bool use_cache = true;
        bool cache_is_cheaper = true;
        /// Measuring the mode that is not `cache_is_cheaper` (before the first decision: both).
        bool probing = true;
        size_t windows_left_in_phase = probeWindows(true);
        /// Zero until the first decision.
        size_t probe_interval = 0;
        /// Time per byte of the last phase in each mode, negative while unknown.
        double cost_cached = -1;
        double cost_plain = -1;
        UInt64 phase_ns = 0;
        size_t phase_units = 0;
        /// The window in progress at the end of the previous block.
        UInt64 window_ns = 0;
        Window window;
        bool precheck = false;
    };

    /// Shared by the threads executing the function; concurrent updates only lose measurements.
    struct Hint
    {
        std::mutex mutex;
        std::optional<State> state;
    };

    ReplaceRegexpModeController(Hint * hint_, bool precheck_allowed_)
        : hint(hint_)
        , precheck_allowed(precheck_allowed_)
    {
        if (hint)
        {
            std::lock_guard lock(hint->mutex);
            if (hint->state)
                state = *hint->state;
        }
        window_start_ns = clock_gettime_ns(CLOCK_MONOTONIC);
    }

    Window carriedWindow() const { return state.window; }

    /// Stores the state, including the unfinished `window`, for the next block.
    void finish(const Window & window)
    {
        state.window_ns += clock_gettime_ns(CLOCK_MONOTONIC) - window_start_ns;
        state.window = window;
        if (hint)
        {
            std::lock_guard lock(hint->mutex);
            hint->state = state;
        }
    }

    bool useCache() const { return state.use_cache; }
    bool usePrecheck() const { return precheck_allowed && state.precheck; }

    void finishWindow(const Window & window)
    {
        const UInt64 now_ns = clock_gettime_ns(CLOCK_MONOTONIC);
        const UInt64 window_ns = state.window_ns + now_ns - window_start_ns;
        window_start_ns = now_ns;
        state.window_ns = 0;

        const bool measured = !(state.probing && state.use_cache && state.windows_left_in_phase == probeWindows(true));
        if (measured)
        {
            state.phase_ns += window_ns;
            state.phase_units += window.units;
        }
        state.precheck = precheck_allowed && window.matched * 20 < window_rows_count;

        if (--state.windows_left_in_phase == 0)
            finishPhase();
    }

private:
    /// On a near tie the plain mode is kept, as it does not grow the cache.
    static constexpr double cache_advantage_required = 0.9;
    static constexpr size_t initial_probe_interval = 4;
    static constexpr size_t max_probe_interval = 256;
    /// More than one, as a single window may cover a run of unusually cheap or expensive rows.
    static constexpr size_t measured_probe_windows = 2;

    /// A probe of the cached mode is not measured in its first window, which refills the cache.
    static constexpr size_t probeWindows(bool cache) { return cache ? measured_probe_windows + 1 : measured_probe_windows; }

    void finishPhase()
    {
        if (state.phase_units)
            (state.use_cache ? state.cost_cached : state.cost_plain)
                = static_cast<double>(state.phase_ns) / static_cast<double>(state.phase_units);
        state.phase_ns = 0;
        state.phase_units = 0;

        if (state.probing && (state.cost_cached < 0 || state.cost_plain < 0))
        {
            state.use_cache = !state.use_cache;
            state.windows_left_in_phase = probeWindows(state.use_cache);
            return;
        }

        if (state.probing)
        {
            const bool cheaper = state.cost_cached < state.cost_plain * cache_advantage_required;
            if (state.probe_interval == 0 || cheaper != state.cache_is_cheaper)
            {
                state.cache_is_cheaper = cheaper;
                state.probe_interval = initial_probe_interval;
            }
            else
                state.probe_interval = std::min(state.probe_interval * 2, max_probe_interval);

            state.probing = false;
            state.use_cache = state.cache_is_cheaper;
            state.windows_left_in_phase = state.probe_interval;
        }
        else
        {
            state.probing = true;
            state.use_cache = !state.cache_is_cheaper;
            state.windows_left_in_phase = probeWindows(state.use_cache);
        }
    }

    Hint * hint;
    const bool precheck_allowed;
    State state;
    UInt64 window_start_ns = 0;
};

}
