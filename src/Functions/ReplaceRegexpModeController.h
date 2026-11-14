#pragma once

#include <Common/Stopwatch.h>
#include <base/types.h>

#include <mutex>
#include <optional>


namespace DB
{

/// Chooses for `ReplaceRegexpImpl` whether to cache results per distinct haystack, and whether to run a
/// capture-free existence check ahead of the regexp. Which is cheaper depends on the cost of the regexp
/// relative to hashing or scanning the haystack, so the time per byte of each window of rows is measured, the
/// cheaper option is kept, and the other one is probed again at exponentially growing intervals. The result
/// does not depend on the choice.
/// The existence check is only considered when almost no row of the last window matched.
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

    /// A choice between doing something (`true`) or not, kept by measuring both.
    struct Choice
    {
        /// Windows at the start of a probe of `true` that are not measured.
        size_t warmup_windows = 0;
        /// The option of the current window, the one found cheaper, and whether the current phase measures
        /// the other one (before the first decision: both).
        bool current = true;
        bool preferred = true;
        bool probing = true;
        size_t windows_left_in_phase = phaseWindows(current, warmup_windows);
        /// Zero until the first decision.
        size_t probe_interval = 0;
        /// Time per byte of the last phase of each option, negative while unknown.
        double cost[2] = {-1, -1};
        UInt64 phase_ns = 0;
        size_t phase_units = 0;
    };

    struct State
    {
        /// A probe of the cache is not measured in its first window, which refills the cache.
        Choice cache{.warmup_windows = 1};
        Choice precheck;
        bool precheck_eligible = false;
        /// The window in progress at the end of the previous block.
        UInt64 window_ns = 0;
        Window window;
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

    bool useCache() const { return state.cache.current; }

    /// While the cache is probed, the existence check is held at its preferred option, so that each
    /// measured window differs from the compared ones in one choice only.
    bool usePrecheck() const
    {
        return precheck_allowed && state.precheck_eligible
            && (state.cache.probing ? state.precheck.preferred : state.precheck.current);
    }

    void finishWindow(const Window & window)
    {
        const UInt64 now_ns = clock_gettime_ns(CLOCK_MONOTONIC);
        const UInt64 window_ns = state.window_ns + now_ns - window_start_ns;
        window_start_ns = now_ns;
        state.window_ns = 0;

        /// Each choice is fed only windows that ran the other choice's preferred option, so that the
        /// options it compares differ in nothing else.
        const bool precheck_window = precheck_allowed && state.precheck_eligible && !state.cache.probing;
        const bool precheck_preferred = !precheck_window || state.precheck.current == state.precheck.preferred;
        if (precheck_window)
            addWindow(state.precheck, window_ns, window.units);

        /// When the cache option changes, the costs of the existence check are not reset: its regular
        /// schedule measures both of its options again, while a reset would re-measure them right away,
        /// in windows that the cache choice then skips, and skew the cache's next comparison.
        if (precheck_preferred)
            addWindow(state.cache, window_ns, window.units);

        state.precheck_eligible = precheck_allowed && window.matched * 20 < window_rows_count;
    }

private:
    /// On a near tie `false` is kept: it does not grow the cache, or does not scan matching rows twice.
    static constexpr double advantage_required = 0.9;
    static constexpr size_t initial_probe_interval = 4;
    static constexpr size_t max_probe_interval = 256;
    /// More than one, as a single window may cover a run of unusually cheap or expensive rows.
    static constexpr size_t measured_probe_windows = 2;

    static constexpr size_t phaseWindows(bool option, size_t warmup_windows)
    {
        return option ? measured_probe_windows + warmup_windows : measured_probe_windows;
    }

    static void addWindow(Choice & choice, UInt64 window_ns, size_t window_units)
    {
        const bool warmup = choice.probing && choice.current && choice.windows_left_in_phase > measured_probe_windows;
        if (!warmup)
        {
            choice.phase_ns += window_ns;
            choice.phase_units += window_units;
        }
        if (--choice.windows_left_in_phase == 0)
            finishPhase(choice);
    }

    static void finishPhase(Choice & choice)
    {
        if (choice.phase_units)
            choice.cost[choice.current] = static_cast<double>(choice.phase_ns) / static_cast<double>(choice.phase_units);
        choice.phase_ns = 0;
        choice.phase_units = 0;

        if (choice.probing && (choice.cost[false] < 0 || choice.cost[true] < 0))
        {
            choice.current = !choice.current;
            choice.windows_left_in_phase = phaseWindows(choice.current, choice.warmup_windows);
            return;
        }

        if (choice.probing)
        {
            const bool cheaper = choice.cost[true] < choice.cost[false] * advantage_required;
            if (choice.probe_interval == 0 || cheaper != choice.preferred)
            {
                choice.preferred = cheaper;
                choice.probe_interval = initial_probe_interval;
            }
            else
                choice.probe_interval = std::min(choice.probe_interval * 2, max_probe_interval);

            choice.probing = false;
            choice.current = choice.preferred;
            choice.windows_left_in_phase = choice.probe_interval;
        }
        else
        {
            choice.probing = true;
            choice.current = !choice.preferred;
            choice.windows_left_in_phase = phaseWindows(choice.current, choice.warmup_windows);
        }
    }

    Hint * hint;
    const bool precheck_allowed;
    State state;
    UInt64 window_start_ns = 0;
};

}
