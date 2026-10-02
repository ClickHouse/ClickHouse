#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <limits>
#include <mutex>

namespace DB
{

/// Splits the bytes of one `FileCache` into three classes in each efficiency window
/// (`efficiency_window_sec`):
/// - active: unique bytes served from the cache in the window, rounded up to granules;
/// - passive: the other bytes of file segments that had at least one read in the window;
/// - idle: the bytes of file segments with no read in the window.
///
/// The class keeps the live window (S = bytes of segments read in it, U = active bytes)
/// and a snapshot of the last full window. `FileSegment` reports reads, size changes and
/// removals; this class only does the accounting. Window ids count from construction.
///
/// Thread safety: all methods are thread-safe. `rotation_mutex` serializes rotation; the live
/// counters are atomics. At a window edge a live counter can take an update meant for the
/// previous window; the error disappears with the next window.
class FileCacheEfficiency
{
public:
    using Clock = std::function<std::chrono::steady_clock::time_point()>;

    /// Window id of a file segment that was never read.
    static constexpr UInt64 NEVER_READ = std::numeric_limits<UInt64>::max();

    struct Snapshot
    {
        UInt64 active_bytes = 0;
        UInt64 passive_bytes = 0;
        UInt64 idle_bytes = 0;
    };

    FileCacheEfficiency(UInt64 window_sec_, std::function<size_t()> get_used_size_);

    bool isEnabled() const { return window_sec != 0; }

    /// The window id now. Does not rotate.
    UInt64 windowNow() const;

    /// The window id now. First rotates the live window if it ended.
    /// Do not call under a key lock or a file segment lock.
    UInt64 currentWindow();

    /// Add to S or U if `window` is the live window; otherwise do nothing. Safe under any lock.
    void addHeldBytes(UInt64 window, Int64 bytes);
    void addActiveBytes(UInt64 window, Int64 bytes);

    /// The last full window; all zeros until the first window ends.
    /// Rotates first. Do not call under a key lock or a file segment lock.
    Snapshot getSnapshot();

    /// Replaces the clock and starts counting windows again. Call only before the cache is used.
    void setClockForTesting(Clock clock_);

private:
    void rotateIfNeeded(UInt64 now_window);

    const UInt64 window_sec;
    const std::function<size_t()> get_used_size;
    Clock clock;
    std::chrono::steady_clock::time_point start;

    std::atomic<UInt64> live_window = 0;
    std::atomic<Int64> live_held_bytes = 0;
    std::atomic<Int64> live_active_bytes = 0;

    std::mutex rotation_mutex;
    Snapshot snapshot TSA_GUARDED_BY(rotation_mutex);
};

}
