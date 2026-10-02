#pragma once

#include <base/defines.h>
#include <base/types.h>
#include <Common/Stopwatch.h>

#include <atomic>
#include <chrono>
#include <functional>
#include <limits>
#include <mutex>

namespace DB
{

/// Splits the bytes of one `FileCache` per efficiency window (`efficiency_window_sec`) into active
/// (served from the cache), passive (not served, in segments with a hit) and idle bytes.
/// Keeps the live window and a snapshot of the last full window. One mutex guards the state; it is a
/// leaf lock, so any method can be called under other cache locks.
class FileCacheEfficiency
{
public:
    using Window = UInt32;
    static constexpr Window NEVER_READ = std::numeric_limits<Window>::max();

    struct Snapshot
    {
        UInt64 active_bytes = 0;
        UInt64 passive_bytes = 0;
        UInt64 idle_bytes = 0;
    };

    FileCacheEfficiency(UInt64 window_sec_, std::function<size_t()> get_used_size_);

    bool isEnabled() const { return window_sec != 0; }

    /// Does not rotate.
    Window windowNow() const;

    /// Rotates if the live window ended.
    Window currentWindow();

    /// No-op unless `window` is the live window.
    void addPassiveBytes(Window window, Int64 bytes);
    void moveToActive(Window window, Int64 bytes);

    /// The last full window.
    Snapshot getSnapshot();

    /// For tests only.
    void shiftTimeForTesting(std::chrono::milliseconds shift) { time_shift_for_testing_ms += shift.count(); }

private:
    /// Lazy: the first `currentWindow` or `getSnapshot` after the window ends freezes it, so a size
    /// change before that still counts in it. The asynchronous metrics call `getSnapshot` on every
    /// update, which bounds the lag.
    void rotateIfNeeded(Window now_window) TSA_REQUIRES(mutex);

    const UInt64 window_sec;
    const std::function<size_t()> get_used_size;
    /// Windows count from construction.
    Stopwatch watch;
    std::atomic<Int64> time_shift_for_testing_ms = 0;

    std::mutex mutex;
    Window live_window TSA_GUARDED_BY(mutex) = 0;
    Int64 live_active_bytes TSA_GUARDED_BY(mutex) = 0;
    Int64 live_passive_bytes TSA_GUARDED_BY(mutex) = 0;
    Snapshot snapshot TSA_GUARDED_BY(mutex);
};

}
