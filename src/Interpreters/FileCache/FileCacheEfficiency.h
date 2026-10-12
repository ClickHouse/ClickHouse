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

/// Active (served from the cache), passive (not served, in file segments with a hit) and idle bytes of one
/// `FileCache` per window of `efficiency_window_sec`, for all and for large file segments. Keeps the current
/// window and a snapshot of the last full one. The mutex is a leaf lock.
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
        UInt64 large_active_bytes = 0;
        UInt64 large_passive_bytes = 0;
        UInt64 large_idle_bytes = 0;
    };

    FileCacheEfficiency(UInt64 window_sec_, std::function<size_t()> get_bytes_, std::function<size_t()> get_large_bytes_);

    bool isEnabled() const { return window_sec != 0; }

    /// Does not rotate.
    Window windowNow() const;

    /// Rotates if the current window ended.
    Window currentWindow();

    /// No-op unless `segment_window` is the current window.
    void addPassiveBytes(Window segment_window, Int64 bytes, bool large);
    void moveToActive(Window segment_window, Int64 bytes, bool large);
    /// A file segment became large or not large.
    void moveToClass(Window segment_window, Int64 active, Int64 passive, bool large);

    /// `reserved_bytes` become passive if the read starts a new window for the file segment.
    struct SegmentRead
    {
        Window segment_window = NEVER_READ;
        Int64 reserved_bytes = 0;
        Int64 active_bytes_in_segment_window = 0;
        Int64 active_bytes_in_new_window = 0;
        bool large = false;
    };

    /// Rotates if needed, counts `read` in the current window and returns it, under one lock.
    Window countRead(const SegmentRead & read);

    /// The last full window.
    Snapshot getSnapshot();

    void shiftTimeForTesting(std::chrono::milliseconds shift) { time_shift_for_testing_ms += shift.count(); }

private:
    /// Lazy: the first call after the window ends freezes it. The asynchronous metrics bound the lag.
    void rotateIfNeeded(Window now_window) TSA_REQUIRES(mutex);

    struct WindowStats
    {
        Int64 active = 0;
        Int64 passive = 0;
    };

    const UInt64 window_sec;
    const std::function<size_t()> get_bytes;
    const std::function<size_t()> get_large_bytes;
    Stopwatch watch;
    std::atomic<Int64> time_shift_for_testing_ms = 0;

    std::mutex mutex;
    Window window TSA_GUARDED_BY(mutex) = 0;
    WindowStats stats TSA_GUARDED_BY(mutex);
    WindowStats large_stats TSA_GUARDED_BY(mutex);
    Snapshot snapshot TSA_GUARDED_BY(mutex);
};

}
