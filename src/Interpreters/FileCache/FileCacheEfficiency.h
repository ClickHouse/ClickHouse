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

    /// A change of the bytes of one file segment, for all and for large file segments.
    struct Delta
    {
        Int64 active = 0;
        Int64 passive = 0;
        Int64 large_active = 0;
        Int64 large_passive = 0;

        /// `bytes` move from passive to active (negative: back); the bytes of each class stay the same.
        static Delta move(Int64 bytes, bool large) { return {bytes, -bytes, large ? bytes : 0, large ? -bytes : 0}; }
        /// The bytes of a file segment join the classes.
        static Delta enroll(Int64 active, Int64 passive, bool large) { return {active, passive, large ? active : 0, large ? passive : 0}; }
        /// The bytes of a file segment leave the classes.
        static Delta dismiss(Int64 active, Int64 passive, bool large) { return enroll(-active, -passive, large); }

        Delta operator+(const Delta & other) const
        {
            return {active + other.active, passive + other.passive, large_active + other.large_active, large_passive + other.large_passive};
        }
    };

    /// The updates below are no-ops unless `segment_window` is the current window. The caller changes the sizes of
    /// `get_bytes` before an update that adds bytes and after one that removes them: then the counters never exceed the
    /// sizes, and a rotation in between sees the change as idle bytes.
    void addPassiveBytes(Window segment_window, Int64 bytes, bool large);
    /// Under one lock, so that a rotation sees all of `delta` or none of it.
    void apply(Window segment_window, const Delta & delta);

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
