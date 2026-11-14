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
/// window and a snapshot of the last full one. The counters are atomics without a common lock: an update that
/// races with a rotation can land in the next window. These are statistics, so that error is fine.
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

    /// The updates below are no-ops unless `segment_window` is the current window.
    void addPassiveBytes(Window segment_window, Int64 bytes, bool large);
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

    /// Rotates if needed, counts `read` in the current window and returns it.
    Window countRead(const SegmentRead & read);

    /// The last full window.
    Snapshot getSnapshot();

    void shiftTimeForTesting(std::chrono::milliseconds shift) { time_shift_for_testing_ms += shift.count(); }

private:
    /// Lazy: the first call after the window ends freezes it. The asynchronous metrics bound the lag.
    void rotateIfNeeded(Window now_window);
    /// `rotateIfNeeded` without its lock-free check.
    void rotateLocked(Window now_window) TSA_REQUIRES(rotation_mutex);

    struct WindowStats
    {
        std::atomic<Int64> active = 0;
        std::atomic<Int64> passive = 0;

        void add(Int64 active_delta, Int64 passive_delta);
    };

    const UInt64 window_sec;
    const std::function<size_t()> get_bytes;
    const std::function<size_t()> get_large_bytes;
    Stopwatch watch;
    std::atomic<Int64> time_shift_for_testing_ms = 0;

    std::atomic<Window> window = 0;
    /// `stats` counts all file segments, `large_stats` only the large ones (range > `boundary_alignment`).
    WindowStats stats;
    WindowStats large_stats;

    /// Serializes the rotations and guards `snapshot`. The updates do not take it.
    std::mutex rotation_mutex;
    Snapshot snapshot TSA_GUARDED_BY(rotation_mutex);
};

}
