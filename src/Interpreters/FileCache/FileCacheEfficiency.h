#pragma once

#include <base/defines.h>
#include <base/types.h>

#include <chrono>
#include <functional>
#include <limits>
#include <mutex>

namespace DB
{

/// Splits the bytes of one `FileCache` per efficiency window (`efficiency_window_sec`) into active
/// (served from the cache), passive (not served, in segments with a hit) and idle bytes.
/// Keeps the live window and a snapshot of the last full window. One mutex guards the state; it is a
/// leaf lock, so any method can be called under other cache locks. `setClockForTesting` is not
/// thread-safe.
class FileCacheEfficiency
{
public:
    using Clock = std::function<std::chrono::steady_clock::time_point()>;

    static constexpr UInt64 NEVER_READ = std::numeric_limits<UInt64>::max();

    struct Snapshot
    {
        UInt64 active_bytes = 0;
        UInt64 passive_bytes = 0;
        UInt64 idle_bytes = 0;
    };

    FileCacheEfficiency(UInt64 window_sec_, std::function<size_t()> get_used_size_);

    bool isEnabled() const { return window_sec != 0; }

    /// Does not rotate.
    UInt64 windowNow() const;

    /// Rotates if the live window ended.
    UInt64 currentWindow();

    /// No-op unless `window` is the live window.
    void addHeldBytes(UInt64 window, Int64 bytes);
    void addActiveBytes(UInt64 window, Int64 bytes);

    /// The last full window.
    Snapshot getSnapshot();

    /// Only before the cache is used.
    void setClockForTesting(Clock clock_);

private:
    void rotateIfNeeded(UInt64 now_window) TSA_REQUIRES(mutex);

    const UInt64 window_sec;
    const std::function<size_t()> get_used_size;
    Clock clock;
    std::chrono::steady_clock::time_point start;

    std::mutex mutex;
    UInt64 live_window TSA_GUARDED_BY(mutex) = 0;
    Int64 live_held_bytes TSA_GUARDED_BY(mutex) = 0;
    Int64 live_active_bytes TSA_GUARDED_BY(mutex) = 0;
    Snapshot snapshot TSA_GUARDED_BY(mutex);
};

}
