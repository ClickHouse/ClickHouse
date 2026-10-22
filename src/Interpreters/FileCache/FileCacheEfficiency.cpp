#include <Interpreters/FileCache/FileCacheEfficiency.h>

#include <algorithm>

namespace DB
{

namespace
{

struct Split
{
    UInt64 active = 0;
    UInt64 passive = 0;
    UInt64 idle = 0;
};

Split split(Int64 window_active, Int64 window_passive, Int64 used)
{
    const Int64 active = std::max<Int64>(window_active, 0);
    const Int64 passive = std::max<Int64>(window_passive, 0);
    return Split{
        .active = static_cast<UInt64>(active),
        .passive = static_cast<UInt64>(passive),
        .idle = static_cast<UInt64>(std::max<Int64>(used - active - passive, 0)),
    };
}

}

FileCacheEfficiency::FileCacheEfficiency(
    UInt64 window_sec_, std::function<size_t()> get_bytes_, std::function<size_t()> get_large_bytes_)
    : window_sec(window_sec_)
    , get_bytes(std::move(get_bytes_))
    , get_large_bytes(std::move(get_large_bytes_))
{
}

FileCacheEfficiency::Window FileCacheEfficiency::windowNow() const
{
    if (!window_sec)
        return 0;
    const Int64 elapsed_ms = static_cast<Int64>(watch.elapsedMilliseconds()) + time_shift_for_testing_ms.load();
    return static_cast<Window>(static_cast<UInt64>(std::max<Int64>(elapsed_ms, 0)) / (window_sec * 1000));
}

FileCacheEfficiency::Window FileCacheEfficiency::currentWindow()
{
    const Window now_window = windowNow();
    std::lock_guard lock(mutex);
    rotateIfNeeded(now_window);
    return now_window;
}

void FileCacheEfficiency::rotateIfNeeded(Window now_window)
{
    if (now_window <= window)
        return;

    /// If the window is not the last full one, the last full window had no cache hits.
    if (window + 1 != now_window)
    {
        stats = {};
        large_stats = {};
    }

    const auto all = split(stats.active, stats.passive, static_cast<Int64>(get_bytes()));
    const auto large = split(large_stats.active, large_stats.passive, static_cast<Int64>(get_large_bytes()));
    snapshot = Snapshot{
        .active_bytes = all.active,
        .passive_bytes = all.passive,
        .idle_bytes = all.idle,
        .large_active_bytes = large.active,
        .large_passive_bytes = large.passive,
        .large_idle_bytes = large.idle,
    };

    stats = {};
    large_stats = {};
    window = now_window;
}

void FileCacheEfficiency::addPassiveBytes(Window segment_window, Int64 bytes, bool large)
{
    std::lock_guard lock(mutex);
    if (segment_window != window)
        return;
    stats.passive += bytes;
    if (large)
        large_stats.passive += bytes;
}

void FileCacheEfficiency::apply(Window segment_window, const Delta & delta)
{
    std::lock_guard lock(mutex);
    if (segment_window != window)
        return;
    stats.active += delta.active;
    stats.passive += delta.passive;
    large_stats.active += delta.large_active;
    large_stats.passive += delta.large_passive;
}

FileCacheEfficiency::Window FileCacheEfficiency::countRead(const SegmentRead & read)
{
    const Window now_window = windowNow();
    std::lock_guard lock(mutex);
    rotateIfNeeded(now_window);
    const bool new_window = read.segment_window != window;
    const Int64 passive = new_window ? read.reserved_bytes : 0;
    const Int64 active = new_window ? read.active_bytes_in_new_window : read.active_bytes_in_segment_window;
    stats.active += active;
    stats.passive += passive - active;
    if (read.large)
    {
        large_stats.active += active;
        large_stats.passive += passive - active;
    }
    return window;
}

FileCacheEfficiency::Snapshot FileCacheEfficiency::getSnapshot()
{
    if (!window_sec)
        return {};
    const Window now_window = windowNow();
    std::lock_guard lock(mutex);
    rotateIfNeeded(now_window);
    return snapshot;
}

}
