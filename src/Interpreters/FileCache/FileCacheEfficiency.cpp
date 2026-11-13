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
    /// Signed: the testing shift can be negative.
    const Int64 elapsed_ms = static_cast<Int64>(watch.elapsedMilliseconds()) + time_shift_for_testing_ms.load();
    return static_cast<Window>(static_cast<UInt64>(std::max<Int64>(elapsed_ms, 0)) / (window_sec * 1000));
}

FileCacheEfficiency::Window FileCacheEfficiency::currentWindow()
{
    const Window now_window = windowNow();
    rotateIfNeeded(now_window);
    return now_window;
}

void FileCacheEfficiency::WindowStats::add(Int64 active_delta, Int64 passive_delta)
{
    active.fetch_add(active_delta, std::memory_order_relaxed);
    passive.fetch_add(passive_delta, std::memory_order_relaxed);
}

void FileCacheEfficiency::rotateIfNeeded(Window now_window)
{
    if (now_window <= window.load(std::memory_order_relaxed))
        return;

    std::lock_guard lock(rotation_mutex);
    rotateLocked(now_window);
}

void FileCacheEfficiency::rotateLocked(Window now_window)
{
    const Window old_window = window.load(std::memory_order_relaxed);
    if (now_window <= old_window)
        return;

    Int64 active = stats.active.exchange(0, std::memory_order_relaxed);
    Int64 passive = stats.passive.exchange(0, std::memory_order_relaxed);
    Int64 large_active = large_stats.active.exchange(0, std::memory_order_relaxed);
    Int64 large_passive = large_stats.passive.exchange(0, std::memory_order_relaxed);
    window.store(now_window, std::memory_order_relaxed);

    /// If the window is not the last full one, the last full window had no cache hits.
    if (old_window + 1 != now_window)
        active = passive = large_active = large_passive = 0;

    const auto all = split(active, passive, static_cast<Int64>(get_bytes()));
    const auto large = split(large_active, large_passive, static_cast<Int64>(get_large_bytes()));
    snapshot = Snapshot{
        .active_bytes = all.active,
        .passive_bytes = all.passive,
        .idle_bytes = all.idle,
        .large_active_bytes = large.active,
        .large_passive_bytes = large.passive,
        .large_idle_bytes = large.idle,
    };
}

void FileCacheEfficiency::addPassiveBytes(Window segment_window, Int64 bytes, bool large)
{
    if (segment_window != window.load(std::memory_order_relaxed))
        return;
    stats.add(0, bytes);
    if (large)
        large_stats.add(0, bytes);
}

void FileCacheEfficiency::apply(Window segment_window, const Delta & delta)
{
    if (segment_window != window.load(std::memory_order_relaxed))
        return;
    stats.add(delta.active, delta.passive);
    large_stats.add(delta.large_active, delta.large_passive);
}

FileCacheEfficiency::Window FileCacheEfficiency::countRead(const SegmentRead & read)
{
    rotateIfNeeded(windowNow());
    const Window current_window = window.load(std::memory_order_relaxed);
    const bool new_window = read.segment_window != current_window;
    const Int64 passive = new_window ? read.reserved_bytes : 0;
    const Int64 active = new_window ? read.active_bytes_in_new_window : read.active_bytes_in_segment_window;
    stats.add(active, passive - active);
    if (read.large)
        large_stats.add(active, passive - active);
    return current_window;
}

FileCacheEfficiency::Snapshot FileCacheEfficiency::getSnapshot()
{
    if (!window_sec)
        return {};
    const Window now_window = windowNow();
    std::lock_guard lock(rotation_mutex);
    rotateLocked(now_window);
    return snapshot;
}

}
