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

Split split(Int64 live_active, Int64 live_passive, Int64 used)
{
    const Int64 active = std::max<Int64>(live_active, 0);
    const Int64 passive = std::max<Int64>(live_passive, 0);
    return Split{
        .active = static_cast<UInt64>(active),
        .passive = static_cast<UInt64>(passive),
        .idle = static_cast<UInt64>(std::max<Int64>(used - active - passive, 0)),
    };
}

}

FileCacheEfficiency::FileCacheEfficiency(
    UInt64 window_sec_, std::function<size_t()> get_used_size_, std::function<size_t()> get_large_used_size_)
    : window_sec(window_sec_)
    , get_used_size(std::move(get_used_size_))
    , get_large_used_size(std::move(get_large_used_size_))
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
    if (now_window <= live_window)
        return;

    /// If the live window is not the last full one, the last full window had no cache hits.
    const bool live_is_last_full = live_window + 1 == now_window;
    const auto all = split(
        live_is_last_full ? live.active : 0, live_is_last_full ? live.passive : 0, static_cast<Int64>(get_used_size()));
    const auto large = split(
        live_is_last_full ? live_large.active : 0, live_is_last_full ? live_large.passive : 0, static_cast<Int64>(get_large_used_size()));
    snapshot = Snapshot{
        .active_bytes = all.active,
        .passive_bytes = all.passive,
        .idle_bytes = all.idle,
        .large_active_bytes = large.active,
        .large_passive_bytes = large.passive,
        .large_idle_bytes = large.idle,
    };

    live = {};
    live_large = {};
    live_window = now_window;
}

void FileCacheEfficiency::addPassiveBytes(Window window, Int64 bytes, bool large)
{
    std::lock_guard lock(mutex);
    if (window != live_window)
        return;
    live.passive += bytes;
    if (large)
        live_large.passive += bytes;
}

void FileCacheEfficiency::moveToActive(Window window, Int64 bytes, bool large)
{
    std::lock_guard lock(mutex);
    if (window != live_window)
        return;
    live.active += bytes;
    live.passive -= bytes;
    if (large)
    {
        live_large.active += bytes;
        live_large.passive -= bytes;
    }
}

void FileCacheEfficiency::moveToClass(Window window, Int64 active, Int64 passive, bool large)
{
    std::lock_guard lock(mutex);
    if (window != live_window)
        return;
    const Int64 sign = large ? 1 : -1;
    live_large.active += sign * active;
    live_large.passive += sign * passive;
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
