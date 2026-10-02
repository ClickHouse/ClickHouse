#include <Interpreters/FileCache/FileCacheEfficiency.h>

#include <algorithm>

namespace DB
{

FileCacheEfficiency::FileCacheEfficiency(UInt64 window_sec_, std::function<size_t()> get_used_size_)
    : window_sec(window_sec_)
    , get_used_size(std::move(get_used_size_))
    , clock([] { return std::chrono::steady_clock::now(); })
    , start(clock())
{
}

UInt64 FileCacheEfficiency::windowNow() const
{
    if (!window_sec)
        return 0;
    const Int64 elapsed = std::chrono::duration_cast<std::chrono::seconds>(clock() - start).count();
    return static_cast<UInt64>(std::max<Int64>(elapsed, 0)) / window_sec;
}

UInt64 FileCacheEfficiency::currentWindow()
{
    const UInt64 now_window = windowNow();
    if (now_window != live_window.load(std::memory_order_acquire))
        rotateIfNeeded(now_window);
    return now_window;
}

void FileCacheEfficiency::rotateIfNeeded(UInt64 now_window)
{
    std::lock_guard lock(rotation_mutex);
    const UInt64 old_window = live_window.load();
    if (now_window <= old_window)
        return;

    const Int64 used = static_cast<Int64>(get_used_size());
    if (old_window + 1 == now_window)
    {
        const Int64 held = std::max<Int64>(live_held_bytes.load(), 0);
        const Int64 active = std::clamp<Int64>(live_active_bytes.load(), 0, held);
        snapshot = Snapshot{
            .active_bytes = static_cast<UInt64>(active),
            .passive_bytes = static_cast<UInt64>(held - active),
            .idle_bytes = static_cast<UInt64>(std::max<Int64>(used - held, 0)),
        };
    }
    else
    {
        /// No reads in the last full window.
        snapshot = Snapshot{.active_bytes = 0, .passive_bytes = 0, .idle_bytes = static_cast<UInt64>(used)};
    }

    live_held_bytes.store(0);
    live_active_bytes.store(0);
    live_window.store(now_window, std::memory_order_release);
}

void FileCacheEfficiency::addHeldBytes(UInt64 window, Int64 bytes)
{
    if (bytes && window == live_window.load(std::memory_order_acquire))
        live_held_bytes.fetch_add(bytes);
}

void FileCacheEfficiency::addActiveBytes(UInt64 window, Int64 bytes)
{
    if (bytes && window == live_window.load(std::memory_order_acquire))
        live_active_bytes.fetch_add(bytes);
}

FileCacheEfficiency::Snapshot FileCacheEfficiency::getSnapshot()
{
    if (!window_sec)
        return {};
    currentWindow();
    std::lock_guard lock(rotation_mutex);
    return snapshot;
}

void FileCacheEfficiency::setClockForTesting(Clock clock_)
{
    std::lock_guard lock(rotation_mutex);
    clock = std::move(clock_);
    start = clock();
    live_window.store(0);
    live_held_bytes.store(0);
    live_active_bytes.store(0);
    snapshot = {};
}

}
