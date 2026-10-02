#include <Interpreters/FileCache/FileCacheEfficiency.h>

#include <algorithm>

namespace DB
{

FileCacheEfficiency::FileCacheEfficiency(UInt64 window_sec_, std::function<size_t()> get_used_size_)
    : window_sec(window_sec_)
    , get_used_size(std::move(get_used_size_))
{
}

UInt64 FileCacheEfficiency::windowNow() const
{
    if (!window_sec)
        return 0;
    const Int64 elapsed_ms = static_cast<Int64>(watch.elapsedMilliseconds()) + time_shift_for_testing_ms.load();
    return static_cast<UInt64>(std::max<Int64>(elapsed_ms, 0)) / (window_sec * 1000);
}

UInt64 FileCacheEfficiency::currentWindow()
{
    const UInt64 now_window = windowNow();
    std::lock_guard lock(mutex);
    rotateIfNeeded(now_window);
    return now_window;
}

void FileCacheEfficiency::rotateIfNeeded(UInt64 now_window)
{
    if (now_window <= live_window)
        return;

    const Int64 used = static_cast<Int64>(get_used_size());
    if (live_window + 1 == now_window)
    {
        const Int64 active = std::max<Int64>(live_active_bytes, 0);
        const Int64 passive = std::max<Int64>(live_passive_bytes, 0);
        snapshot = Snapshot{
            .active_bytes = static_cast<UInt64>(active),
            .passive_bytes = static_cast<UInt64>(passive),
            .idle_bytes = static_cast<UInt64>(std::max<Int64>(used - active - passive, 0)),
        };
    }
    else
    {
        /// No reads in the last full window.
        snapshot = Snapshot{.active_bytes = 0, .passive_bytes = 0, .idle_bytes = static_cast<UInt64>(used)};
    }

    live_active_bytes = 0;
    live_passive_bytes = 0;
    live_window = now_window;
}

void FileCacheEfficiency::addPassiveBytes(UInt64 window, Int64 bytes)
{
    std::lock_guard lock(mutex);
    if (window == live_window)
        live_passive_bytes += bytes;
}

void FileCacheEfficiency::moveToActive(UInt64 window, Int64 bytes)
{
    std::lock_guard lock(mutex);
    if (window == live_window)
    {
        live_active_bytes += bytes;
        live_passive_bytes -= bytes;
    }
}

FileCacheEfficiency::Snapshot FileCacheEfficiency::getSnapshot()
{
    if (!window_sec)
        return {};
    const UInt64 now_window = windowNow();
    std::lock_guard lock(mutex);
    rotateIfNeeded(now_window);
    return snapshot;
}

}
