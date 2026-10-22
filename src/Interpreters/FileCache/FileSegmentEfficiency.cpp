#include <Interpreters/FileCache/FileSegmentEfficiency.h>

#include <base/defines.h>

#include <algorithm>

namespace DB
{

namespace
{

size_t lastGranuleNumber(size_t size)
{
    return (size - 1) / FileSegmentEfficiency::GRANULE_SIZE;
}

size_t lastGranuleSize(size_t size)
{
    return size - lastGranuleNumber(size) * FileSegmentEfficiency::GRANULE_SIZE;
}

}

FileSegmentEfficiency::FileSegmentEfficiency(size_t range_size)
    : active_granules(lastGranuleNumber(range_size) + 1, false)
{
}

void FileSegmentEfficiency::markRead(
    FileCacheEfficiency & cache_efficiency, ByteRange range, ByteRange read, Int64 reserved_bytes, bool large)
{
    const auto granules = getGranules(range, read);
    const auto current_window = cache_efficiency.countRead({
        .segment_window = window,
        .reserved_bytes = reserved_bytes,
        .active_bytes_in_segment_window = granules && window != FileCacheEfficiency::NEVER_READ
            ? static_cast<Int64>(getNewActiveBytes(*granules, range.size, /*new_window=*/false))
            : 0,
        .active_bytes_in_new_window = granules ? static_cast<Int64>(getNewActiveBytes(*granules, range.size, /*new_window=*/true)) : 0,
        .large = large,
    });

    if (window != current_window)
        startWindow(current_window, range.size);
    if (granules)
        setGranules(*granules);
}

void FileSegmentEfficiency::addReservedBytes(FileCacheEfficiency & cache_efficiency, Int64 delta, bool large) const
{
    cache_efficiency.addPassiveBytes(window, delta, large);
}

void FileSegmentEfficiency::shrink(
    FileCacheEfficiency & cache_efficiency, size_t old_range_size, size_t range_size, Int64 reserved_bytes, bool was_large, bool large) const
{
    /// The same bytes that `remove` subtracts later, so the classes stay exact.
    const auto old_active = static_cast<Int64>(getActiveBytes(old_range_size));
    const auto active = static_cast<Int64>(getActiveBytes(range_size));
    if (was_large == large)
    {
        cache_efficiency.apply(window, FileCacheEfficiency::Delta::move(active - old_active, large));
        return;
    }
    /// The file segment leaves its old size class with the old active bytes and joins the new one with the new ones.
    cache_efficiency.apply(window,
        FileCacheEfficiency::Delta::dismiss(old_active, reserved_bytes - old_active, was_large)
            + FileCacheEfficiency::Delta::enroll(active, reserved_bytes - active, large));
}

void FileSegmentEfficiency::remove(FileCacheEfficiency & cache_efficiency, size_t range_size, Int64 reserved_bytes, bool large) const
{
    const auto active = static_cast<Int64>(getActiveBytes(range_size));
    cache_efficiency.apply(window, FileCacheEfficiency::Delta::dismiss(active, reserved_bytes - active, large));
}

FileSegmentEfficiencyInfo FileSegmentEfficiency::getInfo(
    FileCacheEfficiency::Window current_window, size_t range_size, size_t downloaded_size) const
{
    FileSegmentEfficiencyInfo info;
    info.idle_bytes = downloaded_size;
    const bool hit_in_current_window = window == current_window;
    if (hit_in_current_window)
    {
        info.active_bytes = std::min(getActiveBytes(range_size), downloaded_size);
        info.passive_bytes = downloaded_size - info.active_bytes;
        info.idle_bytes = 0;
    }

    const auto last_hit_window = hit_in_current_window ? previous_hit_window : window;
    if (last_hit_window != FileCacheEfficiency::NEVER_READ && last_hit_window <= current_window)
    {
        info.last_hit_windows_ago = current_window - last_hit_window;
        info.last_hit_active_bytes = hit_in_current_window ? previous_active_bytes : getActiveBytes(range_size);
        info.last_hit_passive_bytes = downloaded_size - std::min<size_t>(info.last_hit_active_bytes, downloaded_size);
    }
    return info;
}

std::optional<FileSegmentEfficiency::Granules> FileSegmentEfficiency::getGranules(ByteRange range, ByteRange read)
{
    const auto overlap = read.intersect(range);
    if (overlap.size == 0)
        return std::nullopt;
    return Granules{(overlap.offset - range.offset) / GRANULE_SIZE, (overlap.end() - 1 - range.offset) / GRANULE_SIZE};
}

size_t FileSegmentEfficiency::getNewActiveBytes(Granules granules, size_t range_size, bool new_window) const
{
    const auto [first, last] = granules;
    chassert(new_window || last < active_granules.size());
    const size_t new_granules = new_window
        ? last - first + 1
        : static_cast<size_t>(std::count(active_granules.begin() + first, active_granules.begin() + last + 1, false));
    const bool new_last = last == lastGranuleNumber(range_size) && (new_window || !active_granules[last]);
    if (!new_last)
        return new_granules * GRANULE_SIZE;
    return (new_granules - 1) * GRANULE_SIZE + lastGranuleSize(range_size);
}

void FileSegmentEfficiency::setGranules(Granules granules)
{
    chassert(granules.second < active_granules.size());
    std::fill(active_granules.begin() + granules.first, active_granules.begin() + granules.second + 1, true);
}

size_t FileSegmentEfficiency::getActiveBytes(size_t range_size) const
{
    chassert(lastGranuleNumber(range_size) < active_granules.size());
    const size_t set = std::count(active_granules.begin(), active_granules.end(), true);
    if (!active_granules[lastGranuleNumber(range_size)])
        return set * GRANULE_SIZE;
    return (set - 1) * GRANULE_SIZE + lastGranuleSize(range_size);
}

void FileSegmentEfficiency::startWindow(FileCacheEfficiency::Window new_window, size_t range_size)
{
    if (window != FileCacheEfficiency::NEVER_READ)
    {
        previous_hit_window = window;
        previous_active_bytes = getActiveBytes(range_size);
    }
    /// The range never grows, so the granules allocated in the constructor are enough.
    std::fill(active_granules.begin(), active_granules.end(), false);
    window = new_window;
}

}
