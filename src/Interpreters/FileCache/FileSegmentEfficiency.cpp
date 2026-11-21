#include <Interpreters/FileCache/FileSegmentEfficiency.h>

#include <base/defines.h>

#include <algorithm>
#include <bit>

namespace DB
{

namespace
{

constexpr size_t WORD_BITS = 64;

size_t lastGranuleNumber(size_t size)
{
    return (size - 1) / FileSegmentEfficiency::GRANULE_SIZE;
}

size_t lastGranuleSize(size_t size)
{
    return size - lastGranuleNumber(size) * FileSegmentEfficiency::GRANULE_SIZE;
}

size_t wordCount(size_t range_size)
{
    return lastGranuleNumber(range_size) / WORD_BITS + 1;
}

/// The bits of the granules `[first, last]` in the word `word`.
UInt64 granuleMask(size_t word, size_t first, size_t last)
{
    const size_t word_first = word * WORD_BITS;
    const size_t low = std::max(first, word_first) - word_first;
    const size_t high = std::min(last, word_first + WORD_BITS - 1) - word_first;
    const UInt64 up_to_high = high == WORD_BITS - 1 ? ~UInt64(0) : (UInt64(1) << (high + 1)) - 1;
    return up_to_high & ~((UInt64(1) << low) - 1);
}

bool isSet(const std::vector<UInt64> & words, size_t granule)
{
    return (words[granule / WORD_BITS] >> (granule % WORD_BITS)) & 1;
}

}

FileSegmentEfficiency::FileSegmentEfficiency(size_t range_size)
    : read_granules(wordCount(range_size))
    , active_granules(wordCount(range_size), 0)
{
}

void FileSegmentEfficiency::markRead(ByteRange range, ByteRange read)
{
    const auto overlap = read.intersect(range);
    if (overlap.size == 0)
        return;
    const size_t first = (overlap.offset - range.offset) / GRANULE_SIZE;
    const size_t last = (overlap.end() - 1 - range.offset) / GRANULE_SIZE;
    chassert(last / WORD_BITS < read_granules.size());
    for (size_t word = first / WORD_BITS; word <= last / WORD_BITS; ++word)
    {
        const UInt64 mask = granuleMask(word, first, last);
        /// A read of marked granules does not write, so the readers of hot data do not share a written cache line.
        if ((read_granules[word].load(std::memory_order_relaxed) & mask) != mask)
            read_granules[word].fetch_or(mask, std::memory_order_relaxed);
    }
}

bool FileSegmentEfficiency::hasUnaccountedReads() const
{
    return std::ranges::any_of(read_granules, [](const auto & word) { return word.load(std::memory_order_relaxed) != 0; });
}

void FileSegmentEfficiency::account(FileCacheEfficiency & cache_efficiency, size_t range_size, Int64 reserved_bytes, bool large)
{
    if (!hasUnaccountedReads())
        return;

    const auto current_window = cache_efficiency.currentWindow();
    const bool new_window = window != current_window;
    if (new_window)
        startWindow(current_window, range_size);

    /// A granule that a reader marks after its word is taken stays for the next call.
    const size_t last = lastGranuleNumber(range_size);
    size_t new_granules = 0;
    bool new_last = false;
    for (size_t word = 0; word < read_granules.size(); ++word)
    {
        const UInt64 new_bits = read_granules[word].exchange(0, std::memory_order_relaxed) & ~active_granules[word];
        active_granules[word] |= new_bits;
        new_granules += static_cast<size_t>(std::popcount(new_bits));
        if (word == last / WORD_BITS)
            new_last = (new_bits >> (last % WORD_BITS)) & 1;
    }
    auto new_active = static_cast<Int64>(new_granules) * static_cast<Int64>(GRANULE_SIZE);
    if (new_last)
        new_active -= static_cast<Int64>(GRANULE_SIZE - lastGranuleSize(range_size));

    cache_efficiency.apply(window, new_window
        ? FileCacheEfficiency::Delta::enroll(new_active, reserved_bytes - new_active, large)
        : FileCacheEfficiency::Delta::move(new_active, large));
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

size_t FileSegmentEfficiency::getActiveBytes(size_t range_size) const
{
    const size_t last = lastGranuleNumber(range_size);
    chassert(last / WORD_BITS < active_granules.size());
    size_t set = 0;
    for (const UInt64 word : active_granules)
        set += static_cast<size_t>(std::popcount(word));
    if (!isSet(active_granules, last))
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
    std::ranges::fill(active_granules, 0);
    window = new_window;
}

}
