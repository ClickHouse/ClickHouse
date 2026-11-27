#pragma once

#include <IO/ByteRange.h>
#include <Interpreters/FileCache/FileCacheEfficiency.h>
#include <Interpreters/FileCache/FileSegmentInfo.h>

#include <atomic>
#include <vector>

namespace DB
{

/// The reuse of one file segment for `FileCacheEfficiency`: the granules served in its window and the
/// previous window with a hit. Readers mark the granules that they read without a lock; `account` counts them
/// in the window counters of the cache. `account` and the other methods need the lock of the file segment.
class FileSegmentEfficiency
{
public:
    /// The last granule is cut at the end of the file segment.
    static constexpr size_t GRANULE_SIZE = 64 * 1024;

    /// Allocates the granules here: the range never grows.
    explicit FileSegmentEfficiency(size_t range_size);

    /// Lock-free. Marks the granules of `read` in `range`.
    void markRead(ByteRange range, ByteRange read);
    /// Lock-free. Whether some marked granules are not counted yet.
    [[nodiscard]] bool hasUnaccountedReads() const;
    /// Counts the granules marked since the last call in the current window. The first call in a window
    /// makes `reserved_bytes` passive, except the active ones.
    void account(FileCacheEfficiency & cache_efficiency, size_t range_size, Int64 reserved_bytes, bool large);

    void addReservedBytes(FileCacheEfficiency & cache_efficiency, Int64 delta, bool large) const;
    /// The range shrank from `old_range_size`: the last granule is cut at the new end, and the size class may change.
    void shrink(FileCacheEfficiency & cache_efficiency, size_t old_range_size, size_t range_size, Int64 reserved_bytes, bool was_large, bool large) const;
    void remove(FileCacheEfficiency & cache_efficiency, size_t range_size, Int64 reserved_bytes, bool large) const;

    [[nodiscard]] FileSegmentEfficiencyInfo getInfo(FileCacheEfficiency::Window current_window, size_t range_size, size_t downloaded_size) const;

private:
    [[nodiscard]] size_t getActiveBytes(size_t range_size) const;
    void startWindow(FileCacheEfficiency::Window new_window, size_t range_size);

    /// One bit per granule, 64 granules per word. `read_granules`: marked by readers since the last `account`.
    /// `active_granules`: counted in `window`.
    std::vector<std::atomic<UInt64>> read_granules;
    std::vector<UInt64> active_granules;
    FileCacheEfficiency::Window window = FileCacheEfficiency::NEVER_READ;
    /// The window before `window` with a cache hit, and its active bytes.
    FileCacheEfficiency::Window previous_hit_window = FileCacheEfficiency::NEVER_READ;
    UInt64 previous_active_bytes = 0;
};

}
