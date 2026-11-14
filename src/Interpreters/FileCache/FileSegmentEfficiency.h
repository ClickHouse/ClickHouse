#pragma once

#include <IO/ByteRange.h>
#include <Interpreters/FileCache/FileCacheEfficiency.h>
#include <Interpreters/FileCache/FileSegmentInfo.h>

#include <optional>
#include <utility>
#include <vector>

namespace DB
{

/// The reuse of one file segment for `FileCacheEfficiency`: the granules served in its window and the
/// previous window with a hit. Not thread-safe: the file segment guards it with its own mutex.
class FileSegmentEfficiency
{
public:
    /// The last granule is cut at the end of the file segment.
    static constexpr size_t GRANULE_SIZE = 64 * 1024;

    /// Allocates the granules here, so that no allocation follows a change of the cache counters.
    explicit FileSegmentEfficiency(size_t range_size);

    void markRead(FileCacheEfficiency & cache_efficiency, ByteRange range, ByteRange read, Int64 reserved_bytes, bool large);

    void addReservedBytes(FileCacheEfficiency & cache_efficiency, Int64 delta, bool large) const;
    /// The range shrank from `old_range_size`: the last granule is cut at the new end, and the size class may change.
    void shrink(FileCacheEfficiency & cache_efficiency, size_t old_range_size, size_t range_size, Int64 reserved_bytes, bool was_large, bool large) const;
    void remove(FileCacheEfficiency & cache_efficiency, size_t range_size, Int64 reserved_bytes, bool large) const;

    FileSegmentEfficiencyInfo getInfo(FileCacheEfficiency::Window current_window, size_t range_size, size_t downloaded_size) const;

private:
    /// Granules `[first, last]`.
    using Granules = std::pair<size_t, size_t>;

    static std::optional<Granules> getGranules(ByteRange range, ByteRange read);
    /// The bytes of `granules` not set yet, or of all of them if `new_window`.
    size_t getNewActiveBytes(Granules granules, size_t range_size, bool new_window) const;
    void setGranules(Granules granules);
    size_t getActiveBytes(size_t range_size) const;
    void startWindow(FileCacheEfficiency::Window new_window, size_t range_size);

    std::vector<bool> active_granules;
    FileCacheEfficiency::Window window = FileCacheEfficiency::NEVER_READ;
    /// The window before `window` with a cache hit, and its active bytes.
    FileCacheEfficiency::Window previous_hit_window = FileCacheEfficiency::NEVER_READ;
    UInt64 previous_active_bytes = 0;
};

}
