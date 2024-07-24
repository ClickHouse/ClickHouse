#pragma once

#include <IO/WriteBufferFromFile.h>
#include <Core/Types.h>

#include <filesystem>
#include <mutex>
#include <span>

namespace fs = std::filesystem;

namespace DB::WAL
{

enum class SyncMode
{
    NO_SYNC,
    TIMED_SYNC,
    ALL_SYNC
};

struct Segment
{
    fs::path path;
    UInt64 start_index;
};

struct Entry
{
    UInt64 index;
    std::vector<char> data;
    UInt32 checksum;

    bool operator==(const Entry &) const = default;
};
using Entries = std::vector<Entry>;

struct Settings
{
    SyncMode sync_mode = SyncMode::ALL_SYNC;
    UInt64 max_segment_size = 10 * 1024 * 1024;
};

class AppendLog
{
public:
    AppendLog(const fs::path & log_dir, const Settings & settings_ = Settings{});
    /// Append message at the end of the log and return index of the inserted entry
    UInt64 append(std::span<const char> message);
    /// Read count entries(maybe less) from the front of the log
    Entries readFront(size_t count) const;
    /// Drop all entries from the beginning of the log up to one with the specified index (excluding)
    size_t dropUpTo(UInt64 index);
    /// Return the number of entries in the log
    size_t size() const;
    /// Return the index of the next entry to append
    UInt64 getNextIndex() const;
    /// Return the index of the first entry in the log
    UInt64 getStartIndex() const;
    /// Return the number of segments the log contains
    size_t segmentsCount() const;
    /// Return unique log ID
    UUID getID() const;

private:
    /// Load all segments from the log directory and clean outdated segments
    void load() TSA_REQUIRES(mutex);
    /// Reload segments from the log directory
    void reload() TSA_REQUIRES(mutex);
    /// Read no more than limit entries from the segment and return how much was read
    size_t readEntries(const Segment & segment, Entries & entries, size_t limit) const TSA_REQUIRES(mutex);
    /// Copy entries from the beginning of the segment to the output buffer skipping the first ones
    void copyEntriesWithSkip(Segment & segment, WriteBuffer & out, size_t entries_to_skip) TSA_REQUIRES(mutex);
    /// Create and append new empty segment to the log
    void appendSegment(UInt64 start_index) TSA_REQUIRES(mutex);

    /// Return the sequence number of the segment containing entry with the index
    int findSegment(UInt64 index) const TSA_REQUIRES(mutex);
    /// Return the index after the last entry of the segment
    UInt64 findNextIndex(const Segment & segment) const TSA_REQUIRES(mutex);
    void assertNotCorrupted() const TSA_REQUIRES(mutex);

    UInt64 getStartIndexNoLock() const TSA_REQUIRES(mutex);

    mutable std::mutex mutex;
    /// The active segment file to append
    std::unique_ptr<WriteBufferFromFile> log_file TSA_GUARDED_BY(mutex);
    size_t active_segment_size TSA_GUARDED_BY(mutex) = 0;

    UUID log_id TSA_GUARDED_BY(mutex) {};
    /// Index of the next entry to append
    UInt64 next_index TSA_GUARDED_BY(mutex) = 0;
    /// Log working directory
    fs::path log_dir TSA_GUARDED_BY(mutex);
    /// Settings
    Settings settings TSA_GUARDED_BY(mutex);
    /// Segments compriing the log
    std::vector<Segment> segments TSA_GUARDED_BY(mutex);
    /// Whether corrupted data has been detected
    mutable bool is_corrupt TSA_GUARDED_BY(mutex) = false;
};

}
