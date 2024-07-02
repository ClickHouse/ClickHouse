#pragma once
#include <Disks/IDisk.h>
#include <IO/WriteBufferFromFile.h>

#include <filesystem>
#include <mutex>
#include <span>

namespace fs = std::filesystem;

namespace DB
{

enum class SyncMode
{
    NO_SYNC,
    TIMED_SYNC,
    ALL_SYNC
};

struct ParsedSegmentName
{
    UInt64   start_index;
    bool is_begin = false;
    bool is_end = false;
};

struct Segment
{
    fs::path path;
    UInt64 start_index;
    bool is_first;
};

struct LogEntry
{
    UInt64 index;
    std::vector<char> data;
    UInt64 checksum;

    bool operator==(const LogEntry &) const = default;
};

struct Settings
{
    SyncMode sync_mode = SyncMode::ALL_SYNC;
    UInt64 max_segment_size = 10 * 1024 * 1024;
};

class AppendLog
{
public:

    AppendLog(const fs::path & log_dir, const Settings & settings_ = Settings{});
    UInt64 append(std::span<const char> message);
    /// Read count entries(maybe less) from the front of the log
    std::vector<LogEntry> readFront(size_t count) const;
    /// Drop up to count entries from the front of the log
    size_t dropUpTo(UInt64 index);
    size_t size() const;
    UInt64 getNextIndex() const;
    UInt64 getStartIndex() const;
private:
    void load();
    void reload();
    size_t loadSegmentEntries(const Segment & segment, std::vector<LogEntry> & sink, size_t limit) const;
    void copySegmentWithDrop(Segment & segment, WriteBuffer & out, size_t entries_to_drop);

    int findSegment(UInt64 index) const;
    UInt64 findNextIndex(const Segment & segment) const;
    UInt64 getStartIndexNoLock() const;

    mutable std::mutex mutex;
    /// The current segment file to append
    WriteBufferPtr log_file;
    size_t log_file_size = 0;

    UInt64 next_index = 0;
    fs::path log_dir;
    Settings settings;
    std::vector<Segment> segments;
};

}
