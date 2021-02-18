#pragma once

#include <libnuraft/nuraft.hxx> // Y_IGNORE
#include <city.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/HashingWriteBuffer.h>
#include <Compression/CompressedWriteBuffer.h>
#include <Disks/IDisk.h>

namespace DB
{

using Checksum = CityHash_v1_0_2::uint128;

using LogEntryPtr = nuraft::ptr<nuraft::log_entry>;
using LogEntries = std::vector<LogEntryPtr>;
using LogEntriesPtr = nuraft::ptr<LogEntries>;

using IndexToOffset = std::unordered_map<size_t, off_t>;
using IndexToLogEntry = std::map<size_t, LogEntryPtr>;

enum class ChangelogVersion : uint8_t
{
    V0 = 0,
};

std::string toString(const ChangelogVersion & version);
ChangelogVersion fromString(const std::string & version_str);

static constexpr auto CURRENT_CHANGELOG_VERSION = ChangelogVersion::V0;

struct ChangelogRecordHeader
{
    ChangelogVersion version = CURRENT_CHANGELOG_VERSION;
    size_t index;
    size_t term;
    nuraft::log_val_type value_type;
    size_t blob_size;
    Checksum blob_checksum;
};

struct ChangelogRecord
{
    ChangelogRecordHeader header;
    nuraft::ptr<nuraft::buffer> blob;
};

struct ChangelogFileDescription
{
    std::string prefix;
    size_t from_log_idx;
    size_t to_log_idx;

    std::string path;
};

class ChangelogWriter;

class Changelog
{

public:
    Changelog(const std::string & changelogs_dir_, size_t rotate_interval_);

    void readChangelogAndInitWriter(size_t from_log_idx);

    void appendEntry(size_t index, LogEntryPtr log_entry, bool force_sync);

    void writeAt(size_t index, LogEntryPtr log_entry, bool force_sync);

    void compact(size_t up_to_log_idx);

    size_t getNextEntryIndex() const
    {
        return start_index + logs.size();
    }

    size_t getStartIndex() const
    {
        return start_index;
    }

    LogEntryPtr getLastEntry() const;

    LogEntriesPtr getLogEntriesBetween(size_t start_index, size_t end_idx);

    LogEntryPtr entryAt(size_t idx);

    nuraft::ptr<nuraft::buffer> serializeEntriesToBuffer(size_t index, int32_t cnt);

    void applyEntriesFromBuffer(size_t index, nuraft::buffer & buffer, bool force_sync);

    void flush();

    size_t size() const
    {
        return logs.size();
    }

    ~Changelog();

private:

    void rotate(size_t new_start_log_idx);

    ChangelogRecord buildRecord(size_t index, nuraft::ptr<nuraft::log_entry> log_entry) const;

private:
    std::string changelogs_dir;
    std::map<size_t, ChangelogFileDescription> existing_changelogs;
    std::unique_ptr<ChangelogWriter> current_writer;
    IndexToOffset index_to_start_pos;
    const size_t rotate_interval;
    IndexToLogEntry logs;
    size_t start_index = 0;
};

}
