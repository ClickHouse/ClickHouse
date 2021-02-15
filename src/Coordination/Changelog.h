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

enum class ChangelogVersion : uint8_t
{
    V0 = 0,
};

std::string toString(const ChangelogVersion & version);
ChangelogVersion fromString(const std::string & version_str);

static constexpr auto CURRENT_CHANGELOG_VERSION = ChangeLogVersion::V0;

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

using IndexToOffset = std::unordered_map<size_t, off_t>;
using IndexToLogEntry = std::map<size_t, nuraft::ptr<nuraft::log_entry>>;

struct Changelog
{
public:
private:
    IndexToLogEntry logs;
    size_t start_idx = 0;
};

class ChangelogWriter;

class ChangelogOnDiskHelper
{

public:
    ChangelogOnDiskHelper(const std::string & changelogs_dir_, size_t rotate_interval_);

    Changelog readChangelogAndInitWriter(size_t from_log_idx);

    void appendRecord(size_t index, nuraft::ptr<nuraft::log_entry> log_entry);

    void writeAt(size_t index, nuraft::ptr<nuraft::log_entry> log_entry);

    void compact(size_t up_to_log_idx);

private:
    void rotate(size_t new_start_log_idex);

    ChangelogRecord buildRecord(size_t index, nuraft::ptr<nuraft::log_entry> log_entry) const;

private:
    std::string changelogs_dir;
    std::deque<std::string> existing_changelogs;
    std::unique_ptr<ChangelogWriter> current_writer;
    IndexToOffset index_to_start_pos;
    const size_t rotate_interval;
};

}
