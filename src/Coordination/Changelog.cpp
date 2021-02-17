#include <Coordination/Changelog.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <filesystem>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/trim.hpp>

namespace DB
{

namespace ErrorCodes
{
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int LOGICAL_ERROR;
}

std::string toString(const ChangelogVersion & version)
{
    if (version == ChangelogVersion::V0)
        return "V0";

    throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown chagelog version {}", static_cast<uint8_t>(version));
}

ChangelogVersion fromString(const std::string & version_str)
{
    if (version_str == "V0")
        return ChangelogVersion::V0;

    throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unknown chagelog version {}", version_str);
}

namespace
{

static constexpr auto DEFAULT_PREFIX = "changelog";

struct ChangelogName
{
    std::string prefix;
    size_t from_log_idx;
    size_t to_log_idx;
};

std::string formatChangelogPath(const std::string & prefix, const ChangelogName & name)
{
    std::filesystem::path path(prefix);
    path /= std::filesystem::path(name.prefix + "_" + std::to_string(name.from_log_idx) + "_" + std::to_string(name.to_log_idx) + ".bin");
    return path;
}

ChangelogName getChangelogName(const std::string & path_str)
{
    std::filesystem::path path(path_str);
    std::string filename = path.stem();
    Strings filename_parts;
    boost::split(filename_parts, filename, boost::is_any_of("_"));
    if (filename_parts.size() < 3)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid changelog {}", path_str);

    ChangelogName result;
    result.prefix = filename_parts[0];
    result.from_log_idx = parse<size_t>(filename_parts[1]);
    result.to_log_idx = parse<size_t>(filename_parts[2]);
    return result;
}

LogEntryPtr makeClone(const LogEntryPtr & entry)
{
    return cs_new<nuraft::log_entry>(entry->get_term(), nuraft::buffer::clone(entry->get_buf()), entry->get_val_type());
}

}

class ChangelogWriter
{
public:
    ChangelogWriter(const std::string & filepath_, WriteMode mode, size_t start_index_)
        : filepath(filepath_)
        , plain_buf(filepath, DBMS_DEFAULT_BUFFER_SIZE, mode == WriteMode::Rewrite ? -1 : (O_APPEND | O_CREAT | O_WRONLY))
        , start_index(start_index_)
    {}


    off_t appendRecord(ChangelogRecord && record, bool sync)
    {
        off_t result = plain_buf.count();
        writeIntBinary(record.header.version, plain_buf);
        writeIntBinary(record.header.index, plain_buf);
        writeIntBinary(record.header.term, plain_buf);
        writeIntBinary(record.header.value_type, plain_buf);
        writeIntBinary(record.header.blob_size, plain_buf);
        writeIntBinary(record.header.blob_checksum, plain_buf);

        if (record.header.blob_size != 0)
            plain_buf.write(reinterpret_cast<char *>(record.blob->data_begin()), record.blob->size());

        entries_written++;

        if (sync)
            plain_buf.sync();
        return result;
    }

    void truncateToLength(off_t new_length)
    {
        flush();
        plain_buf.truncate(new_length);
        plain_buf.seek(new_length, SEEK_SET);
    }

    void flush()
    {
        plain_buf.sync();
    }

    size_t getEntriesWritten() const
    {
        return entries_written;
    }

    void setEntriesWritten(size_t entries_written_)
    {
        entries_written = entries_written_;
    }

    size_t getStartIndex() const
    {
        return start_index;
    }

    void setStartIndex(size_t start_index_)
    {
        start_index = start_index_;
    }

private:
    std::string filepath;
    WriteBufferFromFile plain_buf;
    size_t entries_written = 0;
    size_t start_index;
};

class ChangelogReader
{
public:
    explicit ChangelogReader(const std::string & filepath_)
        : filepath(filepath_)
        , read_buf(filepath)
    {}

    size_t readChangelog(IndexToLogEntry & logs, size_t start_log_idx, IndexToOffset & index_to_offset)
    {
        size_t total_read = 0;
        while (!read_buf.eof())
        {
            total_read += 1;
            off_t pos = read_buf.count();
            ChangelogRecord record;
            readIntBinary(record.header.version, read_buf);
            readIntBinary(record.header.index, read_buf);
            readIntBinary(record.header.term, read_buf);
            readIntBinary(record.header.value_type, read_buf);
            readIntBinary(record.header.blob_size, read_buf);
            readIntBinary(record.header.blob_checksum, read_buf);
            auto buffer = nuraft::buffer::alloc(record.header.blob_size);
            auto buffer_begin = reinterpret_cast<char *>(buffer->data_begin());
            read_buf.readStrict(buffer_begin, record.header.blob_size);
            index_to_offset[record.header.index] = pos;

            Checksum checksum = CityHash_v1_0_2::CityHash128(buffer_begin, record.header.blob_size);
            if (checksum != record.header.blob_checksum)
            {
                throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH,
                                "Checksums doesn't match for log {} (version {}), index {}, blob_size {}",
                                filepath, record.header.version, record.header.index, record.header.blob_size);
            }
            if (record.header.index < start_log_idx)
                continue;

            auto log_entry = nuraft::cs_new<nuraft::log_entry>(record.header.term, buffer, record.header.value_type);
            if (!logs.try_emplace(record.header.index, log_entry).second)
                throw Exception(ErrorCodes::CORRUPTED_DATA, "Duplicated index id {} in log {}", record.header.index, filepath);
        }

        return total_read;
    }
private:
    std::string filepath;
    ReadBufferFromFile read_buf;
};

Changelog::Changelog(const std::string & changelogs_dir_, size_t rotate_interval_)
    : changelogs_dir(changelogs_dir_)
    , rotate_interval(rotate_interval_)
{
    namespace fs = std::filesystem;
    if (!fs::exists(changelogs_dir))
        fs::create_directories(changelogs_dir);

    for (const auto & p : fs::directory_iterator(changelogs_dir))
    {
        auto name = getChangelogName(p.path());
        existing_changelogs[name.from_log_idx] = p.path();
    }
}

void Changelog::readChangelogAndInitWriter(size_t from_log_idx)
{
    size_t read_from_last = 0;
    start_index = from_log_idx == 0 ? 1 : from_log_idx;
    size_t total_read = 0;
    for (const auto & [start_id, changelog_file] : existing_changelogs)
    {
        ChangelogName parsed_name = getChangelogName(changelog_file);
        if (parsed_name.to_log_idx >= from_log_idx)
        {
            ChangelogReader reader(changelog_file);
            read_from_last = reader.readChangelog(logs, from_log_idx, index_to_start_pos);
            total_read += read_from_last;
        }
    }

    if (existing_changelogs.size() > 0 && read_from_last < rotate_interval)
    {
        auto str_name = existing_changelogs.rbegin()->second;
        auto parsed_name = getChangelogName(str_name);
        current_writer = std::make_unique<ChangelogWriter>(str_name, WriteMode::Append, parsed_name.from_log_idx);
        current_writer->setEntriesWritten(read_from_last);
    }
    else
    {
        rotate(start_index + total_read);
    }
}

void Changelog::rotate(size_t new_start_log_idx)
{
    if (current_writer)
        current_writer->flush();

    ChangelogName new_name;
    new_name.prefix = DEFAULT_PREFIX;
    new_name.from_log_idx = new_start_log_idx;
    new_name.to_log_idx = new_start_log_idx + rotate_interval - 1;

    auto new_log_path = formatChangelogPath(changelogs_dir, new_name);
    existing_changelogs[new_start_log_idx] = new_log_path;
    current_writer = std::make_unique<ChangelogWriter>(new_log_path, WriteMode::Rewrite, new_start_log_idx);
}

ChangelogRecord Changelog::buildRecord(size_t index, nuraft::ptr<nuraft::log_entry> log_entry) const
{
    ChangelogRecordHeader header;
    header.index = index;
    header.term = log_entry->get_term();
    header.value_type = log_entry->get_val_type();
    auto buffer = log_entry->get_buf_ptr();
    if (buffer)
    {
        header.blob_size = buffer->size();
        header.blob_checksum = CityHash_v1_0_2::CityHash128(reinterpret_cast<char *>(buffer->data_begin()), buffer->size());
    }
    else
    {
        header.blob_size = 0;
        header.blob_checksum = std::make_pair(0, 0);
    }

    return ChangelogRecord{header, buffer};
}

void Changelog::appendEntry(size_t index, nuraft::ptr<nuraft::log_entry> log_entry)
{
    if (!current_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Changelog must be initialized before appending records");

    if (logs.empty())
        start_index = index;

    if (current_writer->getEntriesWritten() == rotate_interval)
        rotate(index);

    auto offset = current_writer->appendRecord(buildRecord(index, log_entry), true);
    if (!index_to_start_pos.try_emplace(index, offset).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Record with index {} already exists", index);

    logs[index] = makeClone(log_entry);
}

void Changelog::writeAt(size_t index, nuraft::ptr<nuraft::log_entry> log_entry)
{
    if (index_to_start_pos.count(index) == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write at index {} because changelog doesn't contain it", index);

    bool need_rollback = index < current_writer->getStartIndex();
    if (need_rollback)
    {
        auto index_changelog = existing_changelogs.lower_bound(index);
        std::string fname;
        if (index_changelog->first == index)
            fname = index_changelog->second;
        else
            fname = std::prev(index_changelog)->second;

        current_writer = std::make_unique<ChangelogWriter>(fname, WriteMode::Append, index_changelog->first);
        auto formated_name = getChangelogName(fname);
        current_writer->setEntriesWritten(formated_name.to_log_idx - formated_name.from_log_idx + 1);
    }

    auto entries_written = current_writer->getEntriesWritten();
    current_writer->truncateToLength(index_to_start_pos[index]);

    if (need_rollback)
    {
        auto to_remove_itr = existing_changelogs.upper_bound(index);
        for (auto itr = to_remove_itr; itr != existing_changelogs.end();)
        {
            std::filesystem::remove(itr->second);
            itr = existing_changelogs.erase(itr);
        }
    }

    /// Rollback in memory state
    for (auto itr = logs.lower_bound(index); itr != logs.end();)
    {
        index_to_start_pos.erase(itr->first);
        itr = logs.erase(itr);
        entries_written--;
    }

    current_writer->setEntriesWritten(entries_written);

    appendEntry(index, log_entry);
}

void Changelog::compact(size_t up_to_log_idx)
{
    for (auto itr = existing_changelogs.begin(); itr != existing_changelogs.end();)
    {
        ChangelogName parsed_name = getChangelogName(itr->second);
        if (parsed_name.to_log_idx <= up_to_log_idx)
        {
            for (size_t idx = parsed_name.from_log_idx; idx <= parsed_name.to_log_idx; ++idx)
            {
                auto index_pos = index_to_start_pos.find(idx);
                if (index_pos == index_to_start_pos.end())
                    break;
                index_to_start_pos.erase(index_pos);
            }
            std::filesystem::remove(itr->second);
            itr = existing_changelogs.erase(itr);
        }
        else
            break;
    }
    auto start = logs.begin();
    auto end = logs.upper_bound(up_to_log_idx);
    logs.erase(start, end);
    start_index = up_to_log_idx + 1;
}

LogEntryPtr Changelog::getLastEntry() const
{

    static LogEntryPtr fake_entry = nuraft::cs_new<nuraft::log_entry>(0, nuraft::buffer::alloc(sizeof(size_t)));

    size_t next_idx = getNextEntryIndex() - 1;
    auto entry = logs.find(next_idx);
    if (entry == logs.end())
        return fake_entry;

    return makeClone(entry->second);
}

LogEntriesPtr Changelog::getLogEntriesBetween(size_t start, size_t end)
{
    LogEntriesPtr ret = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();

    ret->resize(end - start);
    size_t result_pos = 0;
    for (size_t i = start; i < end; ++i)
    {
        (*ret)[result_pos] = entryAt(i);
        result_pos++;
    }
    return ret;
}

LogEntryPtr Changelog::entryAt(size_t idx)
{
    nuraft::ptr<nuraft::log_entry> src = nullptr;
    auto entry = logs.find(idx);
    if (entry == logs.end())
        return nullptr;

    src = entry->second;
    return makeClone(src);
}

nuraft::ptr<nuraft::buffer> Changelog::serializeEntriesToBuffer(size_t index, int32_t cnt)
{
    std::vector<nuraft::ptr<nuraft::buffer>> returned_logs;

    size_t size_total = 0;
    for (size_t i = index; i < index + cnt; ++i)
    {
        auto entry = logs.find(i);
        if (entry == logs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Don't have log entry {}", i);

        nuraft::ptr<nuraft::buffer> buf = entry->second->serialize();
        size_total += buf->size();
        returned_logs.push_back(buf);
    }

    nuraft::ptr<nuraft::buffer> buf_out = nuraft::buffer::alloc(sizeof(int32_t) + cnt * sizeof(int32_t) + size_total);
    buf_out->pos(0);
    buf_out->put(static_cast<int32_t>(cnt));

    for (auto & entry : returned_logs)
    {
        nuraft::ptr<nuraft::buffer> & bb = entry;
        buf_out->put(static_cast<int32_t>(bb->size()));
        buf_out->put(*bb);
    }
    return buf_out;
}

void Changelog::applyEntriesFromBuffer(size_t index, nuraft::buffer & buffer)
{
    buffer.pos(0);
    int num_logs = buffer.get_int();

    for (int i = 0; i < num_logs; ++i)
    {
        size_t cur_idx = index + i;
        int buf_size = buffer.get_int();

        nuraft::ptr<nuraft::buffer> buf_local = nuraft::buffer::alloc(buf_size);
        buffer.get(buf_local);

        LogEntryPtr log_entry = nuraft::log_entry::deserialize(*buf_local);
        if (i == 0 && logs.count(cur_idx))
            writeAt(cur_idx, log_entry);
        else
            appendEntry(cur_idx, log_entry);
    }
}

void Changelog::flush()
{
    current_writer->flush();
}

Changelog::~Changelog() = default;

}
