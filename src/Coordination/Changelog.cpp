#include <Coordination/Changelog.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/ReadBufferFromFile.h>
#include <filesystem>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int LOGICAL_ERROR;
}

namespace
{

constexpr auto DEFAULT_PREFIX = "changelog";

std::string formatChangelogPath(const std::string & prefix, const ChangelogFileDescription & name)
{
    std::filesystem::path path(prefix);
    path /= std::filesystem::path(name.prefix + "_" + std::to_string(name.from_log_index) + "_" + std::to_string(name.to_log_index) + ".bin");
    return path;
}

ChangelogFileDescription getChangelogFileDescription(const std::string & path_str)
{
    std::filesystem::path path(path_str);
    std::string filename = path.stem();
    Strings filename_parts;
    boost::split(filename_parts, filename, boost::is_any_of("_"));
    if (filename_parts.size() < 3)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid changelog {}", path_str);

    ChangelogFileDescription result;
    result.prefix = filename_parts[0];
    result.from_log_index = parse<uint64_t>(filename_parts[1]);
    result.to_log_index = parse<uint64_t>(filename_parts[2]);
    result.path = path_str;
    return result;
}

LogEntryPtr makeClone(const LogEntryPtr & entry)
{
    return cs_new<nuraft::log_entry>(entry->get_term(), nuraft::buffer::clone(entry->get_buf()), entry->get_val_type());
}

Checksum computeRecordChecksum(const ChangelogRecord & record)
{
    SipHash hash;
    hash.update(record.header.version);
    hash.update(record.header.index);
    hash.update(record.header.term);
    hash.update(record.header.value_type);
    hash.update(record.header.blob_size);
    if (record.header.blob_size != 0)
        hash.update(reinterpret_cast<char *>(record.blob->data_begin()), record.blob->size());
    return hash.get64();
}

}

class ChangelogWriter
{
public:
    ChangelogWriter(const std::string & filepath_, WriteMode mode, uint64_t start_index_)
        : filepath(filepath_)
        , plain_buf(filepath, DBMS_DEFAULT_BUFFER_SIZE, mode == WriteMode::Rewrite ? -1 : (O_APPEND | O_CREAT | O_WRONLY))
        , start_index(start_index_)
    {}


    off_t appendRecord(ChangelogRecord && record)
    {
        off_t result = plain_buf.count();
        writeIntBinary(computeRecordChecksum(record), plain_buf);

        writeIntBinary(record.header.version, plain_buf);
        writeIntBinary(record.header.index, plain_buf);
        writeIntBinary(record.header.term, plain_buf);
        writeIntBinary(record.header.value_type, plain_buf);
        writeIntBinary(record.header.blob_size, plain_buf);

        if (record.header.blob_size != 0)
            plain_buf.write(reinterpret_cast<char *>(record.blob->data_begin()), record.blob->size());

        entries_written++;

        return result;
    }

    void truncateToLength(off_t new_length)
    {
        plain_buf.next();
        plain_buf.truncate(new_length);
        plain_buf.seek(new_length, SEEK_SET);
    }

    void flush(bool force_fsync)
    {
        plain_buf.next();
        if (force_fsync)
            plain_buf.sync();
    }

    uint64_t getEntriesWritten() const
    {
        return entries_written;
    }

    void setEntriesWritten(uint64_t entries_written_)
    {
        entries_written = entries_written_;
    }

    uint64_t getStartIndex() const
    {
        return start_index;
    }

    void setStartIndex(uint64_t start_index_)
    {
        start_index = start_index_;
    }

private:
    std::string filepath;
    WriteBufferFromFile plain_buf;
    uint64_t entries_written = 0;
    uint64_t start_index;
};

struct ChangelogReadResult
{
    /// Total entries read from log including skipped.
    /// Useful when we decide to continue to write in the same log and want to know
    /// how many entries was already written in it.
    uint64_t total_entries_read_from_log;

    /// First index in log
    uint64_t log_start_index;

    /// First entry actually read log (not including skipped)
    uint64_t first_read_index;
    /// Last entry read from log (last entry in log)
    /// When we don't skip anything last_read_index - first_read_index = total_entries_read_from_log.
    /// But when some entries from the start of log can be skipped because they are not required.
    uint64_t last_read_index;

    /// last offset we were able to read from log
    off_t last_position;
    bool error;
};

class ChangelogReader
{
public:
    explicit ChangelogReader(const std::string & filepath_)
        : filepath(filepath_)
        , read_buf(filepath)
    {
    }

    /// start_log_index -- all entries with index < start_log_index will be skipped, but accounted into total_entries_read_from_log
    ChangelogReadResult readChangelog(IndexToLogEntry & logs, uint64_t start_log_index, IndexToOffset & index_to_offset, Poco::Logger * log)
    {
        uint64_t previous_index = 0;
        ChangelogReadResult result{};
        try
        {
            while (!read_buf.eof())
            {
                result.last_position = read_buf.count();
                Checksum record_checksum;
                readIntBinary(record_checksum, read_buf);

                /// Initialization is required, otherwise checksums may fail
                ChangelogRecord record;
                readIntBinary(record.header.version, read_buf);
                readIntBinary(record.header.index, read_buf);
                readIntBinary(record.header.term, read_buf);
                readIntBinary(record.header.value_type, read_buf);
                readIntBinary(record.header.blob_size, read_buf);

                if (record.header.version > CURRENT_CHANGELOG_VERSION)
                    throw Exception(ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported changelog version {} on path {}", record.header.version, filepath);

                if (record.header.blob_size != 0)
                {
                    auto buffer = nuraft::buffer::alloc(record.header.blob_size);
                    auto * buffer_begin = reinterpret_cast<char *>(buffer->data_begin());
                    read_buf.readStrict(buffer_begin, record.header.blob_size);
                    record.blob = buffer;
                }
                else
                    record.blob = nullptr;

                if (previous_index != 0 && previous_index + 1 != record.header.index)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "Previous log entry {}, next log entry {}, seems like some entries skipped", previous_index, record.header.index);

                previous_index = record.header.index;

                Checksum checksum = computeRecordChecksum(record);
                if (checksum != record_checksum)
                {
                    throw Exception(ErrorCodes::CHECKSUM_DOESNT_MATCH,
                                    "Checksums doesn't match for log {} (version {}), index {}, blob_size {}",
                                    filepath, record.header.version, record.header.index, record.header.blob_size);
                }

                if (logs.count(record.header.index) != 0)
                    throw Exception(ErrorCodes::CORRUPTED_DATA, "Duplicated index id {} in log {}", record.header.index, filepath);

                result.total_entries_read_from_log += 1;

                if (record.header.index < start_log_index)
                {
                    continue;
                }

                auto log_entry = nuraft::cs_new<nuraft::log_entry>(record.header.term, record.blob, record.header.value_type);
                if (result.first_read_index == 0)
                    result.first_read_index = record.header.index;

                logs.emplace(record.header.index, log_entry);
                index_to_offset[record.header.index] = result.last_position;
                result.last_read_index = record.header.index;

                if (result.total_entries_read_from_log % 50000 == 0)
                    LOG_TRACE(log, "Reading changelog from path {}, entries {}", filepath, result.total_entries_read_from_log);
            }
        }
        catch (const Exception & ex)
        {
            if (ex.code() == ErrorCodes::UNKNOWN_FORMAT_VERSION)
                throw ex;

            result.error = true;
            LOG_WARNING(log, "Cannot completely read changelog on path {}, error: {}", filepath, ex.message());
        }
        catch (...)
        {
            result.error = true;
            tryLogCurrentException(log);
        }
        LOG_TRACE(log, "Totally read from changelog {} {} entries", filepath, result.total_entries_read_from_log);

        return result;
    }

private:
    std::string filepath;
    ReadBufferFromFile read_buf;
};

Changelog::Changelog(
    const std::string & changelogs_dir_,
    uint64_t rotate_interval_,
    bool force_sync_,
    Poco::Logger * log_)
    : changelogs_dir(changelogs_dir_)
    , rotate_interval(rotate_interval_)
    , force_sync(force_sync_)
    , log(log_)
{
    namespace fs = std::filesystem;
    if (!fs::exists(changelogs_dir))
        fs::create_directories(changelogs_dir);

    for (const auto & p : fs::directory_iterator(changelogs_dir))
    {
        auto file_description = getChangelogFileDescription(p.path());
        existing_changelogs[file_description.from_log_index] = file_description;
    }
}

void Changelog::readChangelogAndInitWriter(uint64_t last_commited_log_index, uint64_t logs_to_keep)
{
    std::optional<ChangelogReadResult> last_log_read_result;

    /// Last log has some free space to write
    bool last_log_is_not_complete = false;

    uint64_t start_to_read_from = last_commited_log_index;
    if (start_to_read_from > logs_to_keep)
        start_to_read_from -= logs_to_keep;
    else
        start_to_read_from = 1;

    /// Got through changelog files in order of start_index
    for (const auto & [changelog_start_index, changelog_description] : existing_changelogs)
    {
        if (changelog_description.to_log_index >= start_to_read_from)
        {
            if (!last_log_read_result) /// still nothing was read
            {
                if (changelog_description.from_log_index > last_commited_log_index && (changelog_description.from_log_index - last_commited_log_index) > 1)
                {
                    LOG_ERROR(log, "Some records was lost, last committed log index {}, smallest available log index on disk {}. Hopefully will receive missing records from leader.", last_commited_log_index, changelog_description.from_log_index);
                    /// Nothing to do with our more fresh log, leader will overwrite them, so remove everything and just start from last_commited_index
                    removeAllLogs();
                    min_log_id = last_commited_log_index;
                    max_log_id = last_commited_log_index == 0 ? 0 : last_commited_log_index - 1;
                    rotate(max_log_id + 1);
                    return;
                }
                else if (changelog_description.from_log_index > start_to_read_from)
                    LOG_WARNING(log, "Don't have required amount of reserved log records. Need to read from {}, smallest available log index on disk {}.", start_to_read_from, changelog_description.from_log_index);
            }

            ChangelogReader reader(changelog_description.path);
            last_log_read_result = reader.readChangelog(logs, start_to_read_from, index_to_start_pos, log);

            /// Otherwise we have already initialized it
            if (min_log_id == 0)
                min_log_id = last_log_read_result->first_read_index;

            if (last_log_read_result->last_read_index != 0)
                max_log_id = last_log_read_result->last_read_index;

            last_log_read_result->log_start_index = changelog_description.from_log_index;

            /// How many entries we have in the last changelog
            uint64_t expected_entries_in_log = changelog_description.expectedEntriesCountInLog();

            /// May happen after truncate, crash or simply unfinished log
            if (last_log_read_result->total_entries_read_from_log < expected_entries_in_log)
            {
                last_log_is_not_complete = true;
                break;
            }
        }
    }

    /// we can have empty log (with zero entries) and last_log_read_result will be initialized
    if (!last_log_read_result || min_log_id == 0) /// We just may have no logs (only snapshot or nothing)
    {
        /// Just to be sure they don't exist
        removeAllLogs();

        min_log_id = last_commited_log_index;
        max_log_id = last_commited_log_index == 0 ? 0 : last_commited_log_index - 1;
    }
    else if (last_commited_log_index != 0 && max_log_id < last_commited_log_index - 1) /// If we have more fresh snapshot than our logs
    {
        LOG_WARNING(log, "Our most fresh log_id {} is smaller than stored data in snapshot {}. It can indicate data loss. Removing outdated logs.", max_log_id, last_commited_log_index - 1);

        removeAllLogs();
        min_log_id = last_commited_log_index;
        max_log_id = last_commited_log_index - 1;
    }
    else if (last_log_is_not_complete) /// if it's complete just start new one
    {
        assert(last_log_read_result != std::nullopt);
        /// Actually they shouldn't exist, but to be sure we remove them
        removeAllLogsAfter(last_log_read_result->log_start_index);

        assert(!existing_changelogs.empty());
        assert(existing_changelogs.find(last_log_read_result->log_start_index)->first == existing_changelogs.rbegin()->first);

        /// Continue to write into incomplete existing log
        auto description = existing_changelogs[last_log_read_result->log_start_index];

        if (last_log_read_result->error)
            initWriter(description, last_log_read_result->total_entries_read_from_log,  /* truncate_to_offset = */ last_log_read_result->last_position);
        else
            initWriter(description, last_log_read_result->total_entries_read_from_log);
    }

    /// Start new log if we don't initialize writer from previous log. All logs can be "complete".
    if (!current_writer)
        rotate(max_log_id + 1);
}


void Changelog::initWriter(const ChangelogFileDescription & description, uint64_t entries_already_written, std::optional<uint64_t> truncate_to_offset)
{
    if (description.expectedEntriesCountInLog() != rotate_interval)
        LOG_TRACE(log, "Looks like rotate_logs_interval was changed, current {}, expected entries in last log {}", rotate_interval, description.expectedEntriesCountInLog());

    LOG_TRACE(log, "Continue to write into {}", description.path);
    current_writer = std::make_unique<ChangelogWriter>(description.path, WriteMode::Append, description.from_log_index);
    current_writer->setEntriesWritten(entries_already_written);

    if (truncate_to_offset)
    {
        LOG_WARNING(log, "Changelog {} contain broken enties, truncating all broken log entries", description.path);
        current_writer->truncateToLength(*truncate_to_offset);
    }
}

void Changelog::removeAllLogsAfter(uint64_t start_to_remove_from_id)
{
    auto start_to_remove_from = existing_changelogs.upper_bound(start_to_remove_from_id);

    /// All subsequent logs shouldn't exist. But they may exist if we crashed after writeAt started. Remove them.
    for (auto itr = start_to_remove_from; itr != existing_changelogs.end();)
    {
        LOG_WARNING(log, "Removing changelog {}, because it's goes after broken changelog entry", itr->second.path);
        std::filesystem::remove(itr->second.path);
        itr = existing_changelogs.erase(itr);
    }
}

void Changelog::removeAllLogs()
{
    LOG_WARNING(log, "Removing all changelogs");
    for (auto itr = existing_changelogs.begin(); itr != existing_changelogs.end();)
    {
        LOG_WARNING(log, "Removing changelog {}, because it's goes after broken changelog entry", itr->second.path);
        std::filesystem::remove(itr->second.path);
        itr = existing_changelogs.erase(itr);
    }
}

void Changelog::rotate(uint64_t new_start_log_index)
{
    /// Flush previous log
    flush();

    ChangelogFileDescription new_description;
    new_description.prefix = DEFAULT_PREFIX;
    new_description.from_log_index = new_start_log_index;
    new_description.to_log_index = new_start_log_index + rotate_interval - 1;

    new_description.path = formatChangelogPath(changelogs_dir, new_description);

    LOG_TRACE(log, "Starting new changelog {}", new_description.path);
    existing_changelogs[new_start_log_index] = new_description;
    current_writer = std::make_unique<ChangelogWriter>(new_description.path, WriteMode::Rewrite, new_start_log_index);
}

ChangelogRecord Changelog::buildRecord(uint64_t index, const LogEntryPtr & log_entry)
{
    ChangelogRecord record;
    record.header.version = ChangelogVersion::V0;
    record.header.index = index;
    record.header.term = log_entry->get_term();
    record.header.value_type = log_entry->get_val_type();
    auto buffer = log_entry->get_buf_ptr();
    if (buffer)
        record.header.blob_size = buffer->size();
    else
        record.header.blob_size = 0;

    record.blob = buffer;

    return record;
}

void Changelog::appendEntry(uint64_t index, const LogEntryPtr & log_entry)
{
    if (!current_writer)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Changelog must be initialized before appending records");

    if (logs.empty())
        min_log_id = index;

    if (current_writer->getEntriesWritten() == rotate_interval)
        rotate(index);

    auto offset = current_writer->appendRecord(buildRecord(index, log_entry));
    if (!index_to_start_pos.try_emplace(index, offset).second)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Record with index {} already exists", index);

    logs[index] = makeClone(log_entry);
    max_log_id = index;
}

void Changelog::writeAt(uint64_t index, const LogEntryPtr & log_entry)
{
    if (index_to_start_pos.count(index) == 0)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot write at index {} because changelog doesn't contain it", index);

    bool go_to_previous_file = index < current_writer->getStartIndex();
    if (go_to_previous_file)
    {
        auto index_changelog = existing_changelogs.lower_bound(index);
        ChangelogFileDescription description;
        if (index_changelog->first == index)
            description = index_changelog->second;
        else
            description = std::prev(index_changelog)->second;

        current_writer = std::make_unique<ChangelogWriter>(description.path, WriteMode::Append, index_changelog->first);
        current_writer->setEntriesWritten(description.to_log_index - description.from_log_index + 1);
    }

    auto entries_written = current_writer->getEntriesWritten();
    current_writer->truncateToLength(index_to_start_pos[index]);

    if (go_to_previous_file)
    {
        /// Remove all subsequent files
        auto to_remove_itr = existing_changelogs.upper_bound(index);
        for (auto itr = to_remove_itr; itr != existing_changelogs.end();)
        {
            std::filesystem::remove(itr->second.path);
            itr = existing_changelogs.erase(itr);
        }
    }

    /// Remove redundant logs from memory
    for (uint64_t i = index; ; ++i)
    {
        auto log_itr = logs.find(i);
        if (log_itr == logs.end())
            break;
        logs.erase(log_itr);
        index_to_start_pos.erase(i);
        entries_written--;
    }

    current_writer->setEntriesWritten(entries_written);

    appendEntry(index, log_entry);
}

void Changelog::compact(uint64_t up_to_log_index)
{
    LOG_INFO(log, "Compact logs up to log index {}, our max log id is {}", up_to_log_index, max_log_id);

    bool remove_all_logs = false;
    if (up_to_log_index > max_log_id)
    {
        LOG_INFO(log, "Seems like this node recovers from leaders snapshot, removing all logs");
        /// If we received snapshot from leader we may compact up to more fresh log
        max_log_id = up_to_log_index;
        remove_all_logs = true;
    }

    bool need_rotate = false;
    for (auto itr = existing_changelogs.begin(); itr != existing_changelogs.end();)
    {
        /// Remove all completely outdated changelog files
        if (remove_all_logs || itr->second.to_log_index <= up_to_log_index)
        {
            if (current_writer && itr->second.from_log_index == current_writer->getStartIndex())
            {
                LOG_INFO(log, "Trying to remove log {} which is current active log for write. Possibly this node recovers from snapshot", itr->second.path);
                need_rotate = true;
                current_writer.reset();
            }

            LOG_INFO(log, "Removing changelog {} because of compaction", itr->second.path);
            std::erase_if(index_to_start_pos, [right_index = itr->second.to_log_index] (const auto & item) { return item.first <= right_index; });
            std::filesystem::remove(itr->second.path);
            itr = existing_changelogs.erase(itr);
        }
        else /// Files are ordered, so all subsequent should exist
            break;
    }
    /// Compaction from the past is possible, so don't make our min_log_id smaller.
    min_log_id = std::max(min_log_id, up_to_log_index + 1);
    std::erase_if(logs, [up_to_log_index] (const auto & item) { return item.first <= up_to_log_index; });

    if (need_rotate)
        rotate(up_to_log_index + 1);

    LOG_INFO(log, "Compaction up to {} finished new min index {}, new max index {}", up_to_log_index, min_log_id, max_log_id);
}

LogEntryPtr Changelog::getLastEntry() const
{
    static LogEntryPtr fake_entry = nuraft::cs_new<nuraft::log_entry>(0, nuraft::buffer::alloc(sizeof(uint64_t)));

    auto entry = logs.find(max_log_id);
    if (entry == logs.end())
    {
        return fake_entry;
    }

    return entry->second;
}

LogEntriesPtr Changelog::getLogEntriesBetween(uint64_t start, uint64_t end)
{
    LogEntriesPtr ret = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();

    ret->resize(end - start);
    uint64_t result_pos = 0;
    for (uint64_t i = start; i < end; ++i)
    {
        (*ret)[result_pos] = entryAt(i);
        result_pos++;
    }
    return ret;
}

LogEntryPtr Changelog::entryAt(uint64_t index)
{
    nuraft::ptr<nuraft::log_entry> src = nullptr;
    auto entry = logs.find(index);
    if (entry == logs.end())
        return nullptr;

    src = entry->second;
    return src;
}

nuraft::ptr<nuraft::buffer> Changelog::serializeEntriesToBuffer(uint64_t index, int32_t count)
{
    std::vector<nuraft::ptr<nuraft::buffer>> returned_logs;

    uint64_t size_total = 0;
    for (uint64_t i = index; i < index + count; ++i)
    {
        auto entry = logs.find(i);
        if (entry == logs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Don't have log entry {}", i);

        nuraft::ptr<nuraft::buffer> buf = entry->second->serialize();
        size_total += buf->size();
        returned_logs.push_back(buf);
    }

    nuraft::ptr<nuraft::buffer> buf_out = nuraft::buffer::alloc(sizeof(int32_t) + count * sizeof(int32_t) + size_total);
    buf_out->pos(0);
    buf_out->put(static_cast<int32_t>(count));

    for (auto & entry : returned_logs)
    {
        nuraft::ptr<nuraft::buffer> & bb = entry;
        buf_out->put(static_cast<int32_t>(bb->size()));
        buf_out->put(*bb);
    }
    return buf_out;
}

void Changelog::applyEntriesFromBuffer(uint64_t index, nuraft::buffer & buffer)
{
    buffer.pos(0);
    int num_logs = buffer.get_int();

    for (int i = 0; i < num_logs; ++i)
    {
        uint64_t cur_index = index + i;
        int buf_size = buffer.get_int();

        nuraft::ptr<nuraft::buffer> buf_local = nuraft::buffer::alloc(buf_size);
        buffer.get(buf_local);

        LogEntryPtr log_entry = nuraft::log_entry::deserialize(*buf_local);
        if (i == 0 && logs.count(cur_index))
            writeAt(cur_index, log_entry);
        else
            appendEntry(cur_index, log_entry);
    }
}

void Changelog::flush()
{
    if (current_writer)
        current_writer->flush(force_sync);
}

Changelog::~Changelog()
{
    try
    {
        flush();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
