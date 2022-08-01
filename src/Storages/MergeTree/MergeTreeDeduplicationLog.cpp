#include <Storages/MergeTree/MergeTreeDeduplicationLog.h>
#include <filesystem>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFileBase.h>
#include <Disks/WriteMode.h>
#include <Disks/IDisk.h>

namespace DB
{

namespace
{

/// Deduplication operation part was dropped or added
enum class MergeTreeDeduplicationOp : uint8_t
{
    ADD = 1,
    DROP = 2,
};

/// Record for deduplication on disk
struct MergeTreeDeduplicationLogRecord
{
    MergeTreeDeduplicationOp operation;
    std::string part_name;
    std::string block_id;
};

void writeRecord(const MergeTreeDeduplicationLogRecord & record, WriteBuffer & out)
{
    writeIntText(static_cast<uint8_t>(record.operation), out);
    writeChar('\t', out);
    writeString(record.part_name, out);
    writeChar('\t', out);
    writeString(record.block_id, out);
    writeChar('\n', out);
    out.next();
}

void readRecord(MergeTreeDeduplicationLogRecord & record, ReadBuffer & in)
{
    uint8_t op;
    readIntText(op, in);
    record.operation = static_cast<MergeTreeDeduplicationOp>(op);
    assertChar('\t', in);
    readString(record.part_name, in);
    assertChar('\t', in);
    readString(record.block_id, in);
    assertChar('\n', in);
}


std::string getLogPath(const std::string & prefix, size_t number)
{
    std::filesystem::path path(prefix);
    path /= std::filesystem::path(std::string{"deduplication_log_"} + std::to_string(number) + ".txt");
    return path;
}

size_t getLogNumber(const std::string & path_str)
{
    std::filesystem::path path(path_str);
    std::string filename = path.stem();
    Strings filename_parts;
    boost::split(filename_parts, filename, boost::is_any_of("_"));

    return parse<size_t>(filename_parts[2]);
}

}

MergeTreeDeduplicationLog::MergeTreeDeduplicationLog(
    const std::string & logs_dir_,
    size_t deduplication_window_,
    const MergeTreeDataFormatVersion & format_version_,
    DiskPtr disk_)
    : logs_dir(logs_dir_)
    , deduplication_window(deduplication_window_)
    , rotate_interval(deduplication_window_ * 2) /// actually it doesn't matter
    , format_version(format_version_)
    , deduplication_map(deduplication_window)
    , disk(disk_)
{
    if (deduplication_window != 0 && !disk->exists(logs_dir))
        disk->createDirectories(logs_dir);
}

void MergeTreeDeduplicationLog::load()
{
    if (!disk->exists(logs_dir))
        return;

    for (auto it = disk->iterateDirectory(logs_dir); it->isValid(); it->next())
    {
        const auto & path = it->path();
        auto log_number = getLogNumber(path);
        existing_logs[log_number] = {path, 0};
    }

    /// We should know which logs are exist even in case
    /// of deduplication_window = 0
    if (!existing_logs.empty())
        current_log_number = existing_logs.rbegin()->first;

    if (deduplication_window != 0)
    {
        /// Order important, we load history from the begging to the end
        for (auto & [log_number, desc] : existing_logs)
        {
            try
            {
                desc.entries_count = loadSingleLog(desc.path);
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__, "Error while loading MergeTree deduplication log on path " + desc.path);
            }
        }

        /// Start new log, drop previous
        rotateAndDropIfNeeded();

        /// Can happen in case we have unfinished log
        if (!current_writer)
            current_writer = disk->writeFile(existing_logs.rbegin()->second.path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
    }
}

size_t MergeTreeDeduplicationLog::loadSingleLog(const std::string & path)
{
    auto read_buf = disk->readFile(path);

    size_t total_entries = 0;
    while (!read_buf->eof())
    {
        MergeTreeDeduplicationLogRecord record;
        readRecord(record, *read_buf);
        if (record.operation == MergeTreeDeduplicationOp::DROP)
            deduplication_map.erase(record.block_id);
        else
            deduplication_map.insert(record.block_id, MergeTreePartInfo::fromPartName(record.part_name, format_version));
        total_entries++;
    }
    return total_entries;
}

void MergeTreeDeduplicationLog::rotate()
{
    /// We don't deduplicate anything so we don't need any writers
    if (deduplication_window == 0)
        return;

    current_log_number++;
    auto new_path = getLogPath(logs_dir, current_log_number);
    MergeTreeDeduplicationLogNameDescription log_description{new_path, 0};
    existing_logs.emplace(current_log_number, log_description);

    if (current_writer)
    {
        current_writer->finalize();
        current_writer->sync();
    }

    current_writer = disk->writeFile(log_description.path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
}

void MergeTreeDeduplicationLog::dropOutdatedLogs()
{
    size_t current_sum = 0;
    size_t remove_from_value = 0;
    /// Go from end to the beginning
    for (auto itr = existing_logs.rbegin(); itr != existing_logs.rend(); ++itr)
    {
        if (current_sum > deduplication_window)
        {
            /// We have more logs than required, all older files (including current) can be dropped
            remove_from_value = itr->first;
            break;
        }

        auto & description = itr->second;
        current_sum += description.entries_count;
    }

    /// If we found some logs to drop
    if (remove_from_value != 0)
    {
        /// Go from the beginning to the end and drop all outdated logs
        for (auto itr = existing_logs.begin(); itr != existing_logs.end();)
        {
            size_t number = itr->first;
            disk->removeFile(itr->second.path);
            itr = existing_logs.erase(itr);
            if (remove_from_value == number)
                break;
        }
    }

}

void MergeTreeDeduplicationLog::rotateAndDropIfNeeded()
{
    /// If we don't have logs at all or already have enough records in current
    if (existing_logs.empty() || existing_logs[current_log_number].entries_count >= rotate_interval)
    {
        rotate();
        dropOutdatedLogs();
    }
}

std::pair<MergeTreePartInfo, bool> MergeTreeDeduplicationLog::addPart(const std::string & block_id, const MergeTreePartInfo & part_info)
{
    std::lock_guard lock(state_mutex);

    /// We support zero case because user may want to disable deduplication with
    /// ALTER MODIFY SETTING query. It's much more simpler to handle zero case
    /// here then destroy whole object, check for null pointer from different
    /// threads and so on.
    if (deduplication_window == 0)
        return std::make_pair(part_info, true);

    /// If we already have this block let's deduplicate it
    if (deduplication_map.contains(block_id))
    {
        auto info = deduplication_map.get(block_id);
        return std::make_pair(info, false);
    }

    assert(current_writer != nullptr);

    /// Create new record
    MergeTreeDeduplicationLogRecord record;
    record.operation = MergeTreeDeduplicationOp::ADD;
    record.part_name = part_info.getPartName();
    record.block_id = block_id;
    /// Write it to disk
    writeRecord(record, *current_writer);
    /// We have one more record in current log
    existing_logs[current_log_number].entries_count++;
    /// Add to deduplication map
    deduplication_map.insert(record.block_id, part_info);
    /// Rotate and drop old logs if needed
    rotateAndDropIfNeeded();

    return std::make_pair(part_info, true);
}

void MergeTreeDeduplicationLog::dropPart(const MergeTreePartInfo & drop_part_info)
{
    std::lock_guard lock(state_mutex);

    /// We support zero case because user may want to disable deduplication with
    /// ALTER MODIFY SETTING query. It's much more simpler to handle zero case
    /// here then destroy whole object, check for null pointer from different
    /// threads and so on.
    if (deduplication_window == 0)
        return;

    assert(current_writer != nullptr);

    for (auto itr = deduplication_map.begin(); itr != deduplication_map.end(); /* no increment here, we erasing from map */)
    {
        const auto & part_info = itr->value;
        /// Part is covered by dropped part, let's remove it from
        /// deduplication history
        if (drop_part_info.contains(part_info))
        {
            /// Create drop record
            MergeTreeDeduplicationLogRecord record;
            record.operation = MergeTreeDeduplicationOp::DROP;
            record.part_name = part_info.getPartName();
            record.block_id = itr->key;
            /// Write it to disk
            writeRecord(record, *current_writer);
            /// We have one more record on disk
            existing_logs[current_log_number].entries_count++;

            /// Increment itr before erase, otherwise it will invalidated
            ++itr;
            /// Remove block_id from in-memory table
            deduplication_map.erase(record.block_id);

            /// Rotate and drop old logs if needed
            rotateAndDropIfNeeded();
        }
        else
        {
            ++itr;
        }
    }
}

void MergeTreeDeduplicationLog::setDeduplicationWindowSize(size_t deduplication_window_)
{
    std::lock_guard lock(state_mutex);

    deduplication_window = deduplication_window_;
    rotate_interval = deduplication_window * 2;

    /// If settings was set for the first time with ALTER MODIFY SETTING query
    if (deduplication_window != 0 && !disk->exists(logs_dir))
        disk->createDirectories(logs_dir);

    deduplication_map.setMaxSize(deduplication_window);
    rotateAndDropIfNeeded();

    /// Can happen in case we have unfinished log
    if (!current_writer)
        current_writer = disk->writeFile(existing_logs.rbegin()->second.path, DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Append);
}


void MergeTreeDeduplicationLog::shutdown()
{
    std::lock_guard lock(state_mutex);
    if (stopped)
        return;

    stopped = true;
    if (current_writer)
    {
        current_writer->finalize();
        current_writer.reset();
    }
}

MergeTreeDeduplicationLog::~MergeTreeDeduplicationLog()
{
    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

}
