#include <Storages/MergeTree/MergeTreeDeduplicationLog.h>
#include <filesystem>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>

namespace DB
{

namespace
{

enum class MergeTreeDeduplicationOp : uint8_t
{
    ADD = 1,
    DROP = 2,
};

struct MergeTreeDeduplicationLogRecord
{
    MergeTreeDeduplicationOp operation;
    std::string part_name;
    std::string block_id;
};

void writeRecord(const MergeTreeDeduplicationLogRecord & record, WriteBuffer & out)
{
    writeIntText(static_cast<uint8_t>(record.operation), out);
    writeChar(' ', out);
    writeStringBinary(record.part_name, out);
    writeChar(' ', out);
    writeStringBinary(record.block_id, out);
    writeChar('\n', out);
}

void readRecord(MergeTreeDeduplicationLogRecord & record, ReadBuffer & in)
{
    uint8_t op;
    readIntText(op, in);
    record.operation = static_cast<MergeTreeDeduplicationOp>(op);
    assertChar(' ', in);
    readStringBinary(record.part_name, in);
    assertChar(' ', in);
    readStringBinary(record.block_id, in);
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
    const MergeTreeDataFormatVersion & format_version_)
    : logs_dir(logs_dir_)
    , deduplication_window(deduplication_window_)
    , rotate_interval(deduplication_window_ * 2) /// actually it doesn't matter
    , format_version(format_version_)
    , deduplication_map(deduplication_window)
{}

void MergeTreeDeduplicationLog::load()
{
    namespace fs = std::filesystem;
    if (!fs::exists(logs_dir))
        fs::create_directories(logs_dir);

    for (const auto & p : fs::directory_iterator(logs_dir))
    {
        auto path = p.path();
        auto log_number = getLogNumber(path);
        existing_logs[log_number] = {path, 0};
    }

    /// Order important
    for (auto & [log_number, desc] : existing_logs)
    {
        desc.entries_count = loadSingleLog(desc.path);
        current_log_number = log_number;
    }

    rotateAndDropIfNeeded();
}

size_t MergeTreeDeduplicationLog::loadSingleLog(const std::string & path)
{
    ReadBufferFromFile read_buf(path);

    size_t total_entries = 0;
    while (!read_buf.eof())
    {
        MergeTreeDeduplicationLogRecord record;
        readRecord(record, read_buf);
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
    current_log_number++;
    auto new_path = getLogPath(logs_dir, current_log_number);
    MergeTreeDeduplicationLogNameDescription log_description{new_path, 0};
    existing_logs.emplace(current_log_number, log_description);

    if (current_writer)
        current_writer->sync();

    current_writer = std::make_unique<WriteBufferFromFile>(log_description.path, DBMS_DEFAULT_BUFFER_SIZE, O_APPEND | O_CREAT | O_WRONLY);
}

void MergeTreeDeduplicationLog::dropOutdatedLogs()
{
    size_t current_sum = 0;
    size_t remove_from_value = 0;
    for (auto itr = existing_logs.rbegin(); itr != existing_logs.rend(); ++itr)
    {
        auto & description = itr->second;
        if (current_sum > deduplication_window)
        {
            //std::filesystem::remove(description.path);
            //itr = existing_logs.erase(itr);
            remove_from_value = itr->first;
            break;
        }
        current_sum += description.entries_count;
        ++itr;
    }

    if (remove_from_value != 0)
    {
        for (auto itr = existing_logs.begin(); itr != existing_logs.end();)
        {
        }
    }

}

void MergeTreeDeduplicationLog::rotateAndDropIfNeeded()
{
    if (existing_logs.empty() || existing_logs[current_log_number].entries_count > rotate_interval)
    {
        rotate();
        dropOutdatedLogs();
    }
}

std::pair<MergeTreePartInfo, bool> MergeTreeDeduplicationLog::addPart(const std::string & block_id, const MergeTreeData::MutableDataPartPtr & part)
{
    std::lock_guard lock(state_mutex);

    if (deduplication_map.contains(block_id))
        return std::make_pair(deduplication_map->get(block_id), false);

    assert(current_writer != nullptr);
    MergeTreeDeduplicationLogRecord record;
    record.operation = MergeTreeDeduplicationOp::ADD;
    record.part_name = part->name;
    record.block_id = block_id;
    writeRecord(record, *current_writer);
    existing_logs[current_log_number].entries_count++;

    deduplication_map.insert(record.block_id, part->info);

    return std::make_pair(part->info, true);
}

std::pair<MergeTreePartInfo, bool> MergeTreeDeduplicationLog::addPart(const MergeTreeData::MutableDataPartPtr & part)
{
    return addPart(part->getZeroLevelPartBlockID(), part);
}

void MergeTreeDeduplicationLog::dropPart(const MergeTreeData::DataPartPtr & part)
{
    std::lock_guard lock(state_mutex);

    assert(current_writer != nullptr);

    for (auto itr = deduplication_map.begin(); itr != deduplication_map.end(); ++itr)
    {
        const auto & part_info = itr->second;
        if (part.contains(part_info))
        {
            record.operation = MergeTreeDeduplicationOp::DROP;
            record.part_name = part_info.getPartName();
            record.block_id = itr->first;
            writeRecord(record, *current_writer);
            existing_logs[current_log_number].entries_count++;
            deduplication_map.erase(itr->first);
            rotateAndDropIfNeeded();
        }
    }
}

void MergeTreeDeduplicationLog::dropPartition(const std::string & partition_id)
{
    std::lock_guard lock(state_mutex);
    assert(current_writer != nullptr);

    for (auto itr = deduplication_map.begin(); itr != deduplication_map.end(); ++itr)
    {
        const auto & part_info = itr->second;
        if (part_info.partition_id == partition_id)
        {
            record.operation = MergeTreeDeduplicationOp::DROP;
            record.part_name = part_info.getPartName();
            record.block_id = itr->first;
            writeRecord(record, *current_writer);
            deduplication_map.erase(itr->first);
            existing_logs[current_log_number].entries_count++;

            rotateAndDropIfNeeded();
        }
    }
}

}
