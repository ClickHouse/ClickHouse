#include "AppendLog.h"

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Poco/Checksum.h>
#include <Common/re2.h>

#include <string>

namespace DB
{

namespace ErrorCodes
{
//extern const int BAD_ARGUMENTS;
//extern const int LOGICAL_ERROR;
extern const int ARGUMENT_OUT_OF_BOUND;
}

constexpr char first_ext[] = ".first";
constexpr char tmp_ext[] = ".tmp";
constexpr char prefix[] = "log_";

namespace
{
UInt64 extract_start_index(const String & filename)
{
    return static_cast<UInt64>(std::stoull(filename.substr(strlen(prefix), filename.find("."))));
}
}


AppendLog::AppendLog(const fs::path & log_dir_, const Settings & settings_) : log_dir(log_dir_), settings(settings_)
{
    if (!fs::exists(log_dir))
        fs::create_directories(log_dir);
    load();
}

String makeSegmentName(UInt64 start_index, std::string_view ext = "")
{
    return fmt::format("{}{:020}{}", prefix, start_index, ext);
}

void AppendLog::load()
{
    // TODO: load uuid file

    for (const auto & dir_entry : fs::directory_iterator(log_dir))
    {
        const String filename = dir_entry.path().filename();

        if (!dir_entry.is_regular_file())
            continue;
        if (!filename.starts_with(prefix))
            continue;
        if (filename.ends_with(tmp_ext))
        {
            fs::remove(dir_entry.path());
            continue;
        }

        segments.emplace_back(dir_entry.path(), extract_start_index(filename));
    }
    // The last segment at the beginning
    std::ranges::sort(segments, [](const auto & lhs, const auto & rhs) { return lhs.path > rhs.path; });

    if (segments.empty())
    {
        next_index = 0;
        const auto path = log_dir / makeSegmentName(next_index);
        segments.emplace_back(path, next_index);

        log_file = std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
        return;
    }

    // Find the segment marked as first with the largest index
    auto first_segment = std::ranges::find_if(segments, [](auto && s) { return s.path.extension() == first_ext; });
    if (first_segment != segments.end())
    {
        // Remove outdated segments
        std::for_each(std::next(first_segment), segments.end(), [](auto && s) { fs::remove(s.path); });
        segments.erase(std::next(first_segment), segments.end());

        fs::path new_path = first_segment->path;
        new_path.replace_extension();
        fs::rename(first_segment->path, new_path);
        first_segment->path = new_path;
    }

    Segment & last_segment = segments.front();
    next_index = findNextIndex(last_segment);
    // Open the last segment for appending. File must exists
    log_file = std::make_unique<WriteBufferFromFile>(last_segment.path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND);
}

UInt64 AppendLog::getNextIndex() const
{
    std::lock_guard lock(mutex);
    return next_index;
}

UInt64 AppendLog::getStartIndex() const
{
    std::lock_guard lock(mutex);
    return getStartIndexNoLock();
}

UInt64 AppendLog::getStartIndexNoLock() const
{
    chassert(!segments.empty());
    return segments.front().start_index;
}

void AppendLog::reload()
{
    segments.clear();
    load();
}

size_t AppendLog::size() const
{
    std::lock_guard lock(mutex);
    if (segments.empty())
        return 0;
    return next_index - segments.front().start_index;
}


UInt64 AppendLog::findNextIndex(const Segment & segment) const
{
    ReadBufferFromFile in(segment.path);
    UInt64 seg_next_index = segment.start_index;

    while (!in.eof())
    {
        LogEntry entry;

        // TODO: read and check version of segment

        // Possible exceptions CANNOT_READ_ALL_DATA, ATTEMPT_TO_READ_AFTER_EOF etc.
        readVarUInt(entry.index, in);
        readBinary(entry.data, in);
        readVarUInt(entry.checksum, in);

        Poco::Checksum crc(Poco::Checksum::Type::TYPE_CRC32);
        crc.update(reinterpret_cast<const char *>(&entry.index), sizeof(entry.index));
        crc.update(entry.data.data(), static_cast<unsigned int>(entry.data.size()));

        if (crc.checksum() != entry.checksum)
            throw Exception(); // TODO: Throw corruption error

        seg_next_index = entry.index + 1;
    }

    return seg_next_index;
}

UInt64 AppendLog::append(std::span<const char> message)
{
    // TODO: check correctness ??
    // TODO: create new segment on overflow

    std::lock_guard lock(mutex);
    UInt64 new_index = next_index;

    Poco::Checksum crc(Poco::Checksum::Type::TYPE_CRC32);
    crc.update(reinterpret_cast<const char *>(&new_index), sizeof(new_index));
    crc.update(message.data(), static_cast<unsigned int>(message.size()));

    writeVarUInt(new_index, *log_file);
    writeStringBinary(StringRef(message.data(), message.size()), *log_file);
    writeVarUInt(crc.checksum(), *log_file);
    log_file->sync();
    next_index++;

    return new_index;
}

std::vector<LogEntry> AppendLog::readFront(size_t count) const
{
    std::lock_guard lock(mutex);

    std::vector<LogEntry> res;
    res.reserve(count);
    size_t loaded = 0;

    for (const auto & segment : segments)
    {
        loaded += loadSegmentEntries(segment, res, count - loaded);
        if (loaded >= count)
            break;
    }
    chassert(res.size() <= count);

    return res;
}

size_t AppendLog::loadSegmentEntries(const Segment & segment, std::vector<LogEntry> & sink, size_t limit) const
{
    ReadBufferFromFile in(segment.path);
    size_t loaded = 0;

    while (!in.eof())
    {
        LogEntry entry;
        // TODO: read and check version of segment

        // Possible exceptions CANNOT_READ_ALL_DATA, ATTEMPT_TO_READ_AFTER_EOF etc.
        readVarUInt(entry.index, in);
        readBinary(entry.data, in);
        readVarUInt(entry.checksum, in);

        Poco::Checksum crc(Poco::Checksum::Type::TYPE_CRC32);
        crc.update(reinterpret_cast<const char *>(&entry.index), sizeof(entry.index));
        crc.update(entry.data.data(), static_cast<unsigned int>(entry.data.size()));

        if (crc.checksum() != entry.checksum)
            throw Exception(); // TODO: Throw corruption error

        sink.push_back(std::move(entry));
        loaded++;

        if (loaded >= limit)
            break;
    }
    return loaded;
}

void AppendLog::copySegmentWithDrop(Segment & segment, WriteBuffer & out, size_t entries_to_drop)
{
    ReadBufferFromFile in(segment.path);

    for (size_t i = 0; i < entries_to_drop; i++)
    {
        LogEntry entry;
        // TODO: read and check version of segment

        // Possible exceptions CANNOT_READ_ALL_DATA, ATTEMPT_TO_READ_AFTER_EOF etc.
        readVarUInt(entry.index, in);
        readBinary(entry.data, in);
        readVarUInt(entry.checksum, in);
    }
    // Just copy the rest
    copyData(in, out);
    out.sync();
}

int AppendLog::findSegment(UInt64 index) const
{
    for (size_t i = 0; i < segments.size(); i++)
    {
        UInt64 last_index{};
        if (i == segments.size() - 1) // If it is the last segment
            last_index = next_index;
        else
            last_index = segments[i + 1].start_index;

        if (index >= segments[i].start_index && index < last_index)
            return static_cast<int>(i);
    }
    return -1;
}


size_t AppendLog::dropUpTo(UInt64 index)
{
    std::lock_guard lock(mutex);
    chassert(!segments.empty());
    size_t start_index = getStartIndexNoLock();

    if (index < start_index || index > next_index)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "The index to drop up to is out of range");

    if (index == start_index)
        return 0;

    // Find a segment containing the last index to drop
    int segment_idx = findSegment(index - 1);
    chassert(segment_idx != -1);

    size_t whole_segments_to_drop = segment_idx;
    size_t entries_to_drop = index - segments[segment_idx].start_index;

    size_t new_start_index{};
    if (whole_segments_to_drop == segments.size())
        new_start_index = next_index;
    else
        new_start_index = segments[whole_segments_to_drop].start_index + entries_to_drop;

    String new_first_segment_name = makeSegmentName(new_start_index, first_ext);

    // Create temporary file
    String tmp_name = new_first_segment_name + tmp_ext;
    auto tmp_file = WriteBufferFromFile(log_dir / tmp_name, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT);

    if (whole_segments_to_drop != segments.size())
        copySegmentWithDrop(segments[whole_segments_to_drop], tmp_file, entries_to_drop);

    fs::rename(log_dir / tmp_name, log_dir / new_first_segment_name);
    reload();

    chassert(getStartIndexNoLock() == index);
    return getStartIndexNoLock() - start_index;
}


}
