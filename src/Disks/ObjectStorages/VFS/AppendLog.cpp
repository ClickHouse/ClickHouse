#include "AppendLog.h"

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>
#include <Poco/Checksum.h>
#include <Common/re2.h>

#include <ranges>
#include <string>

namespace DB::ErrorCodes
{
extern const int ATTEMPT_TO_READ_AFTER_EOF;
extern const int BAD_ARGUMENTS;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int CORRUPTED_DATA;
extern const int INVALID_STATE;
}

namespace DB::WAL
{

constexpr char first_ext[] = ".first";
constexpr char tmp_ext[] = ".tmp";
constexpr char prefix[] = "log_";

static UInt64 extract_start_index(const String & filename)
{
    return static_cast<UInt64>(std::stoull(filename.substr(strlen(prefix), filename.find("."))));
}

static String makeSegmentName(UInt64 start_index, std::string_view ext = "")
{
    return fmt::format("{}{:020}{}", prefix, start_index, ext);
}

static void verifyChecksum(const Entry & entry)
{
    Poco::Checksum crc(Poco::Checksum::Type::TYPE_CRC32);
    crc.update(reinterpret_cast<const char *>(&entry.index), sizeof(entry.index));
    crc.update(entry.data.data(), static_cast<unsigned int>(entry.data.size()));

    if (crc.checksum() != entry.checksum)
        throw DB::Exception(ErrorCodes::CORRUPTED_DATA, "Checksum for the entry {} does not match the computed one", entry.index);
}

struct EntryNonOwning
{
    EntryNonOwning(UInt64 index_, std::span<const char> data_) : index(index_), data(data_)
    {
        Poco::Checksum crc(Poco::Checksum::Type::TYPE_CRC32);
        crc.update(reinterpret_cast<const char *>(&index), sizeof(index));
        crc.update(data.data(), static_cast<unsigned int>(data.size()));
        checksum = crc.checksum();
    }

    UInt64 index;
    std::span<const char> data;
    UInt32 checksum;
};


class EntrySerializer
{
public:
    static void serialize(const EntryNonOwning & entry, WriteBuffer & out)
    {
        writeVarUInt(entry.index, out);
        writeStringBinary(StringRef(entry.data.data(), entry.data.size()), out);
        writeVarUInt(entry.checksum, out);
    }

    static Entry deserialize(ReadBuffer & in)
    {
        Entry entry;
        try
        {
            readVarUInt(entry.index, in);
            readBinary(entry.data, in);
            readVarUInt(entry.checksum, in);
        }
        catch (const DB::Exception & e)
        {
            if (e.code() == DB::ErrorCodes::CANNOT_READ_ALL_DATA || e.code() == DB::ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
            {
                throw Exception(
                    ErrorCodes::CORRUPTED_DATA, "Cannot read data. Source data is possibly corrupted");
            }
            else
                throw;
        }

        verifyChecksum(entry);
        return entry;
    }

    static size_t calculateSerializedSize(const EntryNonOwning & entry)
    {
        size_t res = 0;

        res += getLengthOfVarUInt(entry.index);
        res += getLengthOfVarUInt(entry.data.size_bytes());
        res += entry.data.size_bytes();
        res += getLengthOfVarUInt(entry.checksum);

        return res;
    }
};


AppendLog::AppendLog(const fs::path & log_dir_, const Settings & settings_) : log_dir(log_dir_), settings(settings_)
{
    if (!fs::exists(log_dir))
        fs::create_directories(log_dir);
    load();
}

UUID AppendLog::getID() const
{
    std::lock_guard lock(mutex);
    return log_id;
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

        log_file = std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_CREAT | O_TRUNC | O_APPEND);
        active_segment_size = 0;
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
    active_segment_size = log_file->size();
}

UInt64 AppendLog::getNextIndex() const
{
    std::lock_guard lock(mutex);
    assertNotCorrupted();
    return next_index;
}

UInt64 AppendLog::segmentsCount() const
{
    std::lock_guard lock(mutex);
    assertNotCorrupted();
    return segments.size();
}

UInt64 AppendLog::getStartIndex() const
{
    std::lock_guard lock(mutex);
    assertNotCorrupted();
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
    assertNotCorrupted();

    if (segments.empty())
        return 0;
    return next_index - segments.front().start_index;
}


UInt64 AppendLog::findNextIndex(const Segment & segment) const
{
    ReadBufferFromFile in(segment.path);
    UInt64 res = segment.start_index;

    // TODO: read and check version of segment
    while (!in.eof())
    {
        try
        {
            Entry entry = EntrySerializer::deserialize(in);
            res = entry.index + 1;
        }
        catch (const DB::Exception & e)
        {
            if (e.code() == ErrorCodes::CORRUPTED_DATA)
                is_corrupt = true;
            throw;
        }
    }
    return res;
}

void AppendLog::appendSegment(UInt64 start_index)
{
    const auto path = log_dir / makeSegmentName(start_index);
    segments.emplace_back(path, start_index);

    if (log_file)
        log_file->sync();

    log_file = std::make_unique<WriteBufferFromFile>(path, DBMS_DEFAULT_BUFFER_SIZE, O_WRONLY | O_APPEND | O_CREAT);
    active_segment_size = 0;
}

UInt64 AppendLog::append(std::span<const char> message)
{
    std::lock_guard lock(mutex);
    assertNotCorrupted();

    UInt64 index = next_index;
    EntryNonOwning entry(index, message);
    size_t entry_size = EntrySerializer::calculateSerializedSize(entry);

    if (entry_size > settings.max_segment_size)
        throw DB::Exception(
            DB::ErrorCodes::BAD_ARGUMENTS,
            "Log entry size: {} is greater than max_segment_size: {}",
            entry_size,
            settings.max_segment_size);

    if (active_segment_size + entry_size > settings.max_segment_size)
        appendSegment(next_index);

    size_t old_count = log_file->count();
    EntrySerializer::serialize(entry, *log_file);
    log_file->sync();
    next_index++;
    active_segment_size += log_file->count() - old_count;

    return index;
}

Entries AppendLog::readFront(size_t count) const
{
    std::lock_guard lock(mutex);
    assertNotCorrupted();

    Entries entries;
    entries.reserve(count);
    size_t read = 0;

    for (const auto & segment : segments)
    {
        read += readEntries(segment, entries, count - read);
        if (read >= count)
            break;
    }
    chassert(entries.size() <= count);

    return entries;
}

size_t AppendLog::readEntries(const Segment & segment, Entries & entries, size_t limit) const
{
    // TODO: Make a segment cache(LRU) for keeping entries in memory
    ReadBufferFromFile in(segment.path);
    size_t loaded = 0;

    // TODO: read and check version of segment
    while (!in.eof())
    {
        try
        {
            auto entry = EntrySerializer::deserialize(in);
            entries.push_back(entry);
        }
        catch (const DB::Exception & e)
        {
            if (e.code() == ErrorCodes::CORRUPTED_DATA)
                is_corrupt = true;
            throw;
        }
        loaded++;

        if (loaded >= limit)
            break;
    }
    return loaded;
}

void AppendLog::copyEntriesWithSkip(Segment & segment, DB::WriteBuffer & out, size_t entries_to_skip)
{
    ReadBufferFromFile in(segment.path);

    // TODO: read and check version of segment
    for (size_t i = 0; i < entries_to_skip; i++)
    {
        try
        {
            EntrySerializer::deserialize(in);
        }
        catch (const DB::Exception & e)
        {
            if (e.code() == DB::ErrorCodes::CORRUPTED_DATA)
                is_corrupt = true;
            throw;
        }
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
        if (i == segments.size() - 1)
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

    assertNotCorrupted();
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
        copyEntriesWithSkip(segments[whole_segments_to_drop], tmp_file, entries_to_drop);

    fs::rename(log_dir / tmp_name, log_dir / new_first_segment_name);
    reload();

    chassert(getStartIndexNoLock() == index);
    return getStartIndexNoLock() - start_index;
}

void AppendLog::assertNotCorrupted() const
{
    if (is_corrupt)
        throw Exception(ErrorCodes::INVALID_STATE, "WAL possibly contains corrupted data");
}

}
