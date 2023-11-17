#include <filesystem>
#include <Coordination/Changelog.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <IO/ZstdDeflatingAppendableWriteBuffer.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/filesystemHelpers.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromFile.h>
#include <base/errnoToString.h>
#include <libnuraft/log_val_type.hxx>


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

std::string formatChangelogPath(
    const std::string & prefix, const std::string & name_prefix, uint64_t from_index, uint64_t to_index, const std::string & extension)
{
    std::filesystem::path path(prefix);
    path /= std::filesystem::path(fmt::format("{}_{}_{}.{}", name_prefix, from_index, to_index, extension));
    return path;
}

ChangelogFileDescriptionPtr getChangelogFileDescription(const std::filesystem::path & path)
{
    // we can have .bin.zstd so we cannot use std::filesystem stem and extension
    std::string filename_with_extension = path.filename();
    std::string_view filename_with_extension_view = filename_with_extension;

    auto first_dot = filename_with_extension.find('.');
    if (first_dot == std::string::npos)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid changelog file {}", path.generic_string());

    Strings filename_parts;
    boost::split(filename_parts, filename_with_extension_view.substr(0, first_dot), boost::is_any_of("_"));
    if (filename_parts.size() < 3)
        throw Exception(ErrorCodes::CORRUPTED_DATA, "Invalid changelog {}", path.generic_string());

    auto result = std::make_shared<ChangelogFileDescription>();
    result->prefix = filename_parts[0];
    result->from_log_index = parse<uint64_t>(filename_parts[1]);
    result->to_log_index = parse<uint64_t>(filename_parts[2]);
    result->extension = std::string(filename_with_extension.substr(first_dot + 1));
    result->path = path.generic_string();
    return result;
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

/// Appendable log writer
/// New file on disk will be created when:
/// - we have already "rotation_interval" amount of logs in a single file
/// - maximum log file size is reached
/// At least 1 log record should be contained in each log
class ChangelogWriter
{
public:
    ChangelogWriter(
        std::map<uint64_t, ChangelogFileDescriptionPtr> & existing_changelogs_,
        const std::filesystem::path & changelogs_dir_,
        LogFileSettings log_file_settings_)
        : existing_changelogs(existing_changelogs_)
        , log_file_settings(log_file_settings_)
        , changelogs_dir(changelogs_dir_)
        , log(&Poco::Logger::get("Changelog"))
    {
    }

    void setFile(ChangelogFileDescriptionPtr file_description, WriteMode mode)
    {
        try
        {
            if (mode == WriteMode::Append && file_description->expectedEntriesCountInLog() != log_file_settings.rotate_interval)
                LOG_TRACE(
                    log,
                    "Looks like rotate_logs_interval was changed, current {}, expected entries in last log {}",
                    log_file_settings.rotate_interval,
                    file_description->expectedEntriesCountInLog());

            // we have a file we need to finalize first
            if (tryGetFileBuffer() && prealloc_done)
            {
                finalizeCurrentFile();

                assert(current_file_description);
                // if we wrote at least 1 log in the log file we can rename the file to reflect correctly the
                // contained logs
                // file can be deleted from disk earlier by compaction
                if (!current_file_description->deleted && last_index_written
                    && *last_index_written != current_file_description->to_log_index)
                {
                    auto new_path = formatChangelogPath(
                        changelogs_dir,
                        current_file_description->prefix,
                        current_file_description->from_log_index,
                        *last_index_written,
                        current_file_description->extension);
                    std::filesystem::rename(current_file_description->path, new_path);
                    current_file_description->path = std::move(new_path);
                }
            }

            file_buf = std::make_unique<WriteBufferFromFile>(
                file_description->path, DBMS_DEFAULT_BUFFER_SIZE, mode == WriteMode::Rewrite ? -1 : (O_APPEND | O_CREAT | O_WRONLY));
            last_index_written.reset();
            current_file_description = std::move(file_description);

            if (log_file_settings.compress_logs)
                compressed_buffer = std::make_unique<ZstdDeflatingAppendableWriteBuffer>(std::move(file_buf), /* compression level = */ 3, /* append_to_existing_file_ = */ mode == WriteMode::Append);

            prealloc_done = false;
        }
        catch (...)
        {
            tryLogCurrentException(log);
            throw;
        }
    }

    bool isFileSet() const { return tryGetFileBuffer() != nullptr; }


    bool appendRecord(ChangelogRecord && record)
    {
        const auto * file_buffer = tryGetFileBuffer();
        assert(file_buffer && current_file_description);

        assert(record.header.index - getStartIndex() <= current_file_description->expectedEntriesCountInLog());
        // check if log file reached the limit for amount of records it can contain
        if (record.header.index - getStartIndex() == current_file_description->expectedEntriesCountInLog())
        {
            rotate(record.header.index);
        }
        else
        {
            // writing at least 1 log is requirement - we don't want empty log files
            // we use count() that can be unreliable for more complex WriteBuffers, so we should be careful if we change the type of it in the future
            const bool log_too_big = record.header.index != getStartIndex() && log_file_settings.max_size != 0
                && initial_file_size + file_buffer->count() > log_file_settings.max_size;

            if (log_too_big)
            {
                LOG_TRACE(log, "Log file reached maximum allowed size ({} bytes), creating new log file", log_file_settings.max_size);
                rotate(record.header.index);
            }
        }

        if (!prealloc_done) [[unlikely]]
        {
            tryPreallocateForFile();

            if (!prealloc_done)
                return false;
        }

        auto & write_buffer = getBuffer();
        writeIntBinary(computeRecordChecksum(record), write_buffer);

        writeIntBinary(record.header.version, write_buffer);

        writeIntBinary(record.header.index, write_buffer);
        writeIntBinary(record.header.term, write_buffer);
        writeIntBinary(record.header.value_type, write_buffer);
        writeIntBinary(record.header.blob_size, write_buffer);

        if (record.header.blob_size != 0)
            write_buffer.write(reinterpret_cast<char *>(record.blob->data_begin()), record.blob->size());

        if (compressed_buffer)
        {
            /// Flush compressed data to file buffer
            compressed_buffer->next();
        }

        last_index_written = record.header.index;

        return true;
    }

    void flush()
    {
        auto * file_buffer = tryGetFileBuffer();
        if (file_buffer)
        {
            /// Fsync file system if needed
            if (log_file_settings.force_sync)
                file_buffer->sync();
            else
                file_buffer->next();
        }
    }

    uint64_t getStartIndex() const
    {
        assert(current_file_description);
        return current_file_description->from_log_index;
    }

    void rotate(uint64_t new_start_log_index)
    {
        /// Start new one
        auto new_description = std::make_shared<ChangelogFileDescription>();
        new_description->prefix = DEFAULT_PREFIX;
        new_description->from_log_index = new_start_log_index;
        new_description->to_log_index = new_start_log_index + log_file_settings.rotate_interval - 1;
        new_description->extension = "bin";

        if (log_file_settings.compress_logs)
            new_description->extension += "." + toContentEncodingName(CompressionMethod::Zstd);

        new_description->path = formatChangelogPath(
            changelogs_dir,
            new_description->prefix,
            new_start_log_index,
            new_start_log_index + log_file_settings.rotate_interval - 1,
            new_description->extension);

        LOG_TRACE(log, "Starting new changelog {}", new_description->path);
        auto [it, inserted] = existing_changelogs.insert(std::make_pair(new_start_log_index, std::move(new_description)));

        setFile(it->second, WriteMode::Rewrite);
    }

    void finalize()
    {
        if (isFileSet() && prealloc_done)
            finalizeCurrentFile();
    }

private:

    void finalizeCurrentFile()
    {
        const auto * file_buffer = tryGetFileBuffer();
        assert(file_buffer && prealloc_done);

        assert(current_file_description);
        // compact can delete the file and we don't need to do anything
        if (current_file_description->deleted)
        {
            LOG_WARNING(log, "Log {} is already deleted", file_buffer->getFileName());
            return;
        }

        if (log_file_settings.compress_logs)
            compressed_buffer->finalize();

        flush();

        if (log_file_settings.max_size != 0)
            ftruncate(file_buffer->getFD(), initial_file_size + file_buffer->count());

        if (log_file_settings.compress_logs)
            compressed_buffer.reset();
        else
            file_buf.reset();
    }

    WriteBuffer & getBuffer()
    {
        if (compressed_buffer)
            return *compressed_buffer;

        if (file_buf)
            return *file_buf;

        throw Exception(ErrorCodes::LOGICAL_ERROR, "Log writer wasn't initialized for any file");
    }

    WriteBufferFromFile & getFileBuffer()
    {
        auto * file_buffer = tryGetFileBuffer();

        if (!file_buffer)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Log writer wasn't initialized for any file");

        return *file_buffer;
    }

    const WriteBufferFromFile * tryGetFileBuffer() const
    {
        return const_cast<ChangelogWriter *>(this)->tryGetFileBuffer();
    }

    WriteBufferFromFile * tryGetFileBuffer()
    {
        if (compressed_buffer)
            return dynamic_cast<WriteBufferFromFile *>(compressed_buffer->getNestedBuffer());

        if (file_buf)
            return file_buf.get();

        return nullptr;
    }

    void tryPreallocateForFile()
    {
        if (log_file_settings.max_size == 0)
        {
            initial_file_size = 0;
            prealloc_done = true;
            return;
        }

        const auto & file_buffer = getFileBuffer();
#ifdef OS_LINUX
        {
            int res = -1;
            do
            {
                res = fallocate(file_buffer.getFD(), FALLOC_FL_KEEP_SIZE, 0, log_file_settings.max_size + log_file_settings.overallocate_size);
            } while (res < 0 && errno == EINTR);

            if (res != 0)
            {
                if (errno == ENOSPC)
                {
                    LOG_FATAL(log, "Failed to allocate enough space on disk for logs");
                    return;
                }

                LOG_WARNING(log, "Could not preallocate space on disk using fallocate. Error: {}, errno: {}", errnoToString(), errno);
            }
        }
#endif
        initial_file_size = getSizeFromFileDescriptor(file_buffer.getFD());

        prealloc_done = true;
    }

    std::map<uint64_t, ChangelogFileDescriptionPtr> & existing_changelogs;

    ChangelogFileDescriptionPtr current_file_description{nullptr};
    std::unique_ptr<WriteBufferFromFile> file_buf;
    std::optional<uint64_t> last_index_written;
    size_t initial_file_size{0};

    std::unique_ptr<ZstdDeflatingAppendableWriteBuffer> compressed_buffer;

    bool prealloc_done{false};

    LogFileSettings log_file_settings;

    const std::filesystem::path changelogs_dir;

    Poco::Logger * const log;
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
    explicit ChangelogReader(const std::string & filepath_) : filepath(filepath_)
    {
        auto compression_method = chooseCompressionMethod(filepath, "");
        auto read_buffer_from_file = std::make_unique<ReadBufferFromFile>(filepath);
        read_buf = wrapReadBufferWithCompressionMethod(std::move(read_buffer_from_file), compression_method);
    }

    /// start_log_index -- all entries with index < start_log_index will be skipped, but accounted into total_entries_read_from_log
    ChangelogReadResult readChangelog(IndexToLogEntry & logs, uint64_t start_log_index, Poco::Logger * log)
    {
        ChangelogReadResult result{};
        try
        {
            while (!read_buf->eof())
            {
                result.last_position = read_buf->count();
                /// Read checksum
                Checksum record_checksum;
                readIntBinary(record_checksum, *read_buf);

                /// Read header
                ChangelogRecord record;
                readIntBinary(record.header.version, *read_buf);
                readIntBinary(record.header.index, *read_buf);
                readIntBinary(record.header.term, *read_buf);
                readIntBinary(record.header.value_type, *read_buf);
                readIntBinary(record.header.blob_size, *read_buf);

                if (record.header.version > CURRENT_CHANGELOG_VERSION)
                    throw Exception(
                        ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported changelog version {} on path {}", record.header.version, filepath);

                /// Read data
                if (record.header.blob_size != 0)
                {
                    auto buffer = nuraft::buffer::alloc(record.header.blob_size);
                    auto * buffer_begin = reinterpret_cast<char *>(buffer->data_begin());
                    read_buf->readStrict(buffer_begin, record.header.blob_size);
                    record.blob = buffer;
                }
                else
                    record.blob = nullptr;

                /// Compare checksums
                Checksum checksum = computeRecordChecksum(record);
                if (checksum != record_checksum)
                {
                    throw Exception(
                        ErrorCodes::CHECKSUM_DOESNT_MATCH,
                        "Checksums doesn't match for log {} (version {}), index {}, blob_size {}",
                        filepath,
                        record.header.version,
                        record.header.index,
                        record.header.blob_size);
                }

                /// Check for duplicated changelog ids
                if (logs.contains(record.header.index))
                    std::erase_if(logs, [&record](const auto & item) { return item.first >= record.header.index; });

                result.total_entries_read_from_log += 1;

                /// Read but skip this entry because our state is already more fresh
                if (record.header.index < start_log_index)
                    continue;

                /// Create log entry for read data
                auto log_entry = nuraft::cs_new<nuraft::log_entry>(record.header.term, record.blob, static_cast<nuraft::log_val_type>(record.header.value_type));
                if (result.first_read_index == 0)
                    result.first_read_index = record.header.index;

                /// Put it into in memory structure
                logs.emplace(record.header.index, log_entry);
                result.last_read_index = record.header.index;

                if (result.total_entries_read_from_log % 50000 == 0)
                    LOG_TRACE(log, "Reading changelog from path {}, entries {}", filepath, result.total_entries_read_from_log);
            }
        }
        catch (const Exception & ex)
        {
            if (ex.code() == ErrorCodes::UNKNOWN_FORMAT_VERSION)
                throw;

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
    std::unique_ptr<ReadBuffer> read_buf;
};

Changelog::Changelog(
    const std::string & changelogs_dir_,
    Poco::Logger * log_,
    LogFileSettings log_file_settings)
    : changelogs_dir(changelogs_dir_)
    , changelogs_detached_dir(changelogs_dir / "detached")
    , rotate_interval(log_file_settings.rotate_interval)
    , log(log_)
    , write_operations(std::numeric_limits<size_t>::max())
    , append_completion_queue(std::numeric_limits<size_t>::max())
{
    /// Load all files in changelog directory
    namespace fs = std::filesystem;
    if (!fs::exists(changelogs_dir))
        fs::create_directories(changelogs_dir);

    for (const auto & p : fs::directory_iterator(changelogs_dir))
    {
        if (p == changelogs_detached_dir)
            continue;

        auto file_description = getChangelogFileDescription(p.path());
        existing_changelogs[file_description->from_log_index] = std::move(file_description);
    }

    if (existing_changelogs.empty())
        LOG_WARNING(log, "No logs exists in {}. It's Ok if it's the first run of clickhouse-keeper.", changelogs_dir.generic_string());

    clean_log_thread = ThreadFromGlobalPool([this] { cleanLogThread(); });

    write_thread = ThreadFromGlobalPool([this] { writeThread(); });

    append_completion_thread = ThreadFromGlobalPool([this] { appendCompletionThread(); });

    current_writer = std::make_unique<ChangelogWriter>(
        existing_changelogs, changelogs_dir, log_file_settings);
}

void Changelog::readChangelogAndInitWriter(uint64_t last_commited_log_index, uint64_t logs_to_keep)
{
    std::lock_guard writer_lock(writer_mutex);
    std::optional<ChangelogReadResult> last_log_read_result;

    /// Last log has some free space to write
    bool last_log_is_not_complete = false;

    /// We must start to read from this log index
    uint64_t start_to_read_from = last_commited_log_index;

    /// If we need to have some reserved log read additional `logs_to_keep` logs
    if (start_to_read_from > logs_to_keep)
        start_to_read_from -= logs_to_keep;
    else
        start_to_read_from = 1;

    /// Got through changelog files in order of start_index
    for (const auto & [changelog_start_index, changelog_description_ptr] : existing_changelogs)
    {
        const auto & changelog_description = *changelog_description_ptr;
        /// [from_log_index.>=.......start_to_read_from.....<=.to_log_index]
        if (changelog_description.to_log_index >= start_to_read_from)
        {
            if (!last_log_read_result) /// still nothing was read
            {
                /// Our first log starts from the more fresh log_id than we required to read and this changelog is not empty log.
                /// So we are missing something in our logs, but it's not dataloss, we will receive snapshot and required
                /// entries from leader.
                if (changelog_description.from_log_index > last_commited_log_index
                    && (changelog_description.from_log_index - last_commited_log_index) > 1)
                {
                    LOG_ERROR(
                        log,
                        "Some records were lost, last committed log index {}, smallest available log index on disk {}. Hopefully will "
                        "receive missing records from leader.",
                        last_commited_log_index,
                        changelog_description.from_log_index);
                    /// Nothing to do with our more fresh log, leader will overwrite them, so remove everything and just start from last_commited_index
                    removeAllLogs();
                    min_log_id = last_commited_log_index;
                    max_log_id = last_commited_log_index == 0 ? 0 : last_commited_log_index - 1;
                    current_writer->rotate(max_log_id + 1);
                    initialized = true;
                    return;
                }
                else if (changelog_description.from_log_index > start_to_read_from)
                {
                    /// We don't have required amount of reserved logs, but nothing was lost.
                    LOG_WARNING(
                        log,
                        "Don't have required amount of reserved log records. Need to read from {}, smallest available log index on disk "
                        "{}.",
                        start_to_read_from,
                        changelog_description.from_log_index);
                }
            }
            else if ((changelog_description.from_log_index - last_log_read_result->last_read_index) > 1)
            {
                LOG_ERROR(
                    log,
                    "Some records were lost, last found log index {}, while the next log index on disk is {}. Hopefully will receive "
                    "missing records from leader.",
                    last_log_read_result->last_read_index,
                    changelog_description.from_log_index);
                removeAllLogsAfter(last_log_read_result->log_start_index);
                break;
            }

            ChangelogReader reader(changelog_description.path);
            last_log_read_result = reader.readChangelog(logs, start_to_read_from, log);
            last_log_read_result->log_start_index = changelog_description.from_log_index;

            if (last_log_read_result->error)
            {
                last_log_is_not_complete = true;
                break;
            }
            /// Otherwise we have already initialized it
            if (min_log_id == 0)
                min_log_id = last_log_read_result->first_read_index;

            if (last_log_read_result->last_read_index != 0)
                max_log_id = last_log_read_result->last_read_index;

            /// How many entries we have in the last changelog
            uint64_t log_count = changelog_description.expectedEntriesCountInLog();

            /// Unfinished log
            if (last_log_read_result->error || last_log_read_result->total_entries_read_from_log < log_count)
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
        LOG_WARNING(
            log,
            "Our most fresh log_id {} is smaller than stored data in snapshot {}. It can indicate data loss. Removing outdated logs.",
            max_log_id,
            last_commited_log_index - 1);

        removeAllLogs();
        min_log_id = last_commited_log_index;
        max_log_id = last_commited_log_index - 1;
    }
    else if (last_log_is_not_complete) /// if it's complete just start new one
    {
        assert(last_log_read_result != std::nullopt);
        assert(!existing_changelogs.empty());

        /// Actually they shouldn't exist, but to be sure we remove them
        removeAllLogsAfter(last_log_read_result->log_start_index);

        /// This log, even if it finished with error shouldn't be removed
        assert(existing_changelogs.find(last_log_read_result->log_start_index) != existing_changelogs.end());
        assert(existing_changelogs.find(last_log_read_result->log_start_index)->first == existing_changelogs.rbegin()->first);

        /// Continue to write into incomplete existing log if it doesn't finished with error
        const auto & description = existing_changelogs[last_log_read_result->log_start_index];

        if (last_log_read_result->last_read_index == 0 || last_log_read_result->error) /// If it's broken log then remove it
        {
            LOG_INFO(log, "Removing chagelog {} because it's empty or read finished with error", description->path);
            std::filesystem::remove(description->path);
            existing_changelogs.erase(last_log_read_result->log_start_index);
            std::erase_if(logs, [last_log_read_result](const auto & item) { return item.first >= last_log_read_result->log_start_index; });
        }
        else
        {
            initWriter(description);
        }
    }

    /// Start new log if we don't initialize writer from previous log. All logs can be "complete".
    if (!current_writer->isFileSet())
        current_writer->rotate(max_log_id + 1);

    initialized = true;
}


void Changelog::initWriter(ChangelogFileDescriptionPtr description)
{
    LOG_TRACE(log, "Continue to write into {}", description->path);
    current_writer->setFile(std::move(description), WriteMode::Append);
}

namespace
{

std::string getCurrentTimestampFolder()
{
    const auto timestamp = LocalDateTime{std::time(nullptr)};
    return fmt::format(
        "{:02}{:02}{:02}T{:02}{:02}{:02}",
        timestamp.year(),
        timestamp.month(),
        timestamp.day(),
        timestamp.hour(),
        timestamp.minute(),
        timestamp.second());
}

}

void Changelog::removeExistingLogs(ChangelogIter begin, ChangelogIter end)
{
    const auto timestamp_folder = changelogs_detached_dir / getCurrentTimestampFolder();

    for (auto itr = begin; itr != end;)
    {
        if (!std::filesystem::exists(timestamp_folder))
        {
            LOG_WARNING(log, "Moving broken logs to {}", timestamp_folder.generic_string());
            std::filesystem::create_directories(timestamp_folder);
        }

        LOG_WARNING(log, "Removing changelog {}", itr->second->path);
        const std::filesystem::path & path = itr->second->path;
        const auto new_path = timestamp_folder / path.filename();
        std::filesystem::rename(path, new_path);
        itr = existing_changelogs.erase(itr);
    }
}

void Changelog::removeAllLogsAfter(uint64_t remove_after_log_start_index)
{
    auto start_to_remove_from_itr = existing_changelogs.upper_bound(remove_after_log_start_index);
    if (start_to_remove_from_itr == existing_changelogs.end())
        return;

    size_t start_to_remove_from_log_id = start_to_remove_from_itr->first;

    /// All subsequent logs shouldn't exist. But they may exist if we crashed after writeAt started. Remove them.
    LOG_WARNING(log, "Removing changelogs that go after broken changelog entry");
    removeExistingLogs(start_to_remove_from_itr, existing_changelogs.end());

    std::erase_if(logs, [start_to_remove_from_log_id](const auto & item) { return item.first >= start_to_remove_from_log_id; });
}

void Changelog::removeAllLogs()
{
    LOG_WARNING(log, "Removing all changelogs");
    removeExistingLogs(existing_changelogs.begin(), existing_changelogs.end());
    logs.clear();
}

ChangelogRecord Changelog::buildRecord(uint64_t index, const LogEntryPtr & log_entry)
{
    ChangelogRecord record;
    record.header.version = ChangelogVersion::V1;
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
void Changelog::appendCompletionThread()
{
    bool append_ok = false;
    while (append_completion_queue.pop(append_ok))
    {
        if (!append_ok)
            current_writer->finalize();

        // we shouldn't start the raft_server before sending it here
        if (auto raft_server_locked = raft_server.lock())
            raft_server_locked->notify_log_append_completion(append_ok);
        else
            LOG_WARNING(log, "Raft server is not set in LogStore.");
    }
}

void Changelog::writeThread()
{
    WriteOperation write_operation;
    bool batch_append_ok = true;
    while (write_operations.pop(write_operation))
    {
        assert(initialized);

        if (auto * append_log = std::get_if<AppendLog>(&write_operation))
        {
            if (!batch_append_ok)
                continue;

            std::lock_guard writer_lock(writer_mutex);
            assert(current_writer);

            batch_append_ok = current_writer->appendRecord(buildRecord(append_log->index, append_log->log_entry));
        }
        else
        {
            const auto & flush = std::get<Flush>(write_operation);

            if (batch_append_ok)
            {
                {
                    std::lock_guard writer_lock(writer_mutex);
                    current_writer->flush();
                }

                {
                    std::lock_guard lock{durable_idx_mutex};
                    last_durable_idx = flush.index;
                }
            }
            else
            {
                *flush.failed = true;
            }

            durable_idx_cv.notify_all();

            // we need to call completion callback in another thread because it takes a global lock for the NuRaft server
            // NuRaft will in some places wait for flush to be done while having the same global lock leading to deadlock
            // -> future write operations are blocked by flush that cannot be completed because it cannot take NuRaft lock
            // -> NuRaft won't leave lock until its flush is done
            if (!append_completion_queue.push(batch_append_ok))
                LOG_WARNING(log, "Changelog is shut down");

            batch_append_ok = true;
        }
    }
}


void Changelog::appendEntry(uint64_t index, const LogEntryPtr & log_entry)
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Changelog must be initialized before appending records");

    if (logs.empty())
        min_log_id = index;

    logs[index] = log_entry;
    max_log_id = index;

    if (!write_operations.push(AppendLog{index, log_entry}))
        LOG_WARNING(log, "Changelog is shut down");
}

void Changelog::writeAt(uint64_t index, const LogEntryPtr & log_entry)
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Changelog must be initialized before writing records");

    {

        std::lock_guard lock(writer_mutex);
        /// This write_at require to overwrite everything in this file and also in previous file(s)
        const bool go_to_previous_file = index < current_writer->getStartIndex();

        if (go_to_previous_file)
        {
            auto index_changelog = existing_changelogs.lower_bound(index);

            ChangelogFileDescriptionPtr description{nullptr};

            if (index_changelog->first == index) /// exactly this file starts from index
                description = index_changelog->second;
            else
                description = std::prev(index_changelog)->second;

            current_writer->setFile(std::move(description), WriteMode::Append);

            /// Remove all subsequent files if overwritten something in previous one
            auto to_remove_itr = existing_changelogs.upper_bound(index);
            for (auto itr = to_remove_itr; itr != existing_changelogs.end();)
            {
                std::filesystem::remove(itr->second->path);
                itr = existing_changelogs.erase(itr);
            }
        }
    }

    /// Remove redundant logs from memory
    /// Everything >= index must be removed
    std::erase_if(logs, [index](const auto & item) { return item.first >= index; });

    /// Now we can actually override entry at index
    appendEntry(index, log_entry);
}

void Changelog::compact(uint64_t up_to_log_index)
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Changelog must be initialized before compacting records");

    std::lock_guard lock(writer_mutex);
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
        auto & changelog_description = *itr->second;
        /// Remove all completely outdated changelog files
        if (remove_all_logs || changelog_description.to_log_index <= up_to_log_index)
        {
            if (current_writer && changelog_description.from_log_index == current_writer->getStartIndex())
            {
                LOG_INFO(
                    log,
                    "Trying to remove log {} which is current active log for write. Possibly this node recovers from snapshot",
                    changelog_description.path);
                need_rotate = true;
            }

            LOG_INFO(log, "Removing changelog {} because of compaction", changelog_description.path);

            /// If failed to push to queue for background removing, then we will remove it now
            if (!log_files_to_delete_queue.tryPush(changelog_description.path, 1))
            {
                std::error_code ec;
                std::filesystem::remove(changelog_description.path, ec);
                if (ec)
                    LOG_WARNING(log, "Failed to remove changelog {} in compaction, error message: {}", changelog_description.path, ec.message());
                else
                    LOG_INFO(log, "Removed changelog {} because of compaction", changelog_description.path);
            }

            changelog_description.deleted = true;

            itr = existing_changelogs.erase(itr);
        }
        else /// Files are ordered, so all subsequent should exist
            break;
    }
    /// Compaction from the past is possible, so don't make our min_log_id smaller.
    min_log_id = std::max(min_log_id, up_to_log_index + 1);
    std::erase_if(logs, [up_to_log_index](const auto & item) { return item.first <= up_to_log_index; });

    if (need_rotate)
        current_writer->rotate(up_to_log_index + 1);

    LOG_INFO(log, "Compaction up to {} finished new min index {}, new max index {}", up_to_log_index, min_log_id, max_log_id);
}

LogEntryPtr Changelog::getLastEntry() const
{
    /// This entry treaded in special way by NuRaft
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

LogEntryPtr Changelog::getLatestConfigChange() const
{
    for (const auto & [_, entry] : logs)
        if (entry->get_val_type() == nuraft::conf)
            return entry;
    return nullptr;
}

nuraft::ptr<nuraft::buffer> Changelog::serializeEntriesToBuffer(uint64_t index, int32_t count)
{
    std::vector<nuraft::ptr<nuraft::buffer>> returned_logs;
    returned_logs.reserve(count);

    uint64_t size_total = 0;
    for (uint64_t i = index; i < index + count; ++i)
    {
        auto entry = logs.find(i);
        if (entry == logs.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Don't have log entry {}", i);

        nuraft::ptr<nuraft::buffer> buf = entry->second->serialize();
        size_total += buf->size();
        returned_logs.push_back(std::move(buf));
    }

    nuraft::ptr<nuraft::buffer> buf_out = nuraft::buffer::alloc(sizeof(int32_t) + count * sizeof(int32_t) + size_total);
    buf_out->pos(0);
    buf_out->put(static_cast<int32_t>(count));

    for (auto & entry : returned_logs)
    {
        buf_out->put(static_cast<int32_t>(entry->size()));
        buf_out->put(*entry);
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
        if (i == 0 && logs.contains(cur_index))
            writeAt(cur_index, log_entry);
        else
            appendEntry(cur_index, log_entry);
    }
}

bool Changelog::flush()
{
    if (auto failed_ptr = flushAsync())
    {
        std::unique_lock lock{durable_idx_mutex};
        durable_idx_cv.wait(lock, [&] { return *failed_ptr || last_durable_idx == max_log_id; });

        return !*failed_ptr;
    }

    // if we are shutting down let's return true to avoid abort inside NuRaft
    // this can only happen when the config change is appended so no data loss should happen
    return true;
}

std::shared_ptr<bool> Changelog::flushAsync()
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Changelog must be initialized before flushing records");

    auto failed = std::make_shared<bool>(false);
    bool pushed = write_operations.push(Flush{max_log_id, failed});

    if (!pushed)
    {
        LOG_WARNING(log, "Changelog is shut down");
        return nullptr;
    }
    return failed;
}

void Changelog::shutdown()
{
    if (!log_files_to_delete_queue.isFinished())
        log_files_to_delete_queue.finish();

    if (clean_log_thread.joinable())
        clean_log_thread.join();

    if (!write_operations.isFinished())
        write_operations.finish();

    if (write_thread.joinable())
        write_thread.join();

    if (!append_completion_queue.isFinished())
        append_completion_queue.finish();

    if (append_completion_thread.joinable())
        append_completion_thread.join();

    if (current_writer)
    {
        current_writer->finalize();
        current_writer.reset();
    }
}

Changelog::~Changelog()
{
    try
    {
        flush();
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void Changelog::cleanLogThread()
{
    std::string path;
    while (log_files_to_delete_queue.pop(path))
    {
        std::error_code ec;
        if (std::filesystem::remove(path, ec))
            LOG_INFO(log, "Removed changelog {} because of compaction.", path);
        else
            LOG_WARNING(log, "Failed to remove changelog {} in compaction, error message: {}", path, ec.message());
    }
}

void Changelog::setRaftServer(const nuraft::ptr<nuraft::raft_server> & raft_server_)
{
    assert(raft_server_);
    raft_server = raft_server_;
}

}
