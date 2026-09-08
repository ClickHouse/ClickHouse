#include <algorithm>
#include <chrono>
#include <exception>
#include <filesystem>
#include <mutex>
#include <optional>
#include <ranges>
#include <variant>
#include <vector>
#include <Coordination/Changelog.h>
#include <Coordination/Keeper4LWInfo.h>
#include <Coordination/KeeperContext.h>
#include <Coordination/CoordinationSettings.h>
#include <Coordination/KeeperCommon.h>
#include <Disks/DiskLocal.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <IO/ZstdDeflatingAppendableWriteBuffer.h>
#include <base/errnoToString.h>
#include <base/scope_guard.h>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/SipHash.h>
#include <Common/filesystemHelpers.h>
#include <Common/getNumberOfCPUCoresToUse.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/ProfileEvents.h>
#include <Common/Stopwatch.h>
#include <libnuraft/log_val_type.hxx>
#include <libnuraft/log_entry.hxx>
#include <libnuraft/raft_server.hxx>

namespace ProfileEvents
{
    extern const Event KeeperLogsEntryReadFromLatestCache;
    extern const Event KeeperLogsEntryReadFromFile;
    extern const Event KeeperLogsReadAheadFillReopens;
    extern const Event KeeperLogsReadAheadFillDecodedEntries;
    extern const Event KeeperLogsReadAheadCursorsInstalled;
    extern const Event KeeperLogsReadAheadPlanEpochMismatches;
    extern const Event KeeperLogsReadAheadScheduleRejected;
    extern const Event KeeperLogsReadAheadReadersCreated;
    extern const Event KeeperLogsReadAheadTimeoutFallbacks;
    extern const Event KeeperLogsEntryReadFromCommitReadAhead;
    extern const Event KeeperChangelogWrittenBytes;
    extern const Event KeeperChangelogFileSyncMicroseconds;
    extern const Event KeeperChangelogStartupReadMicroseconds;
    extern const Event KeeperChangelogStartupStitchMicroseconds;
    extern const Event KeeperChangelogStartupReadEntries;
    extern const Event KeeperChangelogStartupReadBytes;
}

namespace CurrentMetrics
{
    extern const Metric KeeperChangelogReadAheadThreads;
    extern const Metric KeeperChangelogReadAheadThreadsActive;
    extern const Metric KeeperChangelogReadAheadThreadsScheduled;
    extern const Metric KeeperChangelogStartupReadThreads;
    extern const Metric KeeperChangelogStartupReadThreadsActive;
    extern const Metric KeeperChangelogStartupReadThreadsScheduled;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int CHECKSUM_DOESNT_MATCH;
    extern const int CORRUPTED_DATA;
    extern const int UNKNOWN_FORMAT_VERSION;
    extern const int NOT_IMPLEMENTED;
    extern const int BAD_ARGUMENTS;
    extern const int LOGICAL_ERROR;
    extern const int SYSTEM_ERROR;
    extern const int FAULT_INJECTED;
}

namespace FailPoints
{
    extern const char keeper_changelog_read_plan_resolved[];
    extern const char keeper_changelog_removed_from_disk_set[];
    extern const char keeper_changelog_readahead_fill_wedge[];
    extern const char keeper_changelog_readahead_serve_wait[];
    extern const char keeper_changelog_readahead_park_armed[];
    extern const char keeper_changelog_readahead_pre_drain[];
    extern const char keeper_changelog_readahead_fill_exception[];
}

namespace
{

void moveChangelogBetweenDisks(
    DiskPtr disk_from,
    ChangelogFileDescriptionPtr description,
    DiskPtr disk_to,
    const std::string & path_to,
    const KeeperContextPtr & keeper_context)
{
    auto path_from = description->path;
    moveFileBetweenDisks(
        disk_from,
        path_from,
        disk_to,
        path_to,
        [&]
        {
            /// a different thread could be trying to read from the file
            /// we should make sure the source disk contains the file while read is in progress
            description->withWriteLock(
                [&]
                {
                    description->disk = disk_to;
                    description->path = path_to;
                });
            return true;
        },
        getLogger("Changelog"),
        keeper_context);
}

constexpr auto DEFAULT_PREFIX = "changelog";

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

struct RemoveChangelog
{
};

struct MoveChangelog
{
    std::string new_path;
    DiskPtr new_disk;
};

}

using ChangelogFileOperationVariant = std::variant<RemoveChangelog, MoveChangelog>;

struct ChangelogFileOperation
{
    explicit ChangelogFileOperation(ChangelogFileDescriptionPtr changelog_, ChangelogFileOperationVariant operation_)
        : changelog(std::move(changelog_))
        , operation(std::move(operation_))
    {}

    ChangelogFileDescriptionPtr changelog;
    ChangelogFileOperationVariant operation;
    std::atomic<bool> done = false;

    void setError(std::exception_ptr e)
    {
        if (!e)
            return;
        std::lock_guard lock(error_mutex);
        if (!error)
            error = e;
    }

    std::exception_ptr getError() const
    {
        std::lock_guard lock(error_mutex);
        return error;
    }

private:
    mutable std::mutex error_mutex;
    std::exception_ptr error;
};

void ChangelogFileDescription::waitAllAsyncOperations()
{
    for (const auto & op : file_operations)
    {
        if (auto op_locked = op.lock())
            op_locked->done.wait(false);
    }

    file_operations.clear();
}

void ChangelogFileDescription::ValidRuns::addLocatedRecord(uint64_t index, size_t position, size_t size_in_file)
{
    /// Backwards means re-locating an already-located index -- a bug. Fail fast.
    if (!runs.empty() && index < end_index)
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "Valid-run metadata: located record moves backwards (index {}, already located up to {})", index, end_index);
    /// A forward gap is legitimate (compaction drops pending locations before refreshCache folds them);
    /// any gap starts a fresh run. Reads clip to retained_start, so skipped indices are never queried.
    const bool extends_last_run = !runs.empty() && index == end_index && position == end_position;
    if (!extends_last_run)
        runs.push_back(Run{.start_position = position, .first_index = index});
    end_index = index + 1;
    end_position = position + size_in_file;
}

void ChangelogFileDescription::ValidRuns::truncateAt(uint64_t index, size_t new_end_position)
{
    while (!runs.empty() && runs.back().first_index >= index)
        runs.pop_back();
    if (runs.empty())
    {
        end_index = 0;
        end_position = 0;
        return;
    }
    end_index = index;
    end_position = new_end_position;
}

void ChangelogFileDescription::ValidRuns::clear()
{
    runs.clear();
    end_index = 0;
    end_position = 0;
}

std::string Changelog::formatChangelogPath(const std::string & name_prefix, uint64_t from_index, uint64_t to_index, const std::string & extension)
{
    return fmt::format("{}_{}_{}.{}", name_prefix, from_index, to_index, extension);
}

/// Appendable log writer
/// New file on disk will be created when:
/// - we have already "rotation_interval" amount of logs in a single file
/// - maximum log file size is reached
/// At least 1 log record should be contained in each log
class ChangelogWriter
{
    using MoveChangelogCallback = std::function<void(ChangelogFileDescriptionPtr, std::string, DiskPtr)>;
public:
    ChangelogWriter(
        std::map<uint64_t, ChangelogFileDescriptionPtr> & existing_changelogs_,
        LogEntryStorage & entry_storage_,
        KeeperContextPtr keeper_context_,
        LogFileSettings log_file_settings_,
        MoveChangelogCallback move_changelog_cb_)
        : existing_changelogs(existing_changelogs_)
        , entry_storage(entry_storage_)
        , log_file_settings(log_file_settings_)
        , keeper_context(std::move(keeper_context_))
        , log(getLogger("Changelog"))
        , move_changelog_cb(std::move(move_changelog_cb_))
    {
    }

    void setFile(ChangelogFileDescriptionPtr file_description, WriteMode mode)
    {
        auto disk = getDisk();

        try
        {
            if (mode == WriteMode::Append && file_description->expectedEntriesCountInLog() != log_file_settings.rotate_interval)
                LOG_TRACE(
                    log,
                    "Looks like rotate_logs_interval was changed, current {}, expected entries in last log {}",
                    log_file_settings.rotate_interval,
                    file_description->expectedEntriesCountInLog());

            // we have a file we need to finalize first
            if (tryGetFileBaseBuffer() && prealloc_done)
            {
                chassert(current_file_description);
                // if we wrote at least 1 log in the log file we can rename the file to reflect correctly the
                // contained logs
                // file can be deleted from disk earlier by compaction
                if (current_file_description->marked_as_deleted)
                {
                    LOG_WARNING(log, "Log {} is already deleted", current_file_description->path);
                    prealloc_done = false;
                    cancelCurrentFile();
                }
                else
                {
                    finalizeCurrentFile();

                    auto log_disk = current_file_description->disk;
                    const auto & path = current_file_description->path;
                    std::string new_path = path;
                    if (last_index_written && *last_index_written != current_file_description->to_log_index)
                    {
                        new_path = Changelog::formatChangelogPath(
                            current_file_description->prefix,
                            current_file_description->from_log_index,
                            *last_index_written,
                            current_file_description->extension);

                        current_file_description->to_log_index = *last_index_written;
                    }

                    if (move_changelog_cb)
                        move_changelog_cb(current_file_description, std::move(new_path), disk);
                }
            }
            else
            {
                cancelCurrentFile();
            }

            auto latest_log_disk = getLatestLogDisk();
            chassert(file_description->disk == latest_log_disk);
            file_buf = latest_log_disk->writeFile(file_description->path, DBMS_DEFAULT_BUFFER_SIZE, mode);
            chassert(file_buf);
            last_index_written.reset();
            current_file_description = std::move(file_description);

            if (log_file_settings.compress_logs)
                compressed_buffer = std::make_unique<ZstdDeflatingAppendableWriteBuffer>(
                    std::move(file_buf),
                    /* compression level = */ 3,
                    /* append_to_existing_file_ = */ mode == WriteMode::Append,
                    [latest_log_disk, path = current_file_description->path, read_settings = getReadSettings()]
                    { return latest_log_disk->readFile(path, read_settings); });

            prealloc_done = false;
        }
        catch (...)
        {
            tryLogCurrentException(log, "While setting new changelog file");
            throw;
        }
    }

    /// There is bug when compressed_buffer has value, file_buf's ownership transfer to compressed_buffer
    bool isFileSet() const { return compressed_buffer != nullptr || file_buf != nullptr; }

    ChangelogFileDescriptionPtr getCurrentFileDescription() const { return current_file_description; }

    bool appendRecord(ChangelogRecord && record)
    {
        const auto * file_buffer = tryGetFileBaseBuffer();
        chassert(file_buffer && current_file_description);

        chassert(record.header.index - getStartIndex() <= current_file_description->expectedEntriesCountInLog());
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
        const size_t bytes_before = write_buffer.count();

        auto current_position = initial_file_size + write_buffer.count();
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
        else
        {
            unflushed_indices_with_log_location.emplace_back(
                record.header.index,
                LogLocation{
                    .file_description = current_file_description,
                    .position = current_position,
                    .entry_size = record.header.blob_size,
                    .size_in_file = initial_file_size + write_buffer.count() - current_position});
        }

        chassert(!last_index_written || *last_index_written >= record.header.index || *last_index_written == record.header.index - 1);
        last_index_written = record.header.index;

        const size_t bytes_written = write_buffer.count() - bytes_before;
        ProfileEvents::increment(ProfileEvents::KeeperChangelogWrittenBytes, bytes_written);

        return true;
    }

    void flush()
    {
        auto * file_buffer = tryGetFileBaseBuffer();
        if (file_buffer)
        {
            /// Fsync file system if needed
            if (log_file_settings.force_sync)
            {
                Stopwatch watch;

                file_buffer->sync();

                if (!compressed_buffer)
                    ProfileEvents::increment(ProfileEvents::KeeperChangelogFileSyncMicroseconds, watch.elapsedMicroseconds());
            }
            else
                file_buffer->next();
        }
        entry_storage.addLogLocations(std::move(unflushed_indices_with_log_location));
        unflushed_indices_with_log_location.clear();
    }

    uint64_t getStartIndex() const
    {
        chassert(current_file_description);
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
        new_description->disk = getLatestLogDisk();
        new_description->is_compressed = log_file_settings.compress_logs;

        if (log_file_settings.compress_logs)
            new_description->extension += "." + toContentEncodingName(CompressionMethod::Zstd);

        new_description->path = Changelog::formatChangelogPath(
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
        else
            cancelCurrentFile();
    }

private:
    void finalizeCurrentFile()
    {
        chassert(prealloc_done);

        chassert(current_file_description);
        // compact can delete the file and we don't need to do anything
        chassert(!current_file_description->marked_as_deleted);

        if (compressed_buffer)
            compressed_buffer->finalize();

        flush();

        if (file_buf)
            file_buf->finalize();

        const auto * file_buffer = tryGetFileBuffer();

        if (log_file_settings.max_size != 0 && file_buffer)
        {
            int res = -1;
            do
            {
                res = ftruncate(file_buffer->getFD(), initial_file_size + file_buffer->count());
            } while (res < 0 && errno == EINTR);

            if (res != 0)
                LOG_WARNING(log, "Could not ftruncate file. Error: {}, errno: {}", errnoToString(), errno);
        }

        compressed_buffer.reset();
        file_buf.reset();
    }

    void cancelCurrentFile()
    {
        if (compressed_buffer)
            compressed_buffer->cancel();

        if (file_buf)
            file_buf->cancel();

        compressed_buffer.reset();
        file_buf.reset();
    }

    WriteBuffer & getBuffer()
    {
        /// TODO: unify compressed_buffer and file_buf,
        /// compressed_buffer can use its NestedBuffer directly if compress_logs=false
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

    const WriteBufferFromFile * tryGetFileBuffer() const { return const_cast<ChangelogWriter *>(this)->tryGetFileBuffer(); }

    WriteBufferFromFile * tryGetFileBuffer()
    {
        if (compressed_buffer)
            return dynamic_cast<WriteBufferFromFile *>(compressed_buffer->getNestedBuffer());

        return dynamic_cast<WriteBufferFromFile *>(file_buf.get());
    }

    WriteBufferFromFileBase * tryGetFileBaseBuffer()
    {
        if (compressed_buffer)
            return dynamic_cast<WriteBufferFromFileBase *>(compressed_buffer->getNestedBuffer());

        return file_buf.get();
    }

    void tryPreallocateForFile()
    {
        const auto * file_buffer = tryGetFileBuffer();

        if (file_buffer)
            initial_file_size = getSizeFromFileDescriptor(file_buffer->getFD());

        if (log_file_settings.max_size == 0 || !file_buffer)
        {
            prealloc_done = true;
            return;
        }

#ifdef OS_LINUX
        {
            int res = -1;
            do
            {
                res = fallocate(
                    file_buffer->getFD(), FALLOC_FL_KEEP_SIZE, 0, log_file_settings.max_size + log_file_settings.overallocate_size);
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

        prealloc_done = true;
    }

    DiskPtr getLatestLogDisk() const { return keeper_context->getLatestLogDisk(); }

    DiskPtr getDisk() const { return keeper_context->getLogDisk(); }

    bool isLocalDisk() const { return dynamic_cast<DiskLocal *>(getDisk().get()) != nullptr; }

    std::map<uint64_t, ChangelogFileDescriptionPtr> & existing_changelogs;

    LogEntryStorage & entry_storage;

    std::vector<std::pair<uint64_t, LogLocation>> unflushed_indices_with_log_location;

    ChangelogFileDescriptionPtr current_file_description{nullptr};
    std::unique_ptr<WriteBufferFromFileBase> file_buf;
    std::optional<uint64_t> last_index_written;
    size_t initial_file_size{0};

    std::unique_ptr<ZstdDeflatingAppendableWriteBuffer> compressed_buffer;

    bool prealloc_done{false};

    LogFileSettings log_file_settings;

    KeeperContextPtr keeper_context;

    LoggerPtr const log;

    MoveChangelogCallback move_changelog_cb;
};

namespace
{

struct ChangelogReadResult
{
    /// Total entries read from log including skipped.
    /// Useful when we decide to continue to write in the same log and want to know
    /// how many entries was already written in it.
    uint64_t total_entries_read_from_log{0};

    /// Physical bytes consumed while validating complete records, including records skipped for retention.
    uint64_t total_bytes_read_from_log{0};

    /// First index in log
    uint64_t log_start_index{0};

    /// First entry actually read log (not including skipped)
    uint64_t first_read_index{0};
    /// Last entry read from log (last entry in log)
    /// When we don't skip anything last_read_index - first_read_index = total_entries_read_from_log.
    /// But when some entries from the start of log can be skipped because they are not required.
    uint64_t last_read_index{0};

    /// last offset we were able to read from log
    off_t last_position;

    /// Whether the changelog file was written using compression
    bool compressed_log;
    bool error;
};

ChangelogRecord readChangelogRecord(ReadBuffer & read_buf, const std::string & filepath)
{
    /// Read checksum
    Checksum record_checksum = 0;
    readIntBinary(record_checksum, read_buf);

    /// Read header
    ChangelogRecord record;
    readIntBinary(record.header.version, read_buf);
    readIntBinary(record.header.index, read_buf);
    readIntBinary(record.header.term, read_buf);
    readIntBinary(record.header.value_type, read_buf);
    readIntBinary(record.header.blob_size, read_buf);

    if (record.header.version > CURRENT_CHANGELOG_VERSION)
        throw Exception(
            ErrorCodes::UNKNOWN_FORMAT_VERSION, "Unsupported changelog version {} on path {}", static_cast<uint8_t>(record.header.version), filepath);

    /// Read data
    if (record.header.blob_size != 0)
    {
        auto buffer = nuraft::buffer::alloc(record.header.blob_size);
        auto * buffer_begin = reinterpret_cast<char *>(buffer->data_begin());
        read_buf.readStrict(buffer_begin, record.header.blob_size);
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

    return record;
}

LogEntryPtr logEntryFromRecord(const ChangelogRecord & record)
{
    return nuraft::cs_new<nuraft::log_entry>(record.header.term, record.blob, static_cast<nuraft::log_val_type>(record.header.value_type));
}

size_t logEntrySize(const LogEntryPtr & log_entry)
{
    return log_entry->get_buf().size();
}

}

class ChangelogReader
{
public:
    explicit ChangelogReader(ChangelogFileDescriptionPtr changelog_description_) : changelog_description(std::move(changelog_description_))
    {
        compression_method = chooseCompressionMethod(changelog_description->path, "");
        auto read_buffer_from_file = changelog_description->disk->readFile(changelog_description->path, getReadSettings());
        read_buf = wrapReadBufferWithCompressionMethod(std::move(read_buffer_from_file), compression_method);
    }

    /// start_log_index -- all entries with index < start_log_index will be skipped, but accounted into total_entries_read_from_log
    ChangelogReadResult readChangelog(LogEntryStorage & entry_storage, uint64_t start_log_index, LoggerPtr log)
    {
        ChangelogReadResult result{};
        result.compressed_log = compression_method != CompressionMethod::None;
        const auto & filepath = changelog_description->path;
        try
        {
            while (!read_buf->eof())
            {
                result.last_position = read_buf->count();

                auto record = readChangelogRecord(*read_buf, filepath);

                /// Check for duplicated changelog ids
                if (entry_storage.contains(record.header.index))
                    entry_storage.cleanAfter(record.header.index - 1);

                result.total_entries_read_from_log += 1;
                result.total_bytes_read_from_log = read_buf->count();

                /// Read but skip this entry because our state is already more fresh
                if (record.header.index < start_log_index)
                    continue;

                /// Create log entry for read data
                auto log_entry = logEntryFromRecord(record);
                if (result.first_read_index == 0)
                    result.first_read_index = record.header.index;

                /// Put it into in memory structure
                entry_storage.addEntryWithLocation(
                    record.header.index,
                    log_entry,
                    LogLocation{
                        .file_description = changelog_description,
                        .position = static_cast<size_t>(result.last_position),
                        .entry_size = record.header.blob_size,
                        .size_in_file = read_buf->count() - result.last_position});
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
    ChangelogFileDescriptionPtr changelog_description;
    CompressionMethod compression_method;
    std::unique_ptr<ReadBuffer> read_buf;
};

namespace
{

/// One per validated record, in physical file order. Produced by readChangelogFile and
/// consumed by the stitch (replayStartupMetadata / materializeEntryStorage).
struct ChangelogEntryMetadata
{
    uint64_t index = 0;
    uint64_t term = 0;
    int32_t value_type = 0;
    size_t position = 0;
    size_t size_in_file = 0;
    size_t blob_size = 0;

    /// Set only when the record must survive by value: nuraft::conf records >= start (feeds
    /// latest_config), or every record >= start in unlimited-cache mode (feeds the cache install).
    LogEntryPtr retained_entry;
};

struct ChangelogFileStartupReadResult
{
    ChangelogFileDescriptionPtr file_description;
    ChangelogReadResult read_result{};
    std::vector<ChangelogEntryMetadata> entries; /// records with index >= start_log_index, physical order
    std::exception_ptr fatal_exception;
};

/// Per-file metadata-only startup reader. Mirrors ChangelogReader::readChangelog's parse loop but
/// mutates no LogEntryStorage state; materializes entry values only where the stitch needs them by value.
ChangelogFileStartupReadResult readChangelogFile(
    const ChangelogFileDescriptionPtr & file_description,
    uint64_t start_log_index,
    const ReadSettings & read_settings,
    bool unlimited_cache_mode,
    LoggerPtr log)
{
    ChangelogFileStartupReadResult result;
    result.file_description = file_description;
    result.read_result.compressed_log = false; /// parallel path never runs when a file is compressed
    const auto & filepath = file_description->path;

    std::unique_ptr<ReadBuffer> read_buf;
    try
    {
        read_buf = file_description->disk->readFile(filepath, read_settings);
    }
    catch (...)
    {
        result.fatal_exception = std::current_exception();
        return result;
    }

    try
    {
        while (!read_buf->eof())
        {
            const size_t last_position = read_buf->count();
            result.read_result.last_position = static_cast<off_t>(last_position);

            auto record = readChangelogRecord(*read_buf, filepath);

            result.read_result.total_entries_read_from_log += 1;
            result.read_result.total_bytes_read_from_log = read_buf->count();

            if (record.header.index >= start_log_index)
            {
                ChangelogEntryMetadata meta;
                meta.index = record.header.index;
                meta.term = record.header.term;
                meta.value_type = record.header.value_type;
                meta.position = last_position;
                meta.size_in_file = read_buf->count() - last_position;
                meta.blob_size = record.header.blob_size;

                if (result.read_result.first_read_index == 0)
                    result.read_result.first_read_index = record.header.index;
                result.read_result.last_read_index = record.header.index;

                if (record.header.value_type == nuraft::conf || unlimited_cache_mode)
                    meta.retained_entry = logEntryFromRecord(record);

                result.entries.push_back(std::move(meta));
            }

            if (result.read_result.total_entries_read_from_log % 50000 == 0)
                LOG_TRACE(log, "Reading changelog from path {}, entries {}", filepath, result.read_result.total_entries_read_from_log);
        }
    }
    catch (const Exception & ex)
    {
        if (ex.code() == ErrorCodes::UNKNOWN_FORMAT_VERSION)
            result.fatal_exception = std::current_exception();
        else
        {
            result.read_result.error = true;
            LOG_WARNING(log, "Cannot completely read changelog on path {}, error: {}", filepath, ex.message());
        }
    }
    catch (...)
    {
        result.read_result.error = true;
        tryLogCurrentException(log);
    }

    LOG_TRACE(log, "Totally read from changelog {} {} entries", filepath, result.read_result.total_entries_read_from_log);
    return result;
}

/// Shared between the serial and parallel startup readers: validates the very first in-scope
/// changelog file against `start_to_read_from`/`last_commited_log_index`. Throws CORRUPTED_DATA if
/// this file starts more than one index past what should already be committed (data loss); warns
/// (but doesn't fail) if fewer logs than requested are retained on disk.
void checkFirstChangelogFile(
    uint64_t from_log_index, uint64_t to_log_index, uint64_t last_commited_log_index, uint64_t start_to_read_from, LoggerPtr log)
{
    LOG_INFO(
        log, "from log index: {}, to log index: {}, last committed log index: {}", from_log_index, to_log_index, last_commited_log_index);

    if (from_log_index > last_commited_log_index && (from_log_index - last_commited_log_index) > 1)
        throw Exception(
            ErrorCodes::CORRUPTED_DATA,
            "Some records were lost, last committed log index {}, smallest available log index on disk {}. Manual intervention "
            "is necessary for recovery but removing changelogs can lead to data loss.",
            last_commited_log_index,
            from_log_index);

    if (from_log_index > start_to_read_from)
        LOG_WARNING(
            log,
            "Don't have required amount of reserved log records. Need to read from {}, smallest available log index on disk "
            "{}.",
            start_to_read_from,
            from_log_index);
}

/// Cross-file locals of the replay loop plus the outputs materializeEntryStorage consumes.
struct StitchState
{
    /// A contiguous run of physical records within one file's `entries`, in final (post-trim)
    /// index-ascending order. Segments are index-ascending across the vector too. Indices within a
    /// segment are consecutive (`first_index + k`): writers never leave gaps within a file.
    struct Segment
    {
        size_t result_idx = 0;
        size_t first_offset = 0;   /// offset into results[result_idx].entries
        size_t count = 0;
        uint64_t first_index = 0;  /// == results[result_idx].entries[first_offset].index
    };

    struct ConfigOwner
    {
        size_t result_idx = 0;
        size_t entry_offset = 0;
    };

    std::vector<Segment> segments;
    std::map<uint64_t, ConfigOwner> config_owner;

    std::optional<ChangelogReadResult> last_log_read_result;
    uint64_t last_read_index = 0; /// doubles as the new max_log_id
    uint64_t remove_logs_before_index = 0;
    bool last_log_is_not_complete = false;
};

/// Replays the serial control flow over metadata only -- decisions, no LogEntryStorage mutation.
/// Rethrows a file's captured fatal exception at the point the serial reader would have opened
/// that file; a fatal in a file the serial loop never reaches is ignored, matching serial.
StitchState replayStartupMetadata(
    const std::vector<ChangelogFileStartupReadResult> & results,
    uint64_t start_to_read_from,
    uint64_t last_commited_log_index,
    LoggerPtr log)
{
    StitchState stitch_state;

    auto accumulated_min_index = [&]
    {
        return stitch_state.segments.front().first_index;
    };
    auto accumulated_max_index = [&]
    {
        const auto & segment = stitch_state.segments.back();
        return segment.first_index + segment.count - 1;
    };

    /// Keep only entries with index <= cutoff_index (mirrors LogEntryStorage::cleanAfter(cutoff_index)).
    auto trim_after = [&](uint64_t cutoff_index)
    {
        while (!stitch_state.segments.empty())
        {
            auto & segment = stitch_state.segments.back();
            const uint64_t segment_last_index = segment.first_index + segment.count - 1;
            if (segment_last_index <= cutoff_index)
                break;
            if (segment.first_index > cutoff_index)
            {
                stitch_state.segments.pop_back();
                continue;
            }
            segment.count = cutoff_index - segment.first_index + 1;
            break;
        }
        stitch_state.config_owner.erase(stitch_state.config_owner.upper_bound(cutoff_index), stitch_state.config_owner.end());

    };

    for (size_t i = 0; i < results.size(); ++i)
    {
        const auto & result = results[i];
        const auto & file_description = result.file_description;

        if (!stitch_state.last_log_read_result)
        {
            checkFirstChangelogFile(
                file_description->from_log_index, file_description->to_log_index, last_commited_log_index, start_to_read_from, log);
        }
        else if (file_description->from_log_index > stitch_state.last_read_index
                 && (file_description->from_log_index - stitch_state.last_read_index) > 1)
        {
            if (file_description->from_log_index <= last_commited_log_index)
            {
                LOG_INFO(
                    log,
                    "Found gap in changelogs from {} to {}, but these entries are already present in the existing "
                    "snapshot (last committed: {}). Removing logs before index {}.",
                    stitch_state.last_read_index,
                    file_description->from_log_index,
                    last_commited_log_index,
                    file_description->from_log_index);

                stitch_state.remove_logs_before_index = file_description->from_log_index;

                /// Reset retained metadata: everything before the gap.
                stitch_state.segments.clear();
                stitch_state.config_owner.clear();
                stitch_state.last_log_read_result.reset();
            }
            else
            {
                if (!stitch_state.last_log_read_result->error)
                    throw Exception(
                        ErrorCodes::CORRUPTED_DATA,
                        "Some records were lost, last found log index {}, while the next log index on disk is {}. Manual intervention "
                        "is necessary for recovery but removing changelogs can lead to data loss.",
                        stitch_state.last_read_index,
                        file_description->from_log_index);
                break;
            }
        }

        /// Point where serial would have opened this file -- surface a captured fatal here.
        if (result.fatal_exception)
            std::rethrow_exception(result.fatal_exception);

        for (size_t offset = 0; offset < result.entries.size(); ++offset)
        {
            const auto & entry = result.entries[offset];

            if (!stitch_state.segments.empty() && entry.index >= accumulated_min_index() && entry.index <= accumulated_max_index())
                trim_after(entry.index - 1);

            chassert(entry.index >= start_to_read_from); /// only such records are retained

            /// True when this record is physically the very next one (same file, no gap) after the
            /// last kept segment, so it can extend that segment instead of starting a new one.
            auto * last_segment = stitch_state.segments.empty() ? nullptr : &stitch_state.segments.back();
            if (last_segment && last_segment->result_idx == i && last_segment->first_offset + last_segment->count == offset)
            {
                chassert(entry.index == last_segment->first_index + last_segment->count);
                ++last_segment->count;
            }
            else
                stitch_state.segments.push_back(
                    StitchState::Segment{.result_idx = i, .first_offset = offset, .count = 1, .first_index = entry.index});

            if (entry.value_type == nuraft::conf)
                stitch_state.config_owner[entry.index] = StitchState::ConfigOwner{.result_idx = i, .entry_offset = offset};
        }

        if (result.read_result.first_read_index == 0)
        {
            LOG_TRACE(log, "Changelog is empty or contains only logs before {}", start_to_read_from);
            continue;
        }

        auto & last_log_read_result = stitch_state.last_log_read_result;
        last_log_read_result = result.read_result;
        last_log_read_result->log_start_index = file_description->from_log_index;

        if (last_log_read_result->last_read_index != 0)
            stitch_state.last_read_index = last_log_read_result->last_read_index;

        const uint64_t log_count = file_description->expectedEntriesCountInLog();
        stitch_state.last_log_is_not_complete
            = stitch_state.last_log_read_result->error || stitch_state.last_log_read_result->total_entries_read_from_log < log_count;
    }

    return stitch_state;
}

/// Walks the final segment list and builds the LogEntryStorage by-products (locations, valid_runs,
/// term info, config index, latest_config, and -- in unlimited-cache mode -- the cache). Mutates
/// `results`, freeing each file's `entries` once the walk moves past it.
void materializeEntryStorage(
    LogEntryStorage & entry_storage,
    std::vector<ChangelogFileStartupReadResult> & results,
    const StitchState & stitch_state,
    bool unlimited_cache_mode)
{
    /// Install the config first: it points into some file's `entries`, which freeing below could dangle.
    if (!stitch_state.config_owner.empty())
    {
        const auto & [index, owner] = *stitch_state.config_owner.rbegin();
        auto & meta = results[owner.result_idx].entries[owner.entry_offset];
        chassert(meta.retained_entry != nullptr);
        entry_storage.setLatestConfig(index, meta.retained_entry);
    }

    size_t total_locations = 0;
    for (const auto & segment : stitch_state.segments)
        total_locations += segment.count;
    entry_storage.reserveLocations(total_locations);

    std::optional<size_t> previous_result_idx;
    for (const auto & segment : stitch_state.segments)
    {
        /// result_idx is non-decreasing, so once we move past a file its entries are no longer needed; reset them to reduce memory usage.
        if (previous_result_idx && *previous_result_idx != segment.result_idx)
            results[*previous_result_idx].entries = {};
        previous_result_idx = segment.result_idx;

        auto & result = results[segment.result_idx];
        for (size_t k = 0; k < segment.count; ++k)
        {
            auto & meta = result.entries[segment.first_offset + k];

            LogLocation location{
                .file_description = result.file_description,
                .position = meta.position,
                .entry_size = meta.blob_size,
                .size_in_file = meta.size_in_file};
            entry_storage.addLocation(meta.index, meta.term, meta.value_type, /*log_entry=*/nullptr, location);

            if (unlimited_cache_mode)
            {
                chassert(meta.retained_entry != nullptr);
                entry_storage.addEntryToLatestCache(meta.index, meta.retained_entry);
            }
        }
    }
}

}

void validateReadAheadSettings(const ReadAheadSettings & settings)
{
    if (settings.pool_threads != 0 && settings.pool_threads < settings.max_peer_readers)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "log_readahead_pool_threads must be 0 (auto) or >= log_readahead_max_peer_readers, got "
            "log_readahead_pool_threads={} and log_readahead_max_peer_readers={}",
            settings.pool_threads,
            settings.max_peer_readers);
}

LogEntryStorage::LogEntryStorage(const LogFileSettings & log_settings, ReadAheadSettings readahead_settings_, KeeperContextPtr keeper_context_)
    : latest_logs_cache(log_settings.latest_logs_cache_size_threshold)
    , keeper_context(std::move(keeper_context_))
    , log(getLogger("Changelog"))
    , readahead_settings(std::move(readahead_settings_))
{
}

LogEntryStorage::~LogEntryStorage()
{
    shutdown();
}

LogEntryStorage::InMemoryCache::InMemoryCache(size_t size_threshold_)
    : size_threshold(size_threshold_)
{}

void LogEntryStorage::InMemoryCache::updateStatsWithNewEntry(uint64_t index, size_t size)
{
    cache_size += size;

    if (cache.size() == 1)
    {
        min_index_in_cache = index;
        max_index_in_cache = index;
    }
    else
    {
        chassert(index > max_index_in_cache);
        max_index_in_cache = index;
    }
}

void LogEntryStorage::InMemoryCache::addEntry(uint64_t index, size_t size, LogEntryPtr log_entry)
{
    auto [_, inserted] = cache.emplace(index, std::move(log_entry));
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert log with index {} which is already present in cache", index);

    updateStatsWithNewEntry(index, size);
}

void LogEntryStorage::InMemoryCache::popOldestEntry()
{
    auto it = cache.find(min_index_in_cache);
    if (it == cache.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Couldn't find the oldest entry of index {} in logs cache", min_index_in_cache);
    cache_size -= logEntrySize(it->second);
    cache.erase(it);
    ++min_index_in_cache;
}

bool LogEntryStorage::InMemoryCache::containsEntry(uint64_t index) const
{
    return !cache.empty() && index >= min_index_in_cache && index <= max_index_in_cache;
}

LogEntryPtr LogEntryStorage::InMemoryCache::getEntry(uint64_t index) const
{
    if (!containsEntry(index))
        return nullptr;

    auto it = cache.find(index);
    if (it == cache.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index {} missing from cache while it should be present", index);

    return it->second;
}

void LogEntryStorage::InMemoryCache::cleanUpTo(uint64_t index)
{
    if (empty() || index <= min_index_in_cache)
        return;

    if (index > max_index_in_cache)
    {
        cache.clear();
        cache_size = 0;
        return;
    }

    for (size_t i = min_index_in_cache; i < index; ++i)
    {
        auto it = cache.find(i);
        if (it == cache.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Log entry with index {} unexpectedly missing from cache", i);

        cache_size -= logEntrySize(it->second);
        cache.erase(it);
    }
    min_index_in_cache = index;
}

void LogEntryStorage::InMemoryCache::cleanAfter(uint64_t index)
{
    if (empty() || index >= max_index_in_cache)
        return;

    if (index < min_index_in_cache)
    {
        cache.clear();
        cache_size = 0;
        return;
    }

    for (size_t i = index + 1; i <= max_index_in_cache; ++i)
    {
        auto it = cache.find(i);
        if (it == cache.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Log entry with index {} unexpectedly missing from cache", i);

        cache_size -= logEntrySize(it->second);
        cache.erase(it);
    }

    max_index_in_cache = index;
}

void LogEntryStorage::InMemoryCache::clear()
{
    cache.clear();
    cache_size = 0;
    min_index_in_cache = 0;
    max_index_in_cache = 0;
}

bool LogEntryStorage::InMemoryCache::hasUnlimitedSpace() const
{
    return size_threshold == 0;
}

bool LogEntryStorage::InMemoryCache::empty() const
{
    return cache.empty();
}

size_t LogEntryStorage::InMemoryCache::numberOfEntries() const
{
    return cache.size();
}

bool LogEntryStorage::InMemoryCache::hasSpaceAvailable(size_t log_entry_size) const
{
    if (hasUnlimitedSpace() || empty())
        return true;

    return cache_size + log_entry_size <= size_threshold;
}

void LogEntryStorage::addEntry(uint64_t index, const LogEntryPtr & log_entry)
{
    /// we update the cache for added entries on refreshCache call
    latest_logs_cache.addEntry(index, logEntrySize(log_entry), log_entry);

    if (log_entry->get_val_type() == nuraft::conf)
    {
        latest_config = log_entry;
        latest_config_index = index;
        logs_with_config_changes.insert(index);
    }

    updateTermInfoWithNewEntry(index, log_entry->get_term());
}

void LogEntryStorage::updateTermInfoWithNewEntry(uint64_t index, uint64_t term)
{
    if (!log_term_infos.empty() && log_term_infos.back().term == term)
        return;

    log_term_infos.push_back(LogTermInfo{.term = term, .first_index = index});
}

void LogEntryStorage::addEntryWithLocation(uint64_t index, const LogEntryPtr & log_entry, LogLocation log_location)
{
    addEntryToLatestCache(index, log_entry);
    addLocation(index, log_entry->get_term(), log_entry->get_val_type(), log_entry, std::move(log_location));
}

void LogEntryStorage::addLocation(uint64_t index, uint64_t term, int32_t value_type, const LogEntryPtr & log_entry, LogLocation log_location)
{
    log_location.file_description->valid_runs.addLocatedRecord(index, log_location.position, log_location.size_in_file);
    logs_location.emplace(index, std::move(log_location));

    if (logs_location.size() == 1)
        min_index_with_location = index;

    max_index_with_location = index;

    if (value_type == nuraft::conf)
    {
        if (log_entry)
        {
            latest_config = log_entry;
            latest_config_index = index;
        }

        logs_with_config_changes.insert(index);
    }

    updateTermInfoWithNewEntry(index, term);
}

void LogEntryStorage::addEntryToLatestCache(uint64_t index, const LogEntryPtr & log_entry)
{
    const auto entry_size = logEntrySize(log_entry);
    while (!latest_logs_cache.hasSpaceAvailable(entry_size))
        latest_logs_cache.popOldestEntry();
    latest_logs_cache.addEntry(index, entry_size, log_entry);
}

void LogEntryStorage::reserveLocations(size_t count)
{
    logs_location.reserve(count);
}

void LogEntryStorage::setLatestConfig(uint64_t index, LogEntryPtr log_entry)
{
    latest_config = std::move(log_entry);
    latest_config_index = index;
}

void LogEntryStorage::cleanUpTo(uint64_t index)
{
    /// No valid-run bookkeeping here: compaction only drops a file's head, leaving runs that
    /// over-claim harmlessly -- planners clip every cursor to the plan's start (>= retained_start).
    latest_logs_cache.cleanUpTo(index);

    if (!logs_location.empty() && index > min_index_with_location)
    {
        if (index > max_index_with_location)
        {
            logs_location.clear();
        }
        else
        {
            for (size_t i = min_index_with_location; i < index; ++i)
            {
                auto it = logs_location.find(i);
                if (it == logs_location.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Log entry with index {} unexpectedly missing from logs location", i);

                logs_location.erase(it);
            }

            min_index_with_location = index;

        }
    }

    {
        std::lock_guard lock(logs_location_mutex);
        if (!unapplied_indices_with_log_locations.empty())
        {
            auto last = std::ranges::lower_bound(
                unapplied_indices_with_log_locations,
                index,
                std::ranges::less{},
                [](const auto & index_with_location) { return index_with_location.first; });

            unapplied_indices_with_log_locations.erase(unapplied_indices_with_log_locations.begin(), last);
        }
    }

    std::erase_if(logs_with_config_changes, [&](const auto conf_index) { return conf_index < index; });
    if (auto it = std::max_element(logs_with_config_changes.begin(), logs_with_config_changes.end()); it != logs_with_config_changes.end())
    {
        latest_config_index = *it;
        latest_config = getEntry(latest_config_index);
    }
    else
        latest_config = nullptr;

    if (first_log_index < index)
        first_log_entry = nullptr;

    /// remove all the term infos we don't need (all terms that start before index)
    uint64_t last_removed_term = 0;
    while (!log_term_infos.empty() && log_term_infos.front().first_index < index)
    {
        last_removed_term = log_term_infos.front().term;
        log_term_infos.pop_front();
    }

    /// the last removed term info could contain terms for some indices we didn't cleanup
    /// so we add the last removed term info back but with new first index
    if (last_removed_term != 0 && (log_term_infos.empty() || log_term_infos.front().first_index > index))
        log_term_infos.push_front(LogTermInfo{.term = last_removed_term, .first_index = index});
}

void LogEntryStorage::cleanAfter(uint64_t index)
{
    /// Bumped before any mutation below; validators compare against a fresh load to detect a plan
    /// that straddled this truncation. cleanUpTo (compaction) doesn't bump it -- fenced by removed_from_disk.
    ++truncation_epoch;

    latest_logs_cache.cleanAfter(index);

    if (!logs_location.empty() && index < max_index_with_location)
    {
        if (index < min_index_with_location)
        {
            /// Everything located lies above index: clear runs for every distinct file first.
            for (const auto & [located_index, loc] : logs_location)
                loc.file_description->valid_runs.clear();

            logs_location.clear();
        }
        else
        {
            /// Truncate the boundary file's runs before the erase loop below removes anything, so a
            /// concurrent planner call never observes stale runs past the truncation point.
            const auto & boundary_loc = logs_location.at(index);
            boundary_loc.file_description->valid_runs.truncateAt(index + 1, boundary_loc.position + boundary_loc.size_in_file);

            for (size_t i = index + 1; i <= max_index_with_location; ++i)
            {
                auto it = logs_location.find(i);
                if (it == logs_location.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Log entry with index {} unexpectedly missing from logs location", i);

                /// Clear the whole file's runs unless it's the boundary file (already truncated above).
                if (it->second.file_description != boundary_loc.file_description)
                    it->second.file_description->valid_runs.clear();

                logs_location.erase(it);
            }

            max_index_with_location = index;
        }
    }

    {
        std::lock_guard lock(logs_location_mutex);
        if (!unapplied_indices_with_log_locations.empty())
        {
            auto first = std::ranges::upper_bound(
                unapplied_indices_with_log_locations,
                index,
                std::ranges::less{},
                [](const auto & index_with_location) { return index_with_location.first; });

            unapplied_indices_with_log_locations.erase(first, unapplied_indices_with_log_locations.end());
        }
    }

    if (empty() || first_log_index > index)
    {
        /// if we don't store any logs or if the first log index changed, reset first log cache
        first_log_entry = nullptr;
    }

    std::erase_if(logs_with_config_changes, [&](const auto conf_index) { return conf_index > index; });
    if (auto it = std::max_element(logs_with_config_changes.begin(), logs_with_config_changes.end()); it != logs_with_config_changes.end())
    {
        latest_config_index = *it;
        latest_config = getEntry(latest_config_index);
    }
    else
        latest_config = nullptr;

    /// remove all the term infos we don't need (all terms that start after index)
    while (!log_term_infos.empty() && log_term_infos.back().first_index > index)
        log_term_infos.pop_back();

    /// Entries > index were rewritten; buffered decoded content is now stale.
    closeAllReaders();
}

bool LogEntryStorage::contains(uint64_t index) const
{
    return logs_location.contains(index) || latest_logs_cache.containsEntry(index);
}

namespace
{

/// logs_location positions are decompressed-stream offsets; seeking the raw compressed file would
/// decode garbage.
void assertNotCompressed(const ChangelogFileDescriptionPtr & file_description)
{
    if (file_description->is_compressed)
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "Reading log entries from disk is not supported for compressed changelog files ({}): recorded positions are "
            "decompressed offsets and the raw file cannot be seeked",
            file_description->getPathSafe());
}

}

LogEntryPtr LogEntryStorage::getEntryFromMemory(uint64_t index) const
{
    if (latest_config != nullptr && index == latest_config_index)
        return latest_config;

    if (first_log_entry != nullptr && index == first_log_index)
        return first_log_entry;

    if (auto entry = latest_logs_cache.getEntry(index))
    {
        ProfileEvents::increment(ProfileEvents::KeeperLogsEntryReadFromLatestCache);
        return entry;
    }

    return nullptr;
}

LogEntryPtr LogEntryStorage::getEntry(uint64_t index) const
{
    if (auto entry_from_memory = getEntryFromMemory(index))
        return entry_from_memory;

    LogEntryPtr entry = nullptr;

    if (auto it = logs_location.find(index); it != logs_location.end())
    {
        assertNotCompressed(it->second.file_description);
        it->second.file_description->withReadLock(
            [&]
            {
                const auto & [changelog_description, position, entry_size, size_in_file] = it->second;
                auto file = changelog_description->disk->readFile(changelog_description->path, getReadSettings());
                file->seek(position, SEEK_SET);
                LOG_TRACE(
                    log,
                    "Reading log entry at index {} from path {}, position {}, size {}",
                    index,
                    changelog_description->path,
                    position,
                    entry_size);

                auto record = readChangelogRecord(*file, changelog_description->path);
                entry = logEntryFromRecord(record);
            });

        /// if we fetched the first log entry, we will cache it because it's often accessed
        if (first_log_entry == nullptr && index == getFirstIndex())
        {
            first_log_index = index;
            first_log_entry = entry;
        }

        ProfileEvents::increment(ProfileEvents::KeeperLogsEntryReadFromFile);
    }

    return entry;
}

void LogEntryStorage::clear()
{
    latest_logs_cache.clear();

    for (const auto & [index, loc] : logs_location)
        loc.file_description->valid_runs.clear();

    logs_location.clear();
    max_index_with_location = 0;
    min_index_with_location = 0;

    unapplied_indices_with_log_locations.clear();

    logs_with_config_changes.clear();
    latest_config = nullptr;
    latest_config_index = 0;

    first_log_entry = nullptr;
    first_log_index = 0;

    log_term_infos.clear();
}

LogEntryPtr LogEntryStorage::getLatestConfigChange() const
{
    return latest_config;
}

uint64_t LogEntryStorage::termAt(uint64_t index) const
{
    if (log_term_infos.empty())
        return 0;

    auto it = std::ranges::upper_bound(log_term_infos, index, {}, &LogTermInfo::first_index);

    if (it == log_term_infos.begin())
        return 0;

    --it;
    return it->term;
}

void LogEntryStorage::addLogLocations(std::vector<std::pair<uint64_t, LogLocation>> && indices_with_log_locations)
{
    /// if we have unlimited space in latest logs cache we don't need log location
    if (latest_logs_cache.hasUnlimitedSpace())
        return;

    if (indices_with_log_locations.empty())
        return;

    std::lock_guard lock(logs_location_mutex);
    unapplied_indices_with_log_locations.insert(
        unapplied_indices_with_log_locations.end(),
        std::make_move_iterator(indices_with_log_locations.begin()),
        std::make_move_iterator(indices_with_log_locations.end()));
}

void LogEntryStorage::refreshCache()
{
    /// The only scan opportunity for deployments where serveReadAhead never runs (single-node,
    /// write-only, or peer read-ahead disabled).
    evictIdleReadersIfNeeded();

    /// if we have unlimited space in latest logs cache we don't need log location
    if (latest_logs_cache.hasUnlimitedSpace())
        return;

    std::vector<IndexWithLogLocation> new_unapplied_indices_with_log_locations;
    {
        std::lock_guard lock(logs_location_mutex);
        new_unapplied_indices_with_log_locations.swap(unapplied_indices_with_log_locations);
    }

    for (auto & [index, log_location] : new_unapplied_indices_with_log_locations)
    {
        if (logs_location.empty())
            min_index_with_location = index;

        log_location.file_description->valid_runs.addLocatedRecord(index, log_location.position, log_location.size_in_file);
        logs_location.emplace(index, std::move(log_location));
        max_index_with_location = index;
    }

    if (logs_location.empty())
        return;

    const auto latest_log_cache_over_size_threshold = [&]
    {
        return latest_logs_cache.cache_size > latest_logs_cache.size_threshold;
    };

    while (latest_logs_cache.numberOfEntries() > 1 && latest_logs_cache.min_index_in_cache <= max_index_with_location
           && latest_log_cache_over_size_threshold())
        latest_logs_cache.popOldestEntry();
}

LogReadPlan LogEntryStorage::getReadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes, uint64_t retained_start) const
{
    LogReadPlan plan;
    plan.start_index = start;
    plan.epoch = truncation_epoch.load();
    /// max_size_bytes modes: -1 = backpressure (return empty), 0 = unlimited, >0 = byte budget.
    if (max_size_bytes == -1)
        return plan;

    plan.requested_entry_count = end - start;
    plan.items.reserve(plan.requested_entry_count);

    int64_t total_size = 0;
    /// Returns true when the byte budget is reached; as a side-effect accounts entry_size when not reached.
    const auto try_account_entry = [&](int64_t entry_size)
    {
        if (max_size_bytes == 0)
            return false;
        bool limit_reached = total_size > 0 && total_size + entry_size > max_size_bytes;
        if (!limit_reached)
            total_size += entry_size;
        return limit_reached;
    };

    std::optional<LogReadPlan::FileSpan> run;
    size_t next_position = 0;

    const auto set_new_file = [&](uint64_t idx, const LogLocation & loc)
    {
        run.emplace(LogReadPlan::FileSpan{
            .file_description = loc.file_description,
            .position = loc.position,
            .first_index = idx,
            .count = 1,
        });
        next_position = loc.position + loc.size_in_file;
    };

    const auto flush_run = [&]
    {
        if (run)
        {
            plan.items.emplace_back(std::move(*run));
            run.reset();
        }
    };

    /// The cache is a contiguous suffix of the log (evicted entries always have a location), so below
    /// cache_start no cache lookup is needed.
    const uint64_t cache_start = latest_logs_cache.empty() ? end : latest_logs_cache.min_index_in_cache;

    for (size_t i = start; i < end; ++i)
    {
        if (i >= cache_start)
        {
            if (LogEntryPtr cached = latest_logs_cache.getEntry(i))
            {
                flush_run();
                if (try_account_entry(static_cast<int64_t>(cached->get_buf().size())))
                    break;
                plan.items.emplace_back(std::move(cached));
                continue;
            }
        }

        auto it = logs_location.find(i);
        if (it == logs_location.end())
        {
            if (i < retained_start)
            {
                /// Entry is below retained start - compacted away
                plan.logs_compacted = true;
                return plan;
            }
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Location of log entry with index {} is missing", i);
        }

        const auto & loc = it->second;
        assertNotCompressed(loc.file_description);
        if (try_account_entry(static_cast<int64_t>(loc.size_in_file)))
            break;

        if (!run)
        {
            set_new_file(i, loc);
        }
        else if (run->file_description == loc.file_description && next_position == loc.position)
        {
            ++run->count;
            next_position += loc.size_in_file;
        }
        else
        {
            /// Same file, physical gap (rewrite hole): a new item starts here. appendRunCursors clips
            /// to run boundaries, so read-ahead never crosses into this item's (different) run.
            flush_run();
            set_new_file(i, loc);
        }
    }

    flush_run();
    return plan;
}

namespace
{

/// Decode one record from buf, validate the index, and emit the entry.
/// IO errors propagate as exceptions (caller decides the policy).
/// Returns false on index mismatch without throwing or logging.
/// Caller increments the appropriate ProfileEvent so a decode is never double-counted.
bool decodeOneRecord(ReadBuffer & buf, const String & path, uint64_t expected_index, LogEntryPtr & out)
{
    auto record = readChangelogRecord(buf, path);
    if (record.header.index != expected_index)
        return false;
    out = logEntryFromRecord(record);
    return true;
}

}

LogEntriesPtr LogEntryStorage::executeReadPlan(const LogReadPlan & plan, uint64_t read_deadline_ms) const
{
    if (plan.logs_compacted)
        return nullptr;

    auto ret = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();
    ret->reserve(plan.requested_entry_count);

    Stopwatch watch;
    const auto deadline_hit = [&]() { return read_deadline_ms != 0 && watch.elapsedMilliseconds() > read_deadline_ms; };

    for (const auto & item : plan.items)
    {
        if (const auto * e = std::get_if<LogEntryPtr>(&item))
        {
            ret->push_back(*e);
            continue;
        }

        const auto & file_run = std::get<LogReadPlan::FileSpan>(item);
        bool compacted = false;
        bool deadline = false;

        file_run.file_description->withReadLock(
            [&]
            {
                if (file_run.file_description->removed_from_disk)
                {
                    compacted = true;
                    return;
                }

                const auto & path = file_run.file_description->path;
                LOG_TRACE(log, "Reading from path {} {} entries", path, file_run.count);

                auto raw_file = file_run.file_description->disk->readFile(path, getReadSettings());
                raw_file->seek(file_run.position, SEEK_SET);

                uint64_t expected = file_run.first_index;

                for (size_t k = 0; k < file_run.count; ++k)
                {
                    if (deadline_hit())
                    {
                        deadline = true;
                        return;
                    }
                    LogEntryPtr entry;
                    if (!decodeOneRecord(*raw_file, path, expected, entry))
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR, "Index mismatch while reading from {}, expected index {}", path, expected);
                    ret->push_back(std::move(entry));
                    ProfileEvents::increment(ProfileEvents::KeeperLogsEntryReadFromFile);
                    ++expected;
                }
            });

        if (compacted)
            return nullptr;
        if (deadline)
            return ret; /// return the prefix decoded so far
    }

    /// A concurrent writeAt may have truncated and rewritten entries since the plan's epoch snapshot,
    /// so positions above could describe a stale layout even without a decode error -- discard rather
    /// than serve possibly-stale content.
    if (plan.epoch != truncation_epoch.load())
    {
        ProfileEvents::increment(ProfileEvents::KeeperLogsReadAheadPlanEpochMismatches);
        return nullptr;
    }

    return ret;
}

/// ===== Decoded changelog read-ahead (shared by peer catch-up and commit consumers) =====
/// One ReadAheadReader instance backs either a follower's catch-up stream or, identified by
/// LogEntryStorage::COMMIT_READER_ID, the commit reader. Each is a decoded-entry buffer worked by
/// two actors that communicate via ReadAheadReader under fill_serve_mutex + fill_serve_cv:
///
///   - the SERVE path (consumer, on the request/commit thread): serveReadAhead / drainReader,
///   - the FILL task (producer, on the read-ahead thread pool): fillTask / fillFromCursor; one per
///     reader, scheduled once at creation (makeReaderLocked) on the shared readahead_pool.
///
/// A read request flows as:
///   1. Plan under changelog_lock (getReadAheadPlan / getCommitReadPlan): resolve which entries are
///      in memory vs. on disk, and build read-ahead cursors extending past the requested range.
///   2. Install (installPlanLocked): push those cursors into `pending_cursors` and wake the fill task,
///      kicking off prefetch of the entries just past this request.
///   3. Drain (drainReader): pop the contiguous available prefix from the deque; if the next needed
///      entry isn't decoded yet, wait up to serve_wait_timeout_ms for the fill, then fall back to a
///      direct disk read for the remaining tail.
///
/// Meanwhile the fill task decodes cursors from disk and pushes entries onto the deque — including
/// entries beyond the current request — so subsequent (sequential) requests are served from memory
/// instead of disk. Fill parks at the byte budget and resumes when the serve side consumes.
///
/// ReaderState is the single source of truth for lifecycle (Running / Error / Compacted / Closed);
/// serve and fill both branch on it, and a generation counter (bumped on rewind/close) discards any
/// fill work decoded against a superseded position. See ReaderState.

enum class ReaderState : uint8_t
{
    Running,    /// active: fill is decoding, or blocked on fill_serve_cv with nothing to do ("parked",
                /// e.g. window full or EOF with nothing queued yet); serve consuming
    Compacted,  /// underlying changelog file removed/compacted; serve returns nullptr
    Error,      /// fill hit a decode/IO/non-contiguous error; serve falls back to direct read
    Closed,     /// externally retired (eviction, reaping, rewind-recreate, shutdown)
};

struct ReadAheadReader
{
    std::mutex fill_serve_mutex;
    std::condition_variable fill_serve_cv; /// wakes serve when fill appends; wakes fill when serve consumes
    std::deque<LogEntryPtr> decoded_entries; /// decoded log entries, contiguous from decoded_front_index
    uint64_t decoded_front_index = 0; /// log index of decoded_entries.front(); also "next expected" when deque is empty
    size_t decoded_bytes = 0; /// sum of bytes across decoded_entries
    size_t window_budget_bytes = 0; /// fill parks when decoded_bytes reaches this
    uint64_t generation = 0; /// bumped on rewind; fill checks this per chunk
    ReaderState state = ReaderState::Running;

    std::deque<LogReadPlan::FileSpan> pending_cursors; /// new cursors passed from serve under fill_serve_mutex

    /// Decode stream kept open across fill parks to avoid re-seeking.
    /// Touched only by the fill task.
    std::unique_ptr<ReadBufferFromFileBase> held_buf;
    DiskPtr opened_disk;
    std::string opened_path;
    std::optional<LogReadPlan::FileSpan> resume_cursor;

    /// Guarded by LogEntryStorage::readers_mutex, not fill_serve_mutex.
    std::chrono::steady_clock::time_point last_access;

    /// === Lifecycle helpers (fill_serve_mutex held by caller unless noted) ===
    void setReaderStateLocked(ReaderState s) TSA_REQUIRES(fill_serve_mutex);
    void closeReaderLocked() TSA_REQUIRES(fill_serve_mutex);
    void markCompacted(); ///< self-locking (acquires fill_serve_mutex)

    /// === Decoded deque helpers (fill_serve_mutex held by caller) ===
    bool discardBeforeLocked(uint64_t index) TSA_REQUIRES(fill_serve_mutex);
    LogEntryPtr popFrontLocked() TSA_REQUIRES(fill_serve_mutex);
    void resetToIndexLocked(uint64_t index) TSA_REQUIRES(fill_serve_mutex);
    /// Exclusive upper bound of indices the fill will produce unaided (pending cursors > resume > deque > front).
    uint64_t fillCoverageEndLocked() const TSA_REQUIRES(fill_serve_mutex);

    /// === Park/wake hysteresis (fill_serve_mutex held by caller) ===
    /// Fill parks at the full budget, wakes at half (hysteresis); all pop sites must agree on this.
    size_t lowWaterMarkLocked() const TSA_REQUIRES(fill_serve_mutex) { return window_budget_bytes / 2; }
    /// Whether a pop from bytes_before crossed the low-water mark. Compare against decoded_bytes
    /// *before* the pop(s), not after.
    bool crossedLowWaterLocked(size_t bytes_before) const TSA_REQUIRES(fill_serve_mutex)
    {
        return bytes_before > lowWaterMarkLocked() && decoded_bytes <= lowWaterMarkLocked();
    }

    /// === Fill cursor helpers (fill_serve_mutex held by caller) ===
    void setResumeCursorLocked(const ChangelogFileDescriptionPtr & file_description, size_t position, uint64_t first_index, size_t count)
        TSA_REQUIRES(fill_serve_mutex);
    /// Unlike resetToIndexLocked, also resets held_buf; only ever called from the fill task itself.
    void resetFillCursorLocked() TSA_REQUIRES(fill_serve_mutex);

    /// === Held-buffer helpers (fill task only, no lock required) ===
    void closeHeld();

    /// === Cursor handoff (self-locking) ===
    std::optional<LogReadPlan::FileSpan> takeNextCursor(uint64_t & local_generation, bool & should_exit);
    void waitForCursor(uint64_t local_generation);
};

/// Byte size of a decoded entry for decoded_bytes accounting. Counts only the serialized buffer, so
/// window budgets underestimate resident memory for tiny entries (per-entry object overhead is ignored).
static size_t entryBytes(const LogEntryPtr & entry)
{
    return entry ? entry->get_buf().size() : 0;
}

/// Exclusive upper bound of log indices a fill cursor will produce.
static uint64_t cursorCoverageEnd(const LogReadPlan::FileSpan & cursor)
{
    chassert(cursor.count > 0);
    return cursor.first_index + cursor.count;
}

void ReadAheadReader::setReaderStateLocked(ReaderState s)
{
    state = s;
    fill_serve_cv.notify_all();
}

void ReadAheadReader::closeReaderLocked()
{
    ++generation;
    setReaderStateLocked(ReaderState::Closed);
}

void ReadAheadReader::markCompacted()
{
    std::lock_guard fill_serve_lock(fill_serve_mutex);
    setReaderStateLocked(ReaderState::Compacted);
}

bool ReadAheadReader::discardBeforeLocked(uint64_t index)
{
    bool changed = false;
    while (!decoded_entries.empty() && decoded_front_index < index)
    {
        popFrontLocked();
        changed = true;
    }

    /// Deque drained before index: advance the cursor so serve does not see a gap.
    /// appendChunk skips entries below decoded_front_index, so fill catches up correctly.
    if (decoded_entries.empty() && decoded_front_index < index)
    {
        decoded_front_index = index;
        changed = true;
    }

    return changed;
}

LogEntryPtr ReadAheadReader::popFrontLocked()
{
    auto entry = std::move(decoded_entries.front());
    decoded_bytes -= entryBytes(entry);
    ++decoded_front_index;
    decoded_entries.pop_front();
    return entry;
}

void ReadAheadReader::resetToIndexLocked(uint64_t index)
{
    ++generation;
    decoded_entries.clear();
    pending_cursors.clear();
    decoded_bytes = 0;
    decoded_front_index = index;
    resume_cursor.reset();
}

uint64_t ReadAheadReader::fillCoverageEndLocked() const
{
    /// Cursors are bounded; coverage is purely positional (first_index + count), no file access needed.
    if (!pending_cursors.empty())
        return cursorCoverageEnd(pending_cursors.back());
    if (resume_cursor.has_value())
        return cursorCoverageEnd(*resume_cursor);
    if (!decoded_entries.empty())
        return decoded_front_index + decoded_entries.size();
    return decoded_front_index;
}

void ReadAheadReader::setResumeCursorLocked(
    const ChangelogFileDescriptionPtr & file_description, size_t position, uint64_t first_index, size_t count)
{
    resume_cursor = LogReadPlan::FileSpan{
        .file_description = file_description,
        .position = position,
        .first_index = first_index,
        .count = count,
    };
}

void ReadAheadReader::resetFillCursorLocked()
{
    held_buf.reset();
    resume_cursor.reset();
    /// Wakes a serve waiting on drainReader's cleared-cursor clause, now true.
    fill_serve_cv.notify_all();
}

void ReadAheadReader::closeHeld()
{
    held_buf.reset();
    opened_disk.reset();
    opened_path.clear();
}

std::optional<LogReadPlan::FileSpan> ReadAheadReader::takeNextCursor(uint64_t & local_generation, bool & should_exit)
{
    std::lock_guard fill_serve_lock(fill_serve_mutex);
    if (state != ReaderState::Running)
    {
        should_exit = true;
        return std::nullopt;
    }
    should_exit = false;
    local_generation = generation;
    if (!pending_cursors.empty())
    {
        auto cursor = std::move(pending_cursors.front());
        pending_cursors.pop_front();
        /// Publish into resume_cursor atomically with the pop, so drainReader's "nothing queued"
        /// fallback check never sees a cursor removed from the queue but not yet visible anywhere.
        resume_cursor = cursor;
        return cursor;
    }

    return resume_cursor;
}

void ReadAheadReader::waitForCursor(uint64_t local_generation)
{
    std::unique_lock fill_serve_lock(fill_serve_mutex);
    fill_serve_cv.wait(
        fill_serve_lock,
        [&] TSA_NO_THREAD_SAFETY_ANALYSIS
        {
            return state != ReaderState::Running || generation != local_generation || !pending_cursors.empty()
                || resume_cursor.has_value();
        });
}

namespace
{

enum class AppendChunkResult : uint8_t
{
    Appended,
    RestartCursor,
    Exit,
};

enum class DecodeChunkStatus : uint8_t
{
    Ready,
    EndOfFile,
    Compacted,
    Error,
    FileMoved,
};

struct DecodeChunkResult
{
    uint64_t first_index = 0;
    DecodeChunkStatus status = DecodeChunkStatus::Ready;
};

/// Outcome of processing one fill cursor's worth of entries.
enum class CursorOutcome : uint8_t
{
    Eof,      /// file EOF: outer loop should handle the cache-boundary check
    Restart,  /// generation changed or file moved: outer loop should retake the next cursor
    Terminal, /// reader state is no longer Running: fillTask should return
};

enum class OpenResult : uint8_t
{
    Ready,
    Compacted,
    Error
};

/// Open the held read buffer for cursor, setting opened_disk/opened_path. Does not seek.
/// On compaction marks the reader and returns false; caller maps to OpenResult::Compacted.
bool openHeldBuffer(ReadAheadReader & reader, const LogReadPlan::FileSpan & cursor)
{
    return cursor.file_description->withReadLock(
        [&]
        {
            if (cursor.file_description->removed_from_disk)
            {
                reader.markCompacted();
                return false;
            }

            const DiskPtr & disk = cursor.file_description->disk;
            const std::string & path = cursor.file_description->path;
            reader.held_buf = disk->readFile(path, getReadSettings());
            reader.opened_disk = disk;
            reader.opened_path = path;
            return true;
        });
}

/// Ensure the held read buffer is open and positioned at cursor.position.
/// Does NOT write resume_cursor — that is the caller's responsibility (single write point in fillFromCursor).
OpenResult ensureOpenAt(ReadAheadReader & reader, const LogReadPlan::FileSpan & cursor, LoggerPtr log)
{
    bool need_seek = false;

    bool cursor_compacted = cursor.file_description->withReadLock(
        [&]
        {
            if (cursor.file_description->removed_from_disk)
            {
                reader.markCompacted();
                return true;
            }

            if (reader.held_buf)
            {
                const DiskPtr cur_disk = cursor.file_description->disk;
                const std::string & cur_path = cursor.file_description->path;
                if (reader.opened_disk != cur_disk || reader.opened_path != cur_path)
                    reader.closeHeld();
                else if (static_cast<size_t>(reader.held_buf->getPosition()) != cursor.position)
                    need_seek = true;
            }

            return false;
        });

    if (cursor_compacted)
        return OpenResult::Compacted;

    if (reader.held_buf && !need_seek)
        return OpenResult::Ready;

    ProfileEvents::increment(ProfileEvents::KeeperLogsReadAheadFillReopens);

    try
    {
        if (!reader.held_buf && !openHeldBuffer(reader, cursor))
            return OpenResult::Compacted;

        /// held_buf is guaranteed non-null here: openHeldBuffer only returns true after assigning it
        /// (IDisk::readFile throws rather than returning null on failure).
        chassert(reader.held_buf);
        reader.held_buf->seek(static_cast<off_t>(cursor.position), SEEK_SET);
        return OpenResult::Ready;
    }
    catch (...)
    {
        tryLogCurrentException(log, "While opening a file for filling entries");
        std::lock_guard fill_serve_lock(reader.fill_serve_mutex);
        reader.setReaderStateLocked(ReaderState::Error);
        return OpenResult::Error;
    }
}

/// Decode up to chunk_size log entries, stopping early once headroom_bytes is used up so a chunk
/// can't overshoot the byte budget. Always decodes at least one entry, even if it alone exceeds
/// headroom_bytes, so the fill always makes forward progress.
DecodeChunkResult decodeChunk(
    ReadAheadReader & reader,
    const LogReadPlan::FileSpan & cursor,
    uint64_t file_to_index,
    size_t chunk_size,
    size_t headroom_bytes,
    uint64_t & expected_idx,
    size_t & next_position,
    std::vector<LogEntryPtr> & chunk,
    LoggerPtr log)
{
    DecodeChunkResult result;
    result.first_index = expected_idx;
    chunk.clear();

    result.status = cursor.file_description->withReadLock(
        [&]
        {
            if (cursor.file_description->removed_from_disk)
                return DecodeChunkStatus::Compacted;

            const DiskPtr cur_disk = cursor.file_description->disk;
            const std::string & cur_path = cursor.file_description->path;
            if (reader.opened_disk != cur_disk || reader.opened_path != cur_path)
            {
                reader.closeHeld();
                return DecodeChunkStatus::FileMoved;
            }

            for (size_t k = 0; k < chunk_size; ++k)
            {
                if (expected_idx > file_to_index)
                    return DecodeChunkStatus::EndOfFile;

                try
                {
                    LogEntryPtr entry;
                    if (!decodeOneRecord(*reader.held_buf, cur_path, expected_idx, entry))
                    {
                        LOG_ERROR(log, "Index mismatch while reading from {}, expected index {}", cur_path, expected_idx);
                        return DecodeChunkStatus::Error;
                    }
                    const size_t entry_size = entryBytes(entry);
                    chunk.push_back(std::move(entry));
                    next_position = static_cast<size_t>(reader.held_buf->getPosition());
                    ++expected_idx;

                    /// Stop as soon as this entry crosses the remaining headroom.
                    if (entry_size >= headroom_bytes)
                        return DecodeChunkStatus::Ready;
                    headroom_bytes -= entry_size;
                }
                catch (...)
                {
                    tryLogCurrentException(log, fmt::format("While reading log entry at index {} from {}", expected_idx, cur_path));
                    return DecodeChunkStatus::Error;
                }
            }
            return DecodeChunkStatus::Ready;
        });

    ProfileEvents::increment(ProfileEvents::KeeperLogsReadAheadFillDecodedEntries, chunk.size());
    return result;
}

/// Append a decoded chunk to the reader's deque under fill_serve_mutex.
AppendChunkResult
appendChunk(ReadAheadReader & reader, uint64_t local_generation, uint64_t chunk_first_index, std::vector<LogEntryPtr> & chunk, LoggerPtr log)
{
    std::lock_guard fill_serve_lock(reader.fill_serve_mutex);
    if (reader.state != ReaderState::Running)
        return AppendChunkResult::Exit;
    if (reader.generation != local_generation)
    {
        reader.resetFillCursorLocked();
        return AppendChunkResult::RestartCursor;
    }

    const uint64_t append_index = reader.decoded_front_index + static_cast<uint64_t>(reader.decoded_entries.size());
    if (append_index < chunk_first_index)
    {
        LOG_ERROR(
            log,
            "Read-ahead fill produced a non-contiguous chunk starting at index {} while next append index is {}",
            chunk_first_index,
            append_index);
        reader.setReaderStateLocked(ReaderState::Error);
        return AppendChunkResult::Exit;
    }

    const size_t first_entry_to_append = static_cast<size_t>(std::min<uint64_t>(append_index - chunk_first_index, chunk.size()));
    for (size_t i = first_entry_to_append; i < chunk.size(); ++i)
    {
        reader.decoded_bytes += entryBytes(chunk[i]);
        reader.decoded_entries.push_back(std::move(chunk[i]));
    }
    reader.fill_serve_cv.notify_all();
    return AppendChunkResult::Appended;
}

/// Stream entries from one fill cursor into the reader's deque.
CursorOutcome fillFromCursor(
    ReadAheadReader & reader,
    const LogReadPlan::FileSpan & cursor,
    uint64_t local_generation,
    const ReadAheadSettings & settings,
    LoggerPtr log) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    chassert(cursor.count > 0);
    const uint64_t file_to_index = cursor.first_index + cursor.count - 1;
    const size_t chunk_size = settings.chunk_size;

    uint64_t expected_idx = cursor.first_index;
    size_t next_position = reader.held_buf ? static_cast<size_t>(reader.held_buf->getPosition()) : cursor.position;

    std::vector<LogEntryPtr> chunk;
    chunk.reserve(chunk_size);

    while (true)
    {
        /// A fully-consumed cursor exits via Eof instead of publishing a dead resume cursor.
        if (expected_idx > file_to_index)
            return CursorOutcome::Eof;

        /// Save the current decode position so the fill can resume from here on the next wakeup.
        {
            std::lock_guard fill_serve_lock(reader.fill_serve_mutex);
            /// Must re-check before publishing below: a generation bump means a serve-side reset
            /// happened, and publishing stale coverage could make installPlanLocked skip a needed install.
            if (reader.state != ReaderState::Running)
                return CursorOutcome::Terminal;
            if (reader.generation != local_generation)
            {
                reader.resetFillCursorLocked();
                return CursorOutcome::Restart;
            }
            reader.setResumeCursorLocked(
                cursor.file_description,
                next_position,
                expected_idx,
                static_cast<size_t>(file_to_index - expected_idx + 1));
        }

        /// Failpoint: wedge the fill for testing.
        FailPointInjection::pauseFailPoint(FailPoints::keeper_changelog_readahead_fill_wedge);

        /// Park at the full budget, but wake only at the low-water mark (hysteresis) so the fill
        /// doesn't re-park immediately after a single popped entry.
        size_t headroom_bytes = 0;
        {
            std::unique_lock fill_serve_lock(reader.fill_serve_mutex);
            if (reader.decoded_bytes >= reader.window_budget_bytes)
            {
                FailPointInjection::pauseFailPoint(FailPoints::keeper_changelog_readahead_park_armed);
                reader.fill_serve_cv.wait(
                    fill_serve_lock,
                    [&] TSA_NO_THREAD_SAFETY_ANALYSIS
                    {
                        return reader.state != ReaderState::Running || reader.generation != local_generation
                            || reader.decoded_bytes <= reader.lowWaterMarkLocked();
                    });
                if (reader.state != ReaderState::Running)
                    return CursorOutcome::Terminal;
                if (reader.generation != local_generation)
                {
                    reader.resetFillCursorLocked();
                    return CursorOutcome::Restart;
                }
                continue;
            }
            chassert(reader.window_budget_bytes > reader.decoded_bytes);
            headroom_bytes = reader.window_budget_bytes - reader.decoded_bytes;
        }

        const auto decoded
            = decodeChunk(reader, cursor, file_to_index, chunk_size, headroom_bytes, expected_idx, next_position, chunk, log);

        switch (decoded.status)
        {
            case DecodeChunkStatus::Compacted: {
                reader.markCompacted();
                return CursorOutcome::Terminal;
            }
            case DecodeChunkStatus::Error: {
                std::lock_guard fill_serve_lock(reader.fill_serve_mutex);
                reader.setReaderStateLocked(ReaderState::Error);
                return CursorOutcome::Terminal;
            }
            case DecodeChunkStatus::FileMoved: {
                /// held_buf was reset after detecting a cross-disk move inside withReadLock.
                /// Resume cursor already snapshotted at the top of this iteration.
                std::lock_guard fill_serve_lock(reader.fill_serve_mutex);
                if (reader.state != ReaderState::Running)
                    return CursorOutcome::Terminal;
                return CursorOutcome::Restart;
            }
            case DecodeChunkStatus::Ready:
            case DecodeChunkStatus::EndOfFile: break;
        }

        /// Append the decoded chunk (both Ready and EndOfFile paths produce partial or full chunks).
        {
            const auto append_result = appendChunk(reader, local_generation, decoded.first_index, chunk, log);
            if (append_result == AppendChunkResult::Exit)
                return CursorOutcome::Terminal;
            if (append_result == AppendChunkResult::RestartCursor)
                return CursorOutcome::Restart;
        }

        if (decoded.status == DecodeChunkStatus::EndOfFile)
            return CursorOutcome::Eof;
    }
}
}

/// Mark a reader Closed and erase it from the map. Fill task observes the state change and self-exits.
/// Taken by value, not by reference into the map, since erasing the entry below would dangle a reference.
void LogEntryStorage::retireReaderLocked(int32_t reader_id, std::shared_ptr<ReadAheadReader> reader)
{
    {
        std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
        reader->closeReaderLocked();
    }
    peer_readers.erase(reader_id);
}

/// Close and discard all per-peer readers (and the commit reader). Self-locking (acquires readers_mutex).
void LogEntryStorage::closeAllReaders()
{
    std::lock_guard map_lock(readers_mutex);
    for (auto & [pid, reader] : peer_readers)
    {
        std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
        reader->closeReaderLocked();
    }
    peer_readers.clear();
    /// Also close and drop commit_reader: serialized against tryPopCommitReadAhead by changelog_lock,
    /// so callers there see either the pre-reset reader or a clean nullptr miss, never a torn state.
    retireCommitReaderLocked();
}

/// Append bounded fill cursors covering `file`'s valid runs over [from_index, end_limit).
/// PRECONDITION: caller holds changelog_lock (shared); from_index is located and its location is in
/// `file`. Returns the exclusive end of the emitted coverage (== from_index when nothing to emit).
uint64_t LogEntryStorage::appendRunCursors(
    LogReadPlan::ReadAheadWindow & window, const ChangelogFileDescriptionPtr & file, uint64_t from_index, uint64_t end_limit) const
{
    /// Defense in depth: never emit a fill cursor into a compressed file regardless of caller.
    assertNotCompressed(file);

    const auto & vr = file->valid_runs;
    /// Clamp to the flushed-and-located prefix: exact end for sealed files, live bound for the active file.
    const uint64_t effective_end = std::min<uint64_t>(vr.end_index, max_index_with_location + 1);
    const uint64_t end = std::min(effective_end, end_limit);
    if (from_index >= end)
        return from_index;

    auto next_it = std::ranges::upper_bound(vr.runs, from_index, {}, &ChangelogFileDescription::ValidRuns::Run::first_index);
    if (next_it == vr.runs.begin())
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Valid-run metadata of {} does not cover located index {}", file->getPathSafe(), from_index);

    auto run_it = std::prev(next_it);

    /// End of the run following `it` (exclusive), or vr.end_index if `it` is the last run.
    const auto run_end_after = [&](auto it)
    {
        auto next = std::next(it);
        return next == vr.runs.end() ? vr.end_index : next->first_index;
    };

    /// A located index falling in a gap after the selected run means the runs drifted from
    /// logs_location -- fail fast rather than silently under-report coverage.
    const uint64_t selected_run_end = run_end_after(run_it);
    if (from_index >= selected_run_end)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR, "Valid-run metadata of {} does not cover located index {}", file->getPathSafe(), from_index);

    for (; run_it != vr.runs.end(); ++run_it)
    {
        const uint64_t run_begin = std::max(from_index, run_it->first_index);
        const uint64_t run_end = std::min(end, run_end_after(run_it));
        if (run_begin >= run_end)
            break;
        LogReadPlan::FileSpan cursor;
        cursor.file_description = file;
        cursor.first_index = run_begin;
        cursor.count = run_end - run_begin;
        cursor.position = run_begin == run_it->first_index
            ? run_it->start_position
            : logs_location.at(run_begin).position; /// clipped start: one lookup, index is located
        window.push_back(std::move(cursor));
    }
    return end;
}

void LogEntryStorage::appendNextFileCursors(LogReadPlan::ReadAheadWindow & window, uint64_t coverage_end) const
{
    if (coverage_end > max_index_with_location)
        return;
    const auto & next_loc = logs_location.at(coverage_end);
    appendRunCursors(window, next_loc.file_description, coverage_end, max_index_with_location + 1);
}

/// Build a read-ahead plan: direct-read items via getReadPlan, plus speculative fill cursors covering
/// each touched file's valid runs, clipped to that file's first planned index, plus one extra file
/// beyond the touched range so the fill task is already primed for the next file boundary.
/// PRECONDITION: caller holds changelog_lock (shared).
LogReadPlan LogEntryStorage::getReadAheadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes, uint64_t retained_start) const
{
    LogReadPlan plan = getReadPlan(start, end, max_size_bytes, retained_start);
    if (plan.logs_compacted)
        return plan;

    if (!readahead_settings.enabled)
        return plan;

    /// Emit each touched file's valid runs, clipped to that file's first planned index. A physical
    /// gap (rewrite hole) shows up as adjacent same-file spans in plan.items; the runs already
    /// describe both sides, so the second span is skipped. A non-FileSpan item marks the start of
    /// the terminal latest-cache suffix and ends the walk.
    LogReadPlan::ReadAheadWindow window;
    ChangelogFileDescriptionPtr prev_file;
    uint64_t coverage_end = 0;
    for (const auto & item : plan.items)
    {
        const auto * run = std::get_if<LogReadPlan::FileSpan>(&item);
        if (!run)
            break;
        if (run->file_description == prev_file)
        {
            chassert(run->first_index < coverage_end);
            continue;
        }
        prev_file = run->file_description;
        coverage_end = appendRunCursors(window, run->file_description, run->first_index, std::numeric_limits<uint64_t>::max());
    }

    if (prev_file)
        appendNextFileCursors(window, coverage_end);

    if (!window.empty())
        plan.read_ahead_window = std::move(window);
    return plan;
}

/// Build a plan for entry `index` on the commit path: the single entry plus bounded fill cursors
/// covering the flushed prefix, up to the commit window budget. The base item is built directly
/// from logs_location, not getReadPlan. Caller already checked getEntryFromMemory under the same
/// lock, so `index` is not in latest_logs_cache. PRECONDITION: caller holds changelog_lock (shared).
LogReadPlan LogEntryStorage::getCommitReadPlan(uint64_t index, uint64_t retained_start) const
{
    LogReadPlan plan;
    plan.start_index = index;
    plan.requested_entry_count = 1;
    plan.epoch = truncation_epoch.load();

    auto base_it = logs_location.find(index);
    if (base_it == logs_location.end())
    {
        if (index < retained_start)
        {
            /// Entry is below retained start -- compacted away (same handling as getReadPlan).
            plan.logs_compacted = true;
            return plan;
        }
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Location of log entry with index {} is missing", index);
    }
    const auto & base_loc = base_it->second;
    assertNotCompressed(base_loc.file_description);
    plan.items.emplace_back(
        LogReadPlan::FileSpan{
            .file_description = base_loc.file_description, .position = base_loc.position, .first_index = index, .count = 1});

    const bool can_use_commit_readahead = readahead_settings.commit_window_bytes != 0 && !latest_logs_cache.hasUnlimitedSpace();
    if (!can_use_commit_readahead)
        return plan;

    /// N+1 planning: covers the base file's remainder plus one file beyond it, bounded by
    /// max_index_with_location (not commit_window_bytes, which only gates enablement above). No
    /// per-batch renewal here, so crossing into a third file costs one serve-wait before the next rebuild.
    LogReadPlan::ReadAheadWindow window;
    uint64_t coverage_end = appendRunCursors(window, base_loc.file_description, index, max_index_with_location + 1);
    appendNextFileCursors(window, coverage_end);
    if (!window.empty())
        plan.read_ahead_window = std::move(window);
    return plan;
}

/// Long-lived fill task per ReadAheadReader, running on readahead_pool.
/// Parks when the deque reaches the byte budget; wakes when the serve side consumes entries.
/// Also parks (rather than exiting) on reaching a cache boundary with no pending cursor, so the
/// reader survives file boundaries and gets re-armed by a later installPlanLocked() instead of
/// being retired and recreated.
/// Exits when state transitions out of Running.
void LogEntryStorage::fillTask(std::shared_ptr<ReadAheadReader> reader) const
try
{
    /// Every return below is preceded by a state transition out of Running; skip the check while an
    /// exception is unwinding, since the catch handler (not this scope) transitions state to Error then.
    SCOPE_EXIT({
        if (std::uncaught_exceptions() == 0)
        {
            std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
            chassert(reader->state != ReaderState::Running);
        }
    });

    while (true)
    {
        uint64_t local_generation = 0;
        bool should_exit = false;
        auto current_cursor = reader->takeNextCursor(local_generation, should_exit);
        if (should_exit)
            return;

        if (!current_cursor.has_value())
        {
            reader->waitForCursor(local_generation);
            continue;
        }

        fiu_do_on(FailPoints::keeper_changelog_readahead_fill_exception,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "keeper_changelog_readahead_fill_exception");
        });

        if (ensureOpenAt(*reader, *current_cursor, log) != OpenResult::Ready)
            return;

        switch (fillFromCursor(*reader, *current_cursor, local_generation, readahead_settings, log))
        {
            case CursorOutcome::Terminal: return;
            case CursorOutcome::Restart: continue;
            case CursorOutcome::Eof: {
                std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
                reader->resume_cursor.reset();
                if (reader->state != ReaderState::Running)
                    return;
                /// About to park with nothing queued: wake any drainReader blocked in its serve wait
                /// so it falls back immediately instead of sleeping out its deadline.
                if (reader->pending_cursors.empty())
                {
                    reader->fill_serve_cv.notify_all();
                    FailPointInjection::pauseFailPoint(FailPoints::keeper_changelog_readahead_park_armed);
                }
                continue;
            }
        }
    }
}
catch (...)
{
    /// Block MEMORY_LIMIT_EXCEEDED so the cleanup below can't itself throw under memory pressure.
    LockMemoryExceptionInThread blocker{VariableContext::Global};
    tryLogCurrentException(log, "Read-ahead fill task failed");
    std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
    reader->setReaderStateLocked(ReaderState::Error);
}

void LogEntryStorage::ensureReadAheadPoolLocked()
{
    /// Don't recreate the pool after shutdown — a fill task would capture `this` after teardown.
    if (is_shutdown || readahead_pool)
        return;
    /// Commit read-ahead is independent of readahead_settings.enabled; when commit_window_bytes > 0
    /// the pool reserves +1 thread/queue slot for it, on top of the peer threads.
    size_t peer_threads = 0;
    if (readahead_settings.enabled)
    {
        peer_threads = readahead_settings.pool_threads;
        if (peer_threads == 0)
            peer_threads = readahead_settings.max_peer_readers;
    }
    const size_t threads = peer_threads + 1;
    /// Double the per-reader slot count: a retired reader's fill task keeps its slot until it
    /// observes Closed and returns, so retire/recreate overlap can transiently need up to 2x.
    readahead_pool = std::make_unique<ThreadPool>(
        CurrentMetrics::KeeperChangelogReadAheadThreads,
        CurrentMetrics::KeeperChangelogReadAheadThreadsActive,
        CurrentMetrics::KeeperChangelogReadAheadThreadsScheduled,
        threads,
        threads,
        2 * (readahead_settings.max_peer_readers + 1),
        /*shutdown_on_exception_=*/false); /// a failed fill must not disable read-ahead process-wide
}

void LogEntryStorage::evictIdleReadersLocked(std::chrono::steady_clock::time_point now)
{
    /// Gate: only scan when the map is at capacity or enough time has elapsed since the last scan.
    /// This keeps the eviction check off the hot path when the map is small and readers are warm.
    /// NOTE: an idle reader may be reaped slightly later than eviction_timeout_ms when the gate
    /// suppresses the scan; terminal reaping in acquireReaderLocked covers the per-peer case.
    const bool at_capacity = peer_readers.size() >= readahead_settings.max_peer_readers;
    const auto gate_interval = std::chrono::milliseconds(readahead_settings.eviction_timeout_ms);
    const auto last_scan = lastEvictionScanTimePoint();
    if (!at_capacity && (now - last_scan) < gate_interval)
        return;
    last_eviction_scan_ticks.store(now.time_since_epoch().count(), std::memory_order_relaxed);

    const auto eviction_timeout = std::chrono::milliseconds(readahead_settings.eviction_timeout_ms);
    for (auto it = peer_readers.begin(); it != peer_readers.end();)
    {
        auto & r = it->second;
        if (now - r->last_access > eviction_timeout)
        {
            LOG_DEBUG(log, "Evicting idle read-ahead reader for peer {}", it->first);
            {
                std::lock_guard fill_serve_lock(r->fill_serve_mutex);
                r->closeReaderLocked();
            }
            it = peer_readers.erase(it);
        }
        else
        {
            ++it;
        }
    }

    /// The commit reader is exempt from capacity pressure but not from idle eviction.
    if (commit_reader && (now - commit_reader->last_access > eviction_timeout))
    {
        LOG_DEBUG(log, "Evicting idle commit read-ahead reader");
        retireCommitReaderLocked();
    }
}

void LogEntryStorage::evictIdleReadersIfNeeded()
{
    /// Skip taking readers_mutex entirely when clearly inside the gate interval.
    const auto now = std::chrono::steady_clock::now();
    const auto gate_interval = std::chrono::milliseconds(readahead_settings.eviction_timeout_ms);
    const auto last_scan = lastEvictionScanTimePoint();
    if ((now - last_scan) < gate_interval)
        return;

    std::lock_guard map_lock(readers_mutex);
    evictIdleReadersLocked(now);
}

/// Whether `reader` is unusable for any new plan: any non-Running state. A reader parked with
/// nothing queued (EOF at a boundary) is still Running -- installPlanLocked re-arms it rather than
/// forcing a retire+recreate.
static bool isReaderTerminalForPlan(ReadAheadReader & reader)
{
    std::lock_guard fill_serve_lock(reader.fill_serve_mutex);
    return reader.state != ReaderState::Running;
}

std::shared_ptr<ReadAheadReader>
LogEntryStorage::makeReaderLocked(uint64_t start_index, size_t budget_bytes, std::chrono::steady_clock::time_point now)
{
    /// readahead_pool can be null if shutdown() set is_shutdown before this call.
    if (!readahead_pool)
        return nullptr;

    auto new_reader = std::make_shared<ReadAheadReader>();
    new_reader->last_access = now;
    new_reader->decoded_front_index = start_index;
    new_reader->window_budget_bytes = budget_bytes;

    /// trySchedule never blocks: pool threads can stay pinned to a parked reader indefinitely, so a
    /// full queue is expected, not an error. Blocking here while holding readers_mutex would deadlock
    /// every other caller that needs it. Fail closed: caller falls back to a direct read instead.
    if (!readahead_pool->trySchedule([this, new_reader]() mutable { fillTask(new_reader); }))
    {
        ProfileEvents::increment(ProfileEvents::KeeperLogsReadAheadScheduleRejected);
        return nullptr;
    }

    ProfileEvents::increment(ProfileEvents::KeeperLogsReadAheadReadersCreated);

    return new_reader;
}

std::shared_ptr<ReadAheadReader>
LogEntryStorage::acquireReaderLocked(int32_t reader_id, const LogReadPlan & plan, std::chrono::steady_clock::time_point now)
{
    auto it = peer_readers.find(reader_id);
    if (it != peer_readers.end() && isReaderTerminalForPlan(*it->second))
    {
        retireReaderLocked(reader_id, it->second);
        it = peer_readers.end();
    }

    if (it == peer_readers.end())
    {
        if (peer_readers.size() >= readahead_settings.max_peer_readers)
        {
            LOG_DEBUG(
                log, "Read-ahead max_peer_readers ({}) reached, falling back for peer {}", readahead_settings.max_peer_readers, reader_id);
            return nullptr;
        }

        auto new_reader = makeReaderLocked(plan.start_index, readahead_settings.window_bytes, now);
        if (!new_reader)
            return nullptr;

        try
        {
            peer_readers[reader_id] = new_reader;
        }
        catch (...)
        {
            /// The fill task already holds the only other reference; without this, an unregistered
            /// reader is never closed and shutdown's pool drain waits on it forever.
            std::lock_guard fill_serve_lock(new_reader->fill_serve_mutex);
            new_reader->closeReaderLocked();
            throw;
        }
        return new_reader;
    }

    it->second->last_access = now;
    return it->second;
}

void LogEntryStorage::installPlanLocked(ReadAheadReader & reader, const LogReadPlan & plan) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    /// Terminal states: leave untouched; drainReader falls back to direct read. A reader parked with
    /// nothing queued is still Running and falls through to the Covered/Gap handling below, which
    /// re-arms it with fresh cursors.
    if (reader.state != ReaderState::Running)
        return;

    const uint64_t start = plan.start_index;

    /// Compute coverage_end BEFORE any clearing so the filter uses the live fill state.
    uint64_t coverage_end = 0;
    if (start < reader.decoded_front_index)
    {
        /// Backward rewind: clear state and reinstall from start.
        reader.resetToIndexLocked(start);
        coverage_end = start;
    }
    else
    {
        coverage_end = reader.fillCoverageEndLocked();
        if (start >= coverage_end)
        {
            /// Gap or fresh reader: fill will not reach start unaided; reset and install all.
            reader.resetToIndexLocked(start);
            coverage_end = start;
        }
        else
        {
            /// Covered: the fill is already at or past start. Advance the deque front to free
            /// byte budget, but keep resume_cursor/pending_cursors so the fill runs uninterrupted.
            if (reader.discardBeforeLocked(start))
                reader.fill_serve_cv.notify_all();
        }
    }

    if (!plan.read_ahead_window || plan.read_ahead_window->empty())
        return;

    /// Install only cursors contiguous with the live fill coverage; stop at the first hole. A run that
    /// grew past coverage_end (straddler) can't be clipped here and is dropped whole, along with
    /// everything after it -- installing across the hole would trip appendChunk's contiguity check
    /// and kill the reader. A later plan re-arms via the reset branch above once coverage_end catches up.
    size_t installed = 0;
    uint64_t expected_next = coverage_end;
    for (const auto & cursor : *plan.read_ahead_window)
    {
        if (cursorCoverageEnd(cursor) <= expected_next)
            continue;
        if (cursor.first_index != expected_next)
            break;
        reader.pending_cursors.push_back(cursor);
        expected_next = cursorCoverageEnd(cursor);
        ++installed;
    }

    ProfileEvents::increment(ProfileEvents::KeeperLogsReadAheadCursorsInstalled, installed);

    if (installed > 0)
        reader.fill_serve_cv.notify_all();
}

LogEntriesPtr LogEntryStorage::drainReader(int32_t reader_id, const std::shared_ptr<ReadAheadReader> & reader, const LogReadPlan & plan)
    TSA_NO_THREAD_SAFETY_ANALYSIS
{
    const auto serve_deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(readahead_settings.serve_wait_timeout_ms);

    /// Failpoint: wedge the serve wait for testing.
    FailPointInjection::pauseFailPoint(FailPoints::keeper_changelog_readahead_serve_wait);

    LogEntriesPtr result = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();
    result->reserve(plan.requested_entry_count);

    const auto retire_reader = [&]
    {
        std::lock_guard map_lock(readers_mutex);
        if (reader_id == COMMIT_READER_ID)
        {
            if (commit_reader == reader)
                retireCommitReaderLocked();
            return;
        }
        auto it = peer_readers.find(reader_id);
        if (it != peer_readers.end() && it->second == reader)
            retireReaderLocked(reader_id, reader);
    };

    const auto advance_reader_to = [&](uint64_t index)
    {
        std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
        reader->discardBeforeLocked(index);
        reader->fill_serve_cv.notify_all();
    };

    uint64_t current_index = plan.start_index;

    const auto fallback_from = [&](size_t item_idx, size_t consumed_from_current_item) -> LogEntriesPtr
    {
        if (result->size() >= plan.requested_entry_count)
            return result;

        chassert(current_index == plan.start_index + result->size());

        LogReadPlan fallback_plan;
        fallback_plan.items.insert(fallback_plan.items.end(), plan.items.begin() + static_cast<ssize_t>(item_idx), plan.items.end());
        fallback_plan.requested_entry_count = plan.requested_entry_count - result->size();
        /// A FileSpan only records its run's first-entry position, so re-decode from the run start
        /// (current_index - consumed_from_current_item) and skip the already-served prefix below.
        fallback_plan.start_index = current_index - consumed_from_current_item;
        /// Carry over the original plan's epoch so the staleness check below compares against the
        /// snapshot this data was actually planned under, not a fresh default 0.
        fallback_plan.epoch = plan.epoch;

        auto tail = executeReadPlan(fallback_plan, /*read_deadline_ms=*/0);
        if (!tail)
            return nullptr;

        for (size_t i = consumed_from_current_item; i < tail->size(); ++i)
            result->push_back(std::move((*tail)[i]));

        advance_reader_to(plan.start_index + result->size());
        return result;
    };

    for (size_t item_idx = 0; item_idx < plan.items.size(); ++item_idx)
    {
        const auto & item = plan.items[item_idx];

        if (std::holds_alternative<LogEntryPtr>(item))
        {
            /// Once a LogEntryPtr item appears, every remaining item is one too (plan invariant), so
            /// serve them all in one shot; advance_reader_to(current_index) after the loop syncs the
            /// deque once instead of per item.
            for (size_t tail_idx = item_idx; tail_idx < plan.items.size(); ++tail_idx)
            {
                const auto * tail_entry = std::get_if<LogEntryPtr>(&plan.items[tail_idx]);
                chassert(tail_entry);
                result->push_back(*tail_entry);
                ++current_index;
            }
            break;
        }

        const auto & run = std::get<LogReadPlan::FileSpan>(item);
        for (size_t consumed = 0; consumed < run.count;)
        {
            const uint64_t needed_index = run.first_index + consumed;
            std::unique_lock fill_serve_lock(reader->fill_serve_mutex);

            while (true)
            {
                if (reader->discardBeforeLocked(needed_index))
                    reader->fill_serve_cv.notify_all();

                if (reader->state == ReaderState::Compacted)
                {
                    fill_serve_lock.unlock();
                    retire_reader();
                    return nullptr;
                }

                if (reader->state == ReaderState::Error)
                {
                    fill_serve_lock.unlock();
                    retire_reader();
                    return fallback_from(item_idx, consumed);
                }

                if (!reader->decoded_entries.empty())
                {
                    if (reader->decoded_front_index == needed_index)
                    {
                        /// Pop the whole contiguous available prefix under one fill_serve_mutex hold
                        /// instead of reacquiring per entry; notify once, after releasing the lock.
                        const size_t bytes_before_pop = reader->decoded_bytes;
                        size_t popped = 0;
                        while (consumed < run.count && !reader->decoded_entries.empty()
                               && reader->decoded_front_index == run.first_index + consumed)
                        {
                            result->push_back(reader->popFrontLocked());
                            if (reader_id == COMMIT_READER_ID)
                                ProfileEvents::increment(ProfileEvents::KeeperLogsEntryReadFromCommitReadAhead);
                            ++consumed;
                            ++popped;
                        }
                        current_index = run.first_index + consumed;
                        const bool crossed_low_water = popped > 0 && reader->crossedLowWaterLocked(bytes_before_pop);
                        fill_serve_lock.unlock();
                        if (crossed_low_water)
                            reader->fill_serve_cv.notify_one();
                        break;
                    }

                    if (reader->decoded_front_index > needed_index)
                    {
                        /// discardBeforeLocked only ever advances the front, so an overlapping/retried
                        /// request for an already-served index can't be replayed from the deque.
                        fill_serve_lock.unlock();
                        return fallback_from(item_idx, consumed);
                    }
                }

                /// A parked-but-Running reader (nothing queued) won't produce needed_index without a
                /// fresh plan install, which can't happen from inside this wait -- give up immediately.
                if (reader->state == ReaderState::Closed
                    || (reader->state == ReaderState::Running && reader->pending_cursors.empty() && !reader->resume_cursor.has_value()))
                {
                    fill_serve_lock.unlock();
                    return fallback_from(item_idx, consumed);
                }

                const bool ready = reader->fill_serve_cv.wait_until(
                    fill_serve_lock,
                    serve_deadline,
                    [&]
                    {
                        /// Last clause mirrors the parked-with-nothing-queued check above, so an EOF
                        /// reached during this wait falls back immediately instead of sleeping it out.
                        return !reader->decoded_entries.empty() || reader->state != ReaderState::Running
                            || (reader->pending_cursors.empty() && !reader->resume_cursor.has_value());
                    });

                if (!ready)
                {
                    /// Accepted: on sustained slow storage this duplicate-read fallback can fire on every
                    /// batch; log_readahead_serve_wait_timeout_ms is the operator lever for that case.
                    fill_serve_lock.unlock();
                    ProfileEvents::increment(ProfileEvents::KeeperLogsReadAheadTimeoutFallbacks);
                    auto fallback_result = fallback_from(item_idx, consumed);
                    if (fallback_result)
                    {
                        /// The fill may still be decoding the range just served directly above; fast-forward
                        /// the reader past it (bumping generation discards any in-flight stale chunk) so the
                        /// fill parks with nothing to do until the next plan resumes it from here.
                        const uint64_t end_of_result = plan.start_index + fallback_result->size();
                        std::lock_guard reset_lock(reader->fill_serve_mutex);
                        reader->resetToIndexLocked(end_of_result);
                    }
                    return fallback_result;
                }
            }
        }
    }

    advance_reader_to(current_index);
    return result;
}

/// Serve a read request using the per-peer read-ahead deque.
/// PRECONDITION: called WITHOUT changelog_lock.
LogEntriesPtr LogEntryStorage::serveReadAhead(int32_t reader_id, const LogReadPlan & plan) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    if (!plan.read_ahead_window || plan.logs_compacted)
    {
        /// The direct-read path otherwise skips eviction entirely once all peers are caught up to the tip.
        evictIdleReadersIfNeeded();
        return executeReadPlan(plan, /*read_deadline_ms=*/0);
    }

    const auto now = std::chrono::steady_clock::now();
    std::shared_ptr<ReadAheadReader> reader;
    {
        std::lock_guard map_lock(readers_mutex);
        if (!is_shutdown)
        {
            if (plan.epoch != truncation_epoch.load())
            {
                ProfileEvents::increment(ProfileEvents::KeeperLogsReadAheadPlanEpochMismatches);
                return nullptr;
            }

            ensureReadAheadPoolLocked();
            evictIdleReadersLocked(now);
            reader = acquireReaderLocked(reader_id, plan, now);

            if (reader && plan.epoch != truncation_epoch.load())
            {
                /// The plan's file positions may no longer describe current content after a concurrent
                /// writeAt; fail outright rather than fall back to executeReadPlan, whose items are
                /// equally stale.
                ProfileEvents::increment(ProfileEvents::KeeperLogsReadAheadPlanEpochMismatches);
                return nullptr;
            }
        }
    }
    if (!reader)
        return executeReadPlan(plan, /*read_deadline_ms=*/0);

    FailPointInjection::pauseFailPoint(FailPoints::keeper_changelog_readahead_pre_drain);

    {
        std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
        installPlanLocked(*reader, plan);
    }

    auto served = drainReader(reader_id, reader, plan);

    /// The pre-drain check above narrows but doesn't close the race: a write_at can still close/rewrite
    /// readers while the drain was in flight. Peer-only: the commit path already re-checks post-serve
    /// in entry_at_ext with retry semantics.
    if (served && plan.epoch != truncation_epoch.load())
    {
        ProfileEvents::increment(ProfileEvents::KeeperLogsReadAheadPlanEpochMismatches);
        return nullptr;
    }

    return served;
}

/// Reuse commit_reader unless terminal (Closed/Compacted/Error), else retire and recreate lazily.
/// Returns nullptr if the pool is unavailable (post-shutdown).
std::shared_ptr<ReadAheadReader>
LogEntryStorage::acquireCommitReaderLocked(const LogReadPlan & plan, std::chrono::steady_clock::time_point now)
{
    if (commit_reader && isReaderTerminalForPlan(*commit_reader))
        retireCommitReaderLocked();

    if (!commit_reader)
        commit_reader = makeReaderLocked(plan.start_index, readahead_settings.commit_window_bytes, now);
    else
        commit_reader->last_access = now;

    return commit_reader;
}

void LogEntryStorage::retireCommitReaderLocked()
{
    if (!commit_reader)
        return;
    {
        std::lock_guard fill_serve_lock(commit_reader->fill_serve_mutex);
        commit_reader->closeReaderLocked();
    }
    commit_reader.reset();
}

LogEntryPtr LogEntryStorage::tryPopCommitReadAhead(uint64_t index)
{
    std::shared_ptr<ReadAheadReader> reader;
    {
        std::lock_guard map_lock(readers_mutex);
        reader = commit_reader;
        /// Refresh last_access: this fast path doesn't otherwise touch readers_mutex.
        if (reader)
            reader->last_access = std::chrono::steady_clock::now();
    }
    if (!reader)
        return nullptr;

    LogEntryPtr entry;
    bool crossed_low_water = false;
    {
        std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
        /// Closed/Compacted/Error readers may hold content invalidated by writeAt/compaction; treat
        /// as a miss and rebuild from logs_location.
        if (reader->state != ReaderState::Running)
            return nullptr;
        if (reader->decoded_entries.empty() || reader->decoded_front_index != index)
            return nullptr;

        /// NuRaft's commit loop consumes one at a time, so this stays single-pop, but still applies
        /// the low-water crossing check, notifying only after releasing fill_serve_mutex.
        const size_t bytes_before_pop = reader->decoded_bytes;
        entry = reader->popFrontLocked();
        crossed_low_water = reader->crossedLowWaterLocked(bytes_before_pop);
    }

    if (crossed_low_water)
        reader->fill_serve_cv.notify_one(); /// frees window budget: wakes a parked fill
    ProfileEvents::increment(ProfileEvents::KeeperLogsEntryReadFromCommitReadAhead);
    return entry;
}

/// Serve a single-entry commit read: entry_at_ext handles cheap hits and the fast path under
/// changelog_lock; this only runs the miss path, without the lock.
LogEntryPtr LogEntryStorage::serveCommitEntry(uint64_t index, const LogReadPlan & plan) TSA_NO_THREAD_SAFETY_ANALYSIS
{
    chassert(plan.start_index == index);

    const auto direct_read = [&]() -> LogEntryPtr
    {
        LogReadPlan direct = plan;
        direct.read_ahead_window.reset();
        auto entries = executeReadPlan(direct, /*read_deadline_ms=*/0);
        if (entries == nullptr || entries->empty())
            return nullptr; /// genuinely gone (compacted/removed), or a stale epoch -- the caller
                             /// tells these apart via currentTruncationEpoch and retries on the latter.
        return (*entries)[0];
    };

    if (!plan.read_ahead_window || plan.logs_compacted)
        return direct_read();

    const auto now = std::chrono::steady_clock::now();
    std::shared_ptr<ReadAheadReader> reader;
    bool epoch_stale = false;
    {
        std::lock_guard map_lock(readers_mutex);
        ensureReadAheadPoolLocked();
        reader = acquireCommitReaderLocked(plan, now);
        epoch_stale = plan.epoch != truncation_epoch.load();
    }
    if (!reader)
        return direct_read();

    if (epoch_stale)
    {
        /// This plan's cursors were computed before a completed writeAt; installing them would poison
        /// `reader` with pre-truncation file positions. Skip install/drain and skip the direct-read
        /// fallback too (it would be discarded as stale anyway) -- the caller retries with a fresh plan.
        return nullptr;
    }

    {
        std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
        installPlanLocked(*reader, plan);
    }

    if (auto entries = drainReader(COMMIT_READER_ID, reader, plan); entries != nullptr && !entries->empty())
        return (*entries)[0];

    /// On Compacted, drainReader returns nullptr without reading; since nullptr is fatal in NuRaft,
    /// retry once via blocking direct read and return nullptr only if the entry is genuinely gone.
    return direct_read();
}

void LogEntryStorage::getKeeperLogInfo(KeeperLogInfo & log_info) const
{
    log_info.latest_logs_cache_entries = latest_logs_cache.numberOfEntries();
    log_info.latest_logs_cache_size = latest_logs_cache.cache_size;

    std::lock_guard map_lock(readers_mutex);
    if (commit_reader)
    {
        std::lock_guard fill_serve_lock(commit_reader->fill_serve_mutex);
        log_info.commit_logs_cache_entries = commit_reader->decoded_entries.size();
        log_info.commit_logs_cache_size = commit_reader->decoded_bytes;
    }
}

bool LogEntryStorage::isConfigLog(uint64_t index) const
{
    return logs_with_config_changes.contains(index);
}

size_t LogEntryStorage::empty() const
{
    return logs_location.empty() && latest_logs_cache.empty();
}

size_t LogEntryStorage::size() const
{
    if (empty())
        return 0;

    size_t min_index = 0;
    size_t max_index = 0;

    if (!logs_location.empty())
    {
        min_index = min_index_with_location;
        max_index = max_index_with_location;
    }
    else
        min_index = latest_logs_cache.min_index_in_cache;

    if (!latest_logs_cache.empty())
        max_index = latest_logs_cache.max_index_in_cache;

    return max_index - min_index + 1;
}

size_t LogEntryStorage::getFirstIndex() const
{
    if (!logs_location.empty())
        return min_index_with_location;

    if (!latest_logs_cache.empty())
        return latest_logs_cache.min_index_in_cache;

    return 0;
}

void LogEntryStorage::checkValidRunsConsistency() const
{
    for (const auto & [index, loc] : logs_location)
    {
        const auto & vr = loc.file_description->valid_runs;
        chassert(index < vr.end_index);

        auto run_it = std::ranges::upper_bound(vr.runs, index, {}, &ChangelogFileDescription::ValidRuns::Run::first_index);
        chassert(run_it != vr.runs.begin());
        --run_it;

        if (index == run_it->first_index)
            chassert(loc.position == run_it->start_position);
    }

    std::unordered_set<const ChangelogFileDescription *> checked_files;
    for (const auto & [index, loc] : logs_location)
    {
        const auto * file = loc.file_description.get();
        if (!checked_files.insert(file).second)
            continue;

        const auto & runs = file->valid_runs.runs;
        for (size_t i = 1; i < runs.size(); ++i)
            chassert(runs[i - 1].first_index < runs[i].first_index);
    }
}

size_t LogEntryStorage::getReaderDecodedBytesForTests(int32_t reader_id) const
{
    std::shared_ptr<ReadAheadReader> reader;
    {
        std::lock_guard map_lock(readers_mutex);
        if (reader_id == COMMIT_READER_ID)
            reader = commit_reader;
        else if (auto it = peer_readers.find(reader_id); it != peer_readers.end())
            reader = it->second;
    }
    if (!reader)
        return 0;

    std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
    return reader->decoded_bytes;
}

bool LogEntryStorage::hasCommitReaderForTests() const
{
    std::lock_guard map_lock(readers_mutex);
    return commit_reader != nullptr;
}

void LogEntryStorage::shutdown()
{
    if (is_shutdown.exchange(true))
        return;

    /// Mark all readers Closed, then drain the pool (which joins all scheduled fill tasks).
    std::unique_ptr<ThreadPool> readahead_pool_to_drain;
    {
        std::lock_guard map_lock(readers_mutex);
        for (auto & [pid, reader] : peer_readers)
        {
            std::lock_guard fill_serve_lock(reader->fill_serve_mutex);
            reader->closeReaderLocked();
        }
        peer_readers.clear();
        if (commit_reader)
        {
            std::lock_guard fill_serve_lock(commit_reader->fill_serve_mutex);
            commit_reader->closeReaderLocked();
        }
        commit_reader.reset();
        readahead_pool_to_drain = std::move(readahead_pool);
    }
    if (readahead_pool_to_drain)
    {
        try
        {
            readahead_pool_to_drain->wait();
        }
        catch (...)
        {
            tryLogCurrentException(log, "Failed to drain readahead pool");
        }
    }
}


ChangelogFileDescriptionPtr Changelog::getChangelogFileDescription(const std::filesystem::path & path)
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
    result->is_compressed = chooseCompressionMethod(result->path, "") != CompressionMethod::None;
    return result;
}

void Changelog::readChangelog(ChangelogFileDescriptionPtr changelog_description, LogEntryStorage & entry_storage)
{
    ChangelogReader reader(changelog_description);
    reader.readChangelog(entry_storage, changelog_description->from_log_index, getLogger("Changelog"));
}

void Changelog::spliceChangelog(ChangelogFileDescriptionPtr source_changelog, ChangelogFileDescriptionPtr destination_changelog)
{
    CoordinationSettingsPtr settings = std::make_shared<CoordinationSettings>();
    auto keeper_context = std::make_shared<KeeperContext>(true, settings);
    keeper_context->setLogDisk(destination_changelog->disk);
    LogFileSettings log_file_settings
    {
        .compress_logs = chooseCompressionMethod(destination_changelog->path, "auto") != CompressionMethod::None
    };
    LogEntryStorage entry_storage{log_file_settings, ReadAheadSettings{}, keeper_context};
    readChangelog(source_changelog, entry_storage);

    std::map<uint64_t, ChangelogFileDescriptionPtr> existing_changelogs;
    ChangelogWriter writer(existing_changelogs, entry_storage, keeper_context, log_file_settings, /*move_changelog_cb_=*/{});
    writer.setFile(destination_changelog, WriteMode::Rewrite);

    for (auto i = destination_changelog->from_log_index; i <= destination_changelog->to_log_index; ++i)
    {
        auto entry = entry_storage.getEntry(i);
        writer.appendRecord(buildRecord(i, entry));
    }

    writer.finalize();
}


Changelog::Changelog(
    LoggerPtr log_, LogFileSettings log_file_settings, FlushSettings flush_settings_, ReadAheadSettings readahead_settings_, KeeperContextPtr keeper_context_)
    : changelogs_detached_dir("detached")
    , rotate_interval(log_file_settings.rotate_interval)
    , compress_logs(log_file_settings.compress_logs)
    , startup_read_max_streams(
          log_file_settings.startup_read_max_streams == 0 ? getNumberOfCPUCoresToUse() : log_file_settings.startup_read_max_streams)
    , startup_read_buffer_size(log_file_settings.startup_read_buffer_size)
    , log(log_)
    , entry_storage(log_file_settings, readahead_settings_, keeper_context_)
    , write_operations(std::numeric_limits<size_t>::max())
    , append_completion_queue(std::numeric_limits<size_t>::max())
    , keeper_context(std::move(keeper_context_))
    , flush_settings(flush_settings_)
{
    try
    {
        /// Settings-layer NonZeroUInt64 can be bypassed by direct construction (gtests); re-check here.
        if (log_file_settings.startup_read_buffer_size == 0)
            throw DB::Exception(DB::ErrorCodes::BAD_ARGUMENTS, "startup_read_buffer_size must be greater than 0");

        if (auto latest_log_disk = getLatestLogDisk();
            log_file_settings.force_sync && dynamic_cast<const DiskLocal *>(latest_log_disk.get()) == nullptr)
        {
            throw DB::Exception(
                DB::ErrorCodes::BAD_ARGUMENTS,
                "force_sync is set to true for logs but disk '{}' cannot satisfy such guarantee because it's not of type DiskLocal.\n"
                "If you want to use force_sync and same disk for all logs, please set keeper_server.log_storage_disk to a local disk.\n"
                "If you want to use force_sync and different disk only for old logs, please set 'keeper_server.log_storage_disk' to any "
                "supported disk and 'keeper_server.latest_log_storage_disk' to a local disk.\n"
                "Otherwise, disable force_sync",
                latest_log_disk->getName());
        }

        /// Load all files on changelog disks

        std::unordered_set<DiskPtr> read_disks;

        const auto load_from_disk = [&](const auto & disk)
        {
            if (read_disks.contains(disk))
                return;

            LOG_TRACE(log, "Reading from disk {}", disk->getName());
            std::unordered_map<std::string, std::string> incomplete_files;

            const auto clean_incomplete_file = [&](const auto & file_path)
            {
                if (auto incomplete_it = incomplete_files.find(fs::path(file_path).filename()); incomplete_it != incomplete_files.end())
                {
                    LOG_TRACE(log, "Removing {} from {}", file_path, disk->getName());
                    disk->removeFile(file_path);
                    disk->removeFile(incomplete_it->second);
                    incomplete_files.erase(incomplete_it);
                    return true;
                }

                return false;
            };

            std::vector<std::string> changelog_files;
            for (auto it = disk->iterateDirectory(""); it->isValid(); it->next())
            {
                const auto & file_name = it->name();
                if (file_name == changelogs_detached_dir)
                    continue;

                if (file_name.starts_with(tmp_keeper_file_prefix))
                {
                    incomplete_files.emplace(file_name.substr(tmp_keeper_file_prefix.size()), it->path());
                    continue;
                }

                if (file_name.starts_with(DEFAULT_PREFIX))
                {
                    if (!clean_incomplete_file(it->path()))
                        changelog_files.push_back(it->path());
                }
                else
                {
                    LOG_WARNING(log, "Unknown file found in log directory: {}", file_name);
                }
            }

            for (const auto & changelog_file : changelog_files)
            {
                if (clean_incomplete_file(fs::path(changelog_file).filename()))
                    continue;

                auto file_description = getChangelogFileDescription(changelog_file);
                file_description->disk = disk;

                LOG_TRACE(log, "Found {} on {}", changelog_file, disk->getName());
                auto [changelog_it, inserted] = existing_changelogs.insert_or_assign(file_description->from_log_index, std::move(file_description));

                if (!inserted)
                    LOG_WARNING(log, "Found duplicate entries for {}, will use the entry from {}", changelog_it->second->path, disk->getName());
            }

            for (const auto & [name, path] : incomplete_files)
                disk->removeFile(path);

            read_disks.insert(disk);
        };

        /// Load all files from old disks
        for (const auto & disk : keeper_context->getOldLogDisks())
            load_from_disk(disk);

        auto disk = getDisk();
        load_from_disk(disk);

        auto latest_log_disk = getLatestLogDisk();
        if (disk != latest_log_disk)
            load_from_disk(latest_log_disk);

        if (existing_changelogs.empty())
            LOG_WARNING(log, "No logs exists in {}. It's Ok if it's the first run of clickhouse-keeper.", disk->getPath());

        background_changelog_operations_thread = std::make_unique<ThreadFromGlobalPool>([this] { backgroundChangelogOperationsThread(); });

        write_thread = std::make_unique<ThreadFromGlobalPool>([this] { writeThread(); });

        append_completion_thread = std::make_unique<ThreadFromGlobalPool>([this] { appendCompletionThread(); });

        current_writer = std::make_unique<ChangelogWriter>(
            existing_changelogs,
            entry_storage,
            keeper_context,
            log_file_settings,
            /*move_changelog_cb=*/[&](ChangelogFileDescriptionPtr changelog, std::string new_path, DiskPtr new_disk)
            { moveChangelogAsync(std::move(changelog), std::move(new_path), std::move(new_disk)); });
    }
    catch (...)
    {
        tryLogCurrentException(log);
        throw;
    }
}

void Changelog::readChangelogAndInitWriter(uint64_t last_commited_log_index, uint64_t logs_to_keep)
{
    std::lock_guard writer_lock(writer_mutex);

    /// We must start to read from this log index
    uint64_t start_to_read_from = last_commited_log_index + 1;

    /// If we need to have some reserved log read additional `logs_to_keep` logs
    if (start_to_read_from > logs_to_keep)
        start_to_read_from -= logs_to_keep;
    else
        start_to_read_from = 1;

    /// Files with to_log_index >= start_to_read_from, in from_log_index order.
    std::vector<ChangelogFileDescriptionPtr> in_scope_files;
    for (const auto & [from_idx, file_description] : existing_changelogs)
        if (file_description->to_log_index >= start_to_read_from)
            in_scope_files.push_back(file_description);

    /// Raw-seek reads are invalid for compressed files; only in-scope files matter (a stale
    /// compressed file below the snapshot shouldn't disable the feature).
    const bool any_in_scope_compressed
        = std::ranges::any_of(in_scope_files, [](const auto & file_description) { return file_description->is_compressed; });

    /// A single in-scope file has no parallelism to exploit; `startup_read_max_streams <= 1`
    /// (explicitly requested, or auto-resolved to 1 on a single-core machine) selects serial too.
    const bool use_serial_read = compress_logs || any_in_scope_compressed || force_serial_startup_read_for_test
        || startup_read_max_streams <= 1 || in_scope_files.size() <= 1;

    if (use_serial_read)
        readChangelogAndInitWriterSerialLocked(last_commited_log_index, start_to_read_from);
    else
        readChangelogAndInitWriterParallelLocked(last_commited_log_index, start_to_read_from, std::move(in_scope_files));

    initialized = true;
}

/// Keep this control flow in sync with replayStartupMetadata, which mirrors it over metadata only.
void Changelog::readChangelogAndInitWriterSerialLocked(uint64_t last_commited_log_index, uint64_t start_to_read_from)
{
    std::optional<ChangelogReadResult> last_log_read_result;

    /// Last log has some free space to write
    bool last_log_is_not_complete = false;

    uint64_t last_read_index = 0;

    uint64_t remove_logs_before_index = 0;
    /// Got through changelog files in order of start_index
    for (const auto & [changelog_start_index, changelog_description_ptr] : existing_changelogs)
    {
        const auto & changelog_description = *changelog_description_ptr;
        /// [from_log_index.>=.......start_to_read_from.....<=.to_log_index]
        if (changelog_description.to_log_index >= start_to_read_from)
        {
            if (!last_log_read_result) /// still nothing was read
            {
                checkFirstChangelogFile(
                    changelog_description.from_log_index,
                    changelog_description.to_log_index,
                    last_commited_log_index,
                    start_to_read_from,
                    log);
            }
            else if (changelog_description.from_log_index > last_read_index && (changelog_description.from_log_index - last_read_index) > 1)
            {
                /// If the gap is before the last committed log index, we can remove the logs before the gap
                /// because they are already present in the existing snapshot
                if (changelog_description.from_log_index <= last_commited_log_index)
                {
                    LOG_INFO(
                        log,
                        "Found gap in changelogs from {} to {}, but these entries are already present in the existing snapshot (last committed: {}). "
                        "Removing logs before index {}.",
                        last_read_index,
                        changelog_description.from_log_index,
                        last_commited_log_index,
                        changelog_description.from_log_index);
                    remove_logs_before_index = changelog_description.from_log_index;
                    entry_storage.clear();
                    last_log_read_result.reset();
                }
                else
                {
                    if (!last_log_read_result->error)
                    {
                        throw Exception(
                            ErrorCodes::CORRUPTED_DATA,
                            "Some records were lost, last found log index {}, while the next log index on disk is {}. Manual intervention "
                            "is necessary for recovery but removing changelogs can lead to data loss.",
                            last_read_index,
                            changelog_description.from_log_index);
                    }
                    break;
                }
            }

            ChangelogReader reader(changelog_description_ptr);
            auto log_read_result = reader.readChangelog(entry_storage, start_to_read_from, log);

            /// We didn't find the first required log in this changelog so we move to the next changelog
            /// This can happen in case we failed to rename changelog to a name with correct first and last log index
            if (log_read_result.first_read_index == 0)
            {
                LOG_TRACE(log, "Changelog is empty or contains only logs before {}", start_to_read_from);
                continue;
            }

            last_log_read_result = std::move(log_read_result);

            if (last_log_read_result->last_read_index != 0)
                last_read_index = last_log_read_result->last_read_index;

            last_log_read_result->log_start_index = changelog_description.from_log_index;

            if (last_log_read_result->last_read_index != 0)
                max_log_id.store(last_log_read_result->last_read_index, std::memory_order_relaxed);

            /// How many entries we have in the last changelog
            uint64_t log_count = changelog_description.expectedEntriesCountInLog();

            /// Unfinished log
            last_log_is_not_complete = last_log_read_result->error || last_log_read_result->total_entries_read_from_log < log_count;
        }
    }

    std::optional<LastChangelogReadOutcome> last_log_read_outcome;
    if (last_log_read_result)
        last_log_read_outcome = LastChangelogReadOutcome{
            .log_start_index = last_log_read_result->log_start_index,
            .last_read_index = last_log_read_result->last_read_index,
            .error = last_log_read_result->error,
            .compressed_log = last_log_read_result->compressed_log};

    finalizeChangelogsAfterRead(last_commited_log_index, remove_logs_before_index, last_log_read_outcome, last_log_is_not_complete);
}

void Changelog::finalizeChangelogsAfterRead(
    uint64_t last_commited_log_index,
    uint64_t remove_logs_before_index,
    const std::optional<LastChangelogReadOutcome> & last_log_read_outcome,
    bool last_log_is_not_complete)
{
    if (remove_logs_before_index)
        removeAllLogFilesBefore(remove_logs_before_index);

    const auto move_from_latest_logs_disks = [&](auto & description)
    {
        /// check if we need to move completed log to another disk
        auto latest_log_disk = getLatestLogDisk();
        auto disk = getDisk();

        if (latest_log_disk != disk && latest_log_disk == description->disk)
            moveChangelogBetweenDisks(latest_log_disk, description, disk, description->path, keeper_context);
    };

    /// we can have empty log (with zero entries) and last_log_read_outcome will be initialized
    if (!last_log_read_outcome || entry_storage.empty()) /// We just may have no logs (only snapshot or nothing)
    {
        /// Just to be sure they don't exist
        removeAllLogs();
        max_log_id.store(last_commited_log_index, std::memory_order_relaxed);
    }
    else if (max_log_id.load(std::memory_order_relaxed) < last_commited_log_index) /// If we have more fresh snapshot than our logs
    {
        LOG_WARNING(
            log,
            "Our most fresh log_id {} is smaller than stored data in snapshot {}. It can indicate data loss. Removing outdated logs.",
            max_log_id.load(std::memory_order_relaxed),
            last_commited_log_index);

        removeAllLogs();
        max_log_id.store(last_commited_log_index, std::memory_order_relaxed);
    }
    else if (last_log_is_not_complete) /// if it's complete just start new one
    {
        chassert(last_log_read_outcome != std::nullopt);
        chassert(!existing_changelogs.empty());

        /// Continue to write into incomplete existing log if it didn't finish with error
        auto & description = existing_changelogs[last_log_read_outcome->log_start_index];

        const auto remove_invalid_logs = [&]
        {
            /// Actually they shouldn't exist, but to be sure we remove them
            removeAllLogsAfter(last_log_read_outcome->log_start_index);

            /// This log, even if it finished with error shouldn't be removed
            chassert(existing_changelogs.contains(last_log_read_outcome->log_start_index));
            chassert(existing_changelogs.find(last_log_read_outcome->log_start_index)->first == existing_changelogs.rbegin()->first);
        };

        if (last_log_read_outcome->last_read_index == 0) /// If it's broken or empty log then remove it
        {
            LOG_INFO(log, "Removing changelog {} because it's empty", description->path);
            remove_invalid_logs();
            description->disk->removeFile(description->path);
            existing_changelogs.erase(last_log_read_outcome->log_start_index);
            entry_storage.cleanAfter(last_log_read_outcome->log_start_index - 1);
        }
        else if (last_log_read_outcome->error)
        {
            LOG_INFO(log, "Changelog {} read finished with error but some logs were read from it, file will not be removed", description->path);
            remove_invalid_logs();
            entry_storage.cleanAfter(last_log_read_outcome->last_read_index);
            description->broken_at_end = true;
            move_from_latest_logs_disks(description);
        }
        /// don't mix compressed and uncompressed writes
        else if (compress_logs == last_log_read_outcome->compressed_log)
        {
            initWriter(description);
        }
    }
    else if (last_log_read_outcome.has_value())
    {
        move_from_latest_logs_disks(existing_changelogs.at(last_log_read_outcome->log_start_index));
    }

    /// Start new log if we don't initialize writer from previous log. All logs can be "complete".
    if (!current_writer->isFileSet())
        current_writer->rotate(max_log_id.load(std::memory_order_relaxed) + 1);

    /// Move files to correct disks
    auto latest_start_index = current_writer->getStartIndex();
    auto latest_log_disk = getLatestLogDisk();
    auto disk = getDisk();
    for (const auto & [start_index, description] : existing_changelogs)
    {
        /// latest log should already be on latest_log_disk
        if (start_index == latest_start_index)
        {
            chassert(description->disk == latest_log_disk);
            continue;
        }

        if (description->disk != disk)
            moveChangelogBetweenDisks(description->disk, description, disk, description->path, keeper_context);
    }
}

void Changelog::readChangelogAndInitWriterParallelLocked(
    uint64_t last_commited_log_index, uint64_t start_to_read_from, std::vector<ChangelogFileDescriptionPtr> in_scope_files)
{
    chassert(in_scope_files.size() > 1);
    chassert(startup_read_max_streams > 0);

    const bool unlimited_cache_mode = entry_storage.isUnlimitedCacheMode();

    ReadSettings read_settings = getReadSettings();
    read_settings.local_fs_settings.buffer_size = startup_read_buffer_size;
    read_settings.remote_fs_settings.buffer_size = startup_read_buffer_size;

    /// (1) parallel read. `results` outlives the pool, so its destructor joins all tasks first.
    /// Never more threads than files.
    const uint64_t pool_size = std::min<uint64_t>(startup_read_max_streams, in_scope_files.size());
    chassert(pool_size > 0);
    std::vector<ChangelogFileStartupReadResult> results(in_scope_files.size());
    Stopwatch read_watch;
    {
        ThreadPool pool(
            CurrentMetrics::KeeperChangelogStartupReadThreads,
            CurrentMetrics::KeeperChangelogStartupReadThreadsActive,
            CurrentMetrics::KeeperChangelogStartupReadThreadsScheduled,
            pool_size,
            /*max_free_threads_*/ 0,
            /*queue_size_*/ 0);
        for (size_t i = 0; i < in_scope_files.size(); ++i)
            pool.scheduleOrThrowOnError([&, i]
            {
                results[i] = readChangelogFile(in_scope_files[i], start_to_read_from, read_settings, unlimited_cache_mode, log);
            });
        /// Tasks catch into results[i] instead of rethrowing; replayStartupMetadata rethrows a
        /// fatal at the point serial would have opened that file.
        pool.wait();
    }
    ProfileEvents::increment(ProfileEvents::KeeperChangelogStartupReadMicroseconds, read_watch.elapsedMicroseconds());

    uint64_t total_entries = 0;
    uint64_t total_bytes = 0;
    for (const auto & result : results)
    {
        total_entries += result.read_result.total_entries_read_from_log;
        total_bytes += result.read_result.total_bytes_read_from_log;
    }
    ProfileEvents::increment(ProfileEvents::KeeperChangelogStartupReadEntries, total_entries);
    ProfileEvents::increment(ProfileEvents::KeeperChangelogStartupReadBytes, total_bytes);

    /// (2) metadata replay, decisions only. (3) build entry_storage state from the result.
    Stopwatch stitch_watch;
    StitchState stitch_state = replayStartupMetadata(results, start_to_read_from, last_commited_log_index, log);
    materializeEntryStorage(entry_storage, results, stitch_state, unlimited_cache_mode);
    max_log_id.store(stitch_state.last_read_index, std::memory_order_relaxed);
    ProfileEvents::increment(ProfileEvents::KeeperChangelogStartupStitchMicroseconds, stitch_watch.elapsedMicroseconds());

    /// (4) disposition -- same tail as the serial path
    std::optional<LastChangelogReadOutcome> last_log_read_outcome;
    if (stitch_state.last_log_read_result)
        last_log_read_outcome = LastChangelogReadOutcome{
            .log_start_index = stitch_state.last_log_read_result->log_start_index,
            .last_read_index = stitch_state.last_log_read_result->last_read_index,
            .error = stitch_state.last_log_read_result->error,
            .compressed_log = stitch_state.last_log_read_result->compressed_log};

    finalizeChangelogsAfterRead(
        last_commited_log_index, stitch_state.remove_logs_before_index, last_log_read_outcome, stitch_state.last_log_is_not_complete);

    /// (5) seed the cache with the last live entry (skipped in unlimited mode). Reads max_log_id,
    /// not stitch_state.last_read_index, since finalizeChangelogsAfterRead may have trimmed it.
    if (!unlimited_cache_mode && !entry_storage.empty())
    {
        const auto last_index = max_log_id.load(std::memory_order_relaxed);
        auto last_entry = entry_storage.getEntry(last_index);
        chassert(last_entry);
        entry_storage.addEntryToLatestCache(last_index, last_entry);
    }
}

void Changelog::initWriter(ChangelogFileDescriptionPtr description)
{
    if (description->expectedEntriesCountInLog() != rotate_interval)
        LOG_TRACE(
            log,
            "Looks like rotate_logs_interval was changed, current {}, expected entries in last log {}",
            rotate_interval,
            description->expectedEntriesCountInLog());

    LOG_TRACE(log, "Continue to write into {}", description->path);

    auto log_disk = description->disk;
    auto latest_log_disk = getLatestLogDisk();
    if (log_disk != latest_log_disk)
        moveChangelogBetweenDisks(log_disk, description, latest_log_disk, description->path, keeper_context);

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

DiskPtr Changelog::getDisk() const
{
    return keeper_context->getLogDisk();
}

DiskPtr Changelog::getLatestLogDisk() const
{
    return keeper_context->getLatestLogDisk();
}

void Changelog::removeExistingLogs(ChangelogIter begin, ChangelogIter end)
{
    auto disk = getDisk();

    const auto timestamp_folder = (fs::path(changelogs_detached_dir) / getCurrentTimestampFolder()).generic_string();

    for (auto itr = begin; itr != end;)
    {
        auto & changelog_description = itr->second;

        if (!disk->existsDirectory(timestamp_folder))
        {
            LOG_WARNING(log, "Moving broken logs to {}", timestamp_folder);
            disk->createDirectories(timestamp_folder);
        }

        LOG_WARNING(log, "Removing changelog {}", changelog_description->path);
        const std::filesystem::path & path = changelog_description->path;
        const auto new_path = timestamp_folder / path.filename();

        auto changelog_disk = changelog_description->disk;
        if (changelog_disk == disk)
        {
            try
            {
                disk->moveFile(path.generic_string(), new_path.generic_string());
            }
            catch (const DB::Exception & e)
            {
                if (e.code() == DB::ErrorCodes::NOT_IMPLEMENTED)
                    moveChangelogBetweenDisks(changelog_disk, changelog_description, disk, new_path, keeper_context);
            }
        }
        else
            moveChangelogBetweenDisks(changelog_disk, changelog_description, disk, new_path, keeper_context);

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

    entry_storage.cleanAfter(start_to_remove_from_log_id - 1);
}

void Changelog::removeAllLogFilesBefore(uint64_t remove_before_log_start_index)
{
    auto end_to_remove_to_itr = existing_changelogs.lower_bound(remove_before_log_start_index);
    if (end_to_remove_to_itr == existing_changelogs.begin())
        return;

    /// Remove all changelogs that come before the specified index
    LOG_WARNING(log, "Removing changelogs that go before specified changelog entry");
    removeExistingLogs(existing_changelogs.begin(), end_to_remove_to_itr);
}

void Changelog::removeAllLogs()
{
    LOG_WARNING(log, "Removing all changelogs");
    removeExistingLogs(existing_changelogs.begin(), existing_changelogs.end());
    entry_storage.clear();
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
            LOG_INFO(log, "Raft server is not set in LogStore.");
    }
}

void Changelog::writeThread()
{
    /// The only consumer of an exception escaping this thread is the catch-all below, which calls
    /// `std::terminate`. Rotation allocates (the file buffer, and the zstd buffer when logs are
    /// compressed), so under memory pressure a refused allocation kills the process outright.
    LockMemoryExceptionInThread blocker{VariableContext::Global};

    WriteOperation write_operation;
    bool batch_append_ok = true;
    size_t pending_appends = 0;

    /// Flush request that we delay to batch it with more appends and to limit the flush frequency.
    /// A newer Flush request subsumes an older pending one: its index is not less,
    /// and one completion notification is enough for both.
    std::optional<Flush> pending_flush;

    /// We don't start a flush earlier than min_time_between_fsyncs_ms after the start of the previous flush.
    std::chrono::steady_clock::time_point earliest_next_flush_time{};

    const auto flush_logs = [&](const Flush & flush)
    {
        LOG_TEST(log, "Flushing {} logs", pending_appends);

        earliest_next_flush_time
            = std::chrono::steady_clock::now() + std::chrono::milliseconds(flush_settings.min_time_between_fsyncs_ms);

        {
            std::lock_guard writer_lock(writer_mutex);
            current_writer->flush();
        }

        {
            std::lock_guard lock{durable_idx_mutex};
            last_durable_idx = flush.index;
        }

        pending_appends = 0;
    };

    const auto notify_append_completion = [&]
    {
        durable_idx_cv.notify_all();

        // we need to call completion callback in another thread because it takes a global lock for the NuRaft server
        // NuRaft will in some places wait for flush to be done while having the same global lock leading to deadlock
        // -> future write operations are blocked by flush that cannot be completed because it cannot take NuRaft lock
        // -> NuRaft won't leave lock until its flush is done
        if (!append_completion_queue.push(batch_append_ok))
            LOG_WARNING(log, "Changelog is shut down");
    };

    try
    {
        /// NuRaft writes a batch of request by first calling multiple store requests, i.e. AppendLog
        /// finished by a flush request
        /// We assume that after some number of appends, we always get flush request
        while (true)
        {
            if (pending_flush)
            {
                bool do_flush = false;

                if (!batch_append_ok)
                {
                    /// An append failed, fail the flush without batching more operations.
                    do_flush = true;
                }
                else if (const auto now = std::chrono::steady_clock::now(); now < earliest_next_flush_time)
                {
                    /// Wait out the flush throttling interval, batching all appends that arrive in the meantime.
                    /// (The batch may exceed max_flush_batch_size since we can't flush earlier anyway.)
                    /// tryPop returns false either when the timeout expires or on shutdown; flush in both cases.
                    const auto timeout = std::chrono::ceil<std::chrono::milliseconds>(earliest_next_flush_time - now);
                    do_flush = !write_operations.tryPop(write_operation, timeout.count());
                }
                else
                {
                    /// Flush if we have the maximum allowed number of pending appends
                    /// or no more operations are immediately available for batching.
                    do_flush = pending_appends >= flush_settings.max_flush_batch_size || !write_operations.tryPop(write_operation);
                }

                if (do_flush)
                {
                    if (batch_append_ok)
                    {
                        flush_logs(*pending_flush);
                    }
                    else
                    {
                        std::lock_guard lock{durable_idx_mutex};
                        *pending_flush->failed = true;
                    }

                    notify_append_completion();
                    pending_flush.reset();
                    batch_append_ok = true;
                    continue;
                }
            }
            else if (!write_operations.pop(write_operation))
            {
                break;
            }

            chassert(initialized);

            if (auto * append_log = std::get_if<AppendLog>(&write_operation))
            {
                if (!batch_append_ok)
                    continue;

                std::lock_guard writer_lock(writer_mutex);
                chassert(current_writer);

                batch_append_ok = current_writer->appendRecord(buildRecord(append_log->index, append_log->log_entry));
                ++pending_appends;
            }
            else
            {
                pending_flush = std::get<Flush>(write_operation);
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Write thread failed, aborting");
        std::terminate();
    }
}


void Changelog::appendEntry(uint64_t index, const LogEntryPtr & log_entry)
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Changelog must be initialized before appending records");

    entry_storage.addEntry(index, log_entry);
    max_log_id.store(index, std::memory_order_relaxed);

    if (!write_operations.push(AppendLog{index, log_entry}))
        LOG_WARNING(log, "Changelog is shut down");
}

void Changelog::writeAt(uint64_t index, const LogEntryPtr & log_entry)
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Changelog must be initialized before writing records");

    /// wait for all appends to finish before changing active changelog file
    flush();

    {
        /// After flush(), last_durable_idx == old max_log_id. But we are about to
        /// truncate entries from 'index' onward and rewrite them. The new entries
        /// are not durable until the write thread fsyncs them, so we must decrease
        /// last_durable_idx to reflect that entries at 'index' and beyond are no
        /// longer durably persisted. Without this, the NuRaft follower durability
        /// loop would see the stale high value and skip waiting for the fsync.
        std::lock_guard lock{durable_idx_mutex};
        last_durable_idx = std::min(last_durable_idx, index - 1);
    }

    /// Superseded-segment removals; wait outside writer_mutex so writeThread is not pinned on unlinks.
    std::vector<ChangelogFileOperationPtr> pending_superseded_removes;

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

            description->waitAllAsyncOperations();
            /// if the changelog is broken at end, we cannot append it with new logs
            /// we create a new file starting with the required index
            if (description->broken_at_end)
            {
                LOG_INFO(log, "Cannot write into {} because it has broken changelog at end, rotating", description->path);
                current_writer->rotate(index);
            }
            else
            {
                auto log_disk = description->disk;
                auto latest_log_disk = getLatestLogDisk();
                if (log_disk != latest_log_disk)
                    moveChangelogBetweenDisks(log_disk, description, latest_log_disk, description->path, keeper_context);

                LOG_INFO(log, "Writing into {}", description->path);
                current_writer->setFile(std::move(description), WriteMode::Append);
            }

            /// Remove all subsequent files if overwritten something in previous one
            auto to_remove_itr = existing_changelogs.upper_bound(index);
            for (auto itr = to_remove_itr; itr != existing_changelogs.end();)
            {
                pending_superseded_removes.push_back(removeChangelogAsync(itr->second));
                itr = existing_changelogs.erase(itr);
            }
        }
    }

    /// Append the rewrite only after superseded changelog files are gone.
    for (const auto & op : pending_superseded_removes)
    {
        op->done.wait(false);
        if (auto error = op->getError())
        {
            tryLogException(
                std::move(error),
                log,
                fmt::format(
                    "Failed to remove a superseded changelog while rewriting at index {}. Terminating to avoid an inconsistent changelog state",
                    index),
                LogsLevel::fatal);
            std::terminate();
        }
    }

    /// Remove redundant logs from memory
    /// Everything >= index must be removed
    entry_storage.cleanAfter(index - 1);

    /// Now we can actually override entry at index
    appendEntry(index, log_entry);
}

void Changelog::compact(uint64_t up_to_log_index)
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Changelog must be initialized before compacting records");

    std::lock_guard lock(writer_mutex);
    LOG_INFO(log, "Compact logs up to log index {}, our max log id is {}", up_to_log_index, max_log_id.load(std::memory_order_relaxed));

    bool remove_all_logs = false;
    if (up_to_log_index > max_log_id.load(std::memory_order_relaxed))
    {
        LOG_INFO(log, "Seems like this node recovers from leaders snapshot, removing all logs");
        /// If we received snapshot from leader we may compact up to more fresh log
        max_log_id.store(up_to_log_index, std::memory_order_relaxed);
        remove_all_logs = true;
    }

    bool need_rotate = false;
    for (auto itr = existing_changelogs.begin(); itr != existing_changelogs.end();)
    {
        auto & changelog_description = *itr->second;
        auto path = changelog_description.getPathSafe();
        /// Remove all completely outdated changelog files
        if (remove_all_logs || changelog_description.to_log_index <= up_to_log_index)
        {
            if (current_writer && changelog_description.from_log_index == current_writer->getStartIndex())
            {
                LOG_INFO(
                    log,
                    "Trying to remove log {} which is current active log for write. Possibly this node recovers from snapshot",
                    path);
                need_rotate = true;
            }

            LOG_INFO(log, "Removing changelog {} because of compaction", path);
            removeChangelogAsync(itr->second);
            changelog_description.marked_as_deleted = true;

            itr = existing_changelogs.erase(itr);
        }
        else /// Files are ordered, so all subsequent should exist
            break;
    }

    entry_storage.cleanUpTo(up_to_log_index + 1);

    if (need_rotate)
        current_writer->rotate(up_to_log_index + 1);

    LOG_INFO(log, "Compaction up to {} finished new min index {}, new max index {}", up_to_log_index, getStartIndex(), max_log_id.load(std::memory_order_relaxed));
}

uint64_t Changelog::getNextEntryIndex() const
{
    return max_log_id.load(std::memory_order_relaxed) + 1;
}

uint64_t Changelog::getStartIndex() const
{
    return entry_storage.empty() ? max_log_id.load(std::memory_order_relaxed) + 1 : entry_storage.getFirstIndex();
}

LogEntryPtr Changelog::getLastEntry() const
{
    /// This entry treaded in special way by NuRaft
    static LogEntryPtr fake_entry = nuraft::cs_new<nuraft::log_entry>(0, nuraft::buffer::alloc(0));

    auto entry = entry_storage.getEntry(max_log_id.load(std::memory_order_relaxed));
    if (entry == nullptr)
        return fake_entry;

    return entry;
}


LogReadPlan Changelog::getReadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes)
{
    /// getStartIndex() handles the empty-store case: returns max_log_id+1 when empty (NOT 0)
    return entry_storage.getReadPlan(start, end, max_size_bytes, /*retained_start=*/getStartIndex());
}

LogEntriesPtr Changelog::executeReadPlan(const LogReadPlan & plan, uint64_t read_deadline_ms)
{
    return entry_storage.executeReadPlan(plan, read_deadline_ms);
}

LogReadPlan Changelog::getReadAheadPlan(uint64_t start, uint64_t end, int64_t max_size_bytes) const
{
    return entry_storage.getReadAheadPlan(start, end, max_size_bytes, getStartIndex());
}

LogEntriesPtr Changelog::serveReadAhead(int32_t reader_id, const LogReadPlan & plan)
{
    return entry_storage.serveReadAhead(reader_id, plan);
}

bool Changelog::isPeerReadAheadEnabled() const
{
    return entry_storage.isPeerReadAheadEnabled();
}

LogEntryPtr Changelog::entryFromMemory(uint64_t index) const
{
    return entry_storage.getEntryFromMemory(index);
}

LogEntryPtr Changelog::tryPopCommitReadAhead(uint64_t index)
{
    return entry_storage.tryPopCommitReadAhead(index);
}

LogReadPlan Changelog::getCommitReadPlan(uint64_t index) const
{
    return entry_storage.getCommitReadPlan(index, getStartIndex());
}

LogEntryPtr Changelog::serveCommitEntry(uint64_t index, const LogReadPlan & plan)
{
    return entry_storage.serveCommitEntry(index, plan);
}

uint64_t Changelog::currentTruncationEpoch() const
{
    return entry_storage.currentTruncationEpoch();
}

LogEntryPtr Changelog::entryAt(uint64_t index) const
{
    return entry_storage.getEntry(index);
}

LogEntryPtr Changelog::getLatestConfigChange() const
{
    return entry_storage.getLatestConfigChange();
}

nuraft::ptr<nuraft::buffer> Changelog::serializeEntriesToBuffer(uint64_t index, int32_t count)
{
    std::vector<nuraft::ptr<nuraft::buffer>> returned_logs;
    returned_logs.reserve(count);

    uint64_t size_total = 0;
    for (uint64_t i = index; i < index + count; ++i)
    {
        auto entry = entry_storage.getEntry(i);
        if (entry == nullptr)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Don't have log entry {}", i);

        nuraft::ptr<nuraft::buffer> buf = entry->serialize();
        size_total += buf->size();
        returned_logs.push_back(std::move(buf));
    }

    nuraft::ptr<nuraft::buffer> buf_out = nuraft::buffer::alloc(sizeof(int32_t) + count * sizeof(int32_t) + size_total);
    buf_out->pos(0);
    buf_out->put(count);

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
        if (i == 0 && cur_index >= entry_storage.getFirstIndex() && cur_index <= max_log_id.load(std::memory_order_relaxed))
            writeAt(cur_index, log_entry);
        else
            appendEntry(cur_index, log_entry);
    }
}

bool Changelog::isConfigLog(uint64_t index) const
{
    return entry_storage.isConfigLog(index);
}

uint64_t Changelog::termAt(uint64_t index) const
{
    return entry_storage.termAt(index);
}

bool Changelog::flush()
{
    if (auto failed_ptr = flushAsync())
    {
        std::unique_lock lock{durable_idx_mutex};
        durable_idx_cv.wait(lock, [&] { return *failed_ptr || last_durable_idx == max_log_id.load(std::memory_order_relaxed); });

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
    bool pushed = write_operations.push(Flush{max_log_id.load(std::memory_order_relaxed), failed});

    if (!pushed)
    {
        LOG_INFO(log, "Changelog is shut down");
        return nullptr;
    }

    entry_storage.refreshCache();
    return failed;
}

uint64_t Changelog::size() const
{
    return entry_storage.size();
}

void Changelog::shutdown()
{
    LOG_DEBUG(log, "Shutting down Changelog");
    if (!changelog_operation_queue.isFinished())
        changelog_operation_queue.finish();

    if (background_changelog_operations_thread->joinable())
        background_changelog_operations_thread->join();

    if (!write_operations.isFinished())
        write_operations.finish();

    if (write_thread->joinable())
        write_thread->join();

    if (!append_completion_queue.isFinished())
        append_completion_queue.finish();

    if (append_completion_thread->joinable())
        append_completion_thread->join();

    if (current_writer)
    {
        current_writer->finalize();
        current_writer.reset();
    }

    entry_storage.shutdown();
}

Changelog::~Changelog()
{
    if (initialized)
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

    try
    {
        shutdown();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void Changelog::backgroundChangelogOperationsThread()
{
    /// A failed removal here is stored and rethrown by `writeAt`, whose only handling is
    /// `std::terminate`. A blocker on the write thread cannot help: the exception is created here,
    /// and suppression applies where an exception is raised, not where it is rethrown.
    LockMemoryExceptionInThread blocker{VariableContext::Global};

    ChangelogFileOperationPtr changelog_operation;
    while (changelog_operation_queue.pop(changelog_operation))
    {
        if (std::holds_alternative<RemoveChangelog>(changelog_operation->operation))
        {
            chassert(changelog_operation->changelog);
            auto & changelog = *changelog_operation->changelog; /// mutable: we set removed_from_disk
            changelog.withWriteLock(
                [&]
                {
                    changelog.removed_from_disk = true; /// set BEFORE removeFile
                    FailPointInjection::pauseFailPoint(FailPoints::keeper_changelog_removed_from_disk_set);
                    try
                    {
                        changelog.disk->removeFile(changelog.path);
                        LOG_INFO(log, "Removed changelog {} because of compaction.", changelog.path);
                    }
                    catch (Exception & e)
                    {
                        LOG_WARNING(log, "Failed to remove changelog {} in compaction, error message: {}", changelog.path, e.message());
                        changelog_operation->setError(std::current_exception());
                    }
                    catch (...)
                    {
                        tryLogCurrentException(log);
                        changelog_operation->setError(std::current_exception());
                    }
                });
        }
        else if (auto * move_operation = std::get_if<MoveChangelog>(&changelog_operation->operation))
        {
            const auto & changelog = changelog_operation->changelog;

            if (move_operation->new_disk == changelog->disk)
            {
                if (move_operation->new_path != changelog->path)
                {
                    changelog->withWriteLock(
                        [&]
                        {
                            try
                            {
                                changelog->disk->moveFile(changelog->path, move_operation->new_path);
                            }
                            catch (...)
                            {
                                tryLogCurrentException(log, fmt::format("File rename failed on disk {}", changelog->disk->getName()));
                            }
                            changelog->path = std::move(move_operation->new_path);
                        });
                }
            }
            else
            {
                moveChangelogBetweenDisks(changelog->disk, changelog, move_operation->new_disk, move_operation->new_path, keeper_context);
            }
        }
        else
        {
            LOG_ERROR(log, "Unsupported operation detected for changelog {}", changelog_operation->changelog->path);
            chassert(false);
        }
        changelog_operation->done = true;
        /// Wake up `waitAllAsyncOperations`; a bare store does not wake an `std::atomic::wait`.
        changelog_operation->done.notify_all();
    }
}

void Changelog::modifyChangelogAsync(ChangelogFileOperationPtr changelog_operation)
{
    if (!changelog_operation_queue.tryPush(changelog_operation, 60 * 1000))
    {
        throw DB::Exception(
            ErrorCodes::SYSTEM_ERROR, "Background thread for changelog operations is stuck or not keeping up with operations");
    }

    changelog_operation->changelog->file_operations.push_back(changelog_operation);
}

ChangelogFileOperationPtr Changelog::removeChangelogAsync(ChangelogFileDescriptionPtr changelog)
{
    auto operation = std::make_shared<ChangelogFileOperation>(std::move(changelog), RemoveChangelog{});
    modifyChangelogAsync(operation);
    return operation;
}

void Changelog::moveChangelogAsync(ChangelogFileDescriptionPtr changelog, std::string new_path, DiskPtr new_disk)
{
    modifyChangelogAsync(
        std::make_shared<ChangelogFileOperation>(
            std::move(changelog), MoveChangelog{.new_path = std::move(new_path), .new_disk = std::move(new_disk)}));
}

void Changelog::setRaftServer(const nuraft::ptr<nuraft::raft_server> & raft_server_)
{
    chassert(raft_server_);
    raft_server = raft_server_;
}

bool Changelog::isInitialized() const
{
    return initialized;
}

void Changelog::getKeeperLogInfo(KeeperLogInfo & log_info) const
{
    if (!entry_storage.empty())
    {
        log_info.first_log_idx = getStartIndex();
        log_info.first_log_term = termAt(log_info.first_log_idx);

        log_info.last_log_idx = max_log_id.load(std::memory_order_relaxed);
        log_info.last_log_term = termAt(log_info.last_log_idx);
    }

    entry_storage.getKeeperLogInfo(log_info);
}

std::vector<KeeperChangelogStatus> Changelog::getChangelogsStatus() const
{
    std::lock_guard lock(writer_mutex);

    std::vector<KeeperChangelogStatus> result;
    result.reserve(existing_changelogs.size());

    ChangelogFileDescriptionPtr active_description;
    if (current_writer && current_writer->isFileSet())
        active_description = current_writer->getCurrentFileDescription();

    const uint64_t current_max_log_id = max_log_id.load(std::memory_order_relaxed);

    for (const auto & [from_index, description] : existing_changelogs)
    {
        chassert(description);

        const bool active = active_description && description == active_description;

        DiskPtr disk;
        String path;
        description->withReadLock(
            [&]
            {
                disk = description->disk;
                path = description->path;
            });
        const uint64_t to_log_index = description->to_log_index;

        std::optional<uint64_t> last_entry_index;
        if (active)
        {
            if (current_max_log_id >= description->from_log_index)
                last_entry_index = std::min(to_log_index, current_max_log_id);
        }
        else if (!description->broken_at_end)
        {
            last_entry_index = to_log_index;
        }

        result.push_back(KeeperChangelogStatus{
            .from_log_index = description->from_log_index,
            .to_log_index = to_log_index,
            .last_entry_index = last_entry_index,
            .path = std::move(path),
            .disk = std::move(disk),
            .is_compressed = description->is_compressed,
            .active = active,
            .is_broken = description->broken_at_end,
        });
    }

    return result;
}

}
