#include <exception>
#include <filesystem>
#include <mutex>
#include <ranges>
#include <variant>
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
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <Common/Exception.h>
#include <Common/SipHash.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>
#include <Common/ThreadPool.h>
#include <Common/ProfileEvents.h>
#include <Common/SharedLockGuard.h>
#include <libnuraft/log_val_type.hxx>
#include <libnuraft/log_entry.hxx>
#include <libnuraft/raft_server.hxx>

namespace ProfileEvents
{
    extern const Event KeeperLogsEntryReadFromLatestCache;
    extern const Event KeeperLogsEntryReadFromCommitCache;
    extern const Event KeeperLogsEntryReadFromFile;
    extern const Event KeeperLogsPrefetchedEntries;
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
            description->withLock(
                [&]
                {
                    description->disk = disk_to;
                    description->path = path_to;
                });
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
                assert(current_file_description);
                // if we wrote at least 1 log in the log file we can rename the file to reflect correctly the
                // contained logs
                // file can be deleted from disk earlier by compaction
                if (current_file_description->deleted)
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
                    /* compressi)on level = */ 3,
                    /* append_to_existing_file_ = */ mode == WriteMode::Append,
                    [latest_log_disk, path = current_file_description->path, read_settings = getReadSettings()] { return latest_log_disk->readFile(path, read_settings); });

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

        last_index_written = record.header.index;

        return true;
    }

    void flush()
    {
        auto * file_buffer = tryGetFileBaseBuffer();
        if (file_buffer)
        {
            /// Fsync file system if needed
            if (log_file_settings.force_sync)
                file_buffer->sync();
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
        chassert(!current_file_description->deleted);

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
    Checksum record_checksum;
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

LogEntryPtr getLogEntry(const CacheEntry & cache_entry)
{
    if (const auto * log_entry = std::get_if<LogEntryPtr>(&cache_entry))
        return *log_entry;

    const auto & prefetched_log_entry = std::get<PrefetchedCacheEntryPtr>(cache_entry);
    chassert(prefetched_log_entry);
    return prefetched_log_entry->getLogEntry();
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

PrefetchedCacheEntry::PrefetchedCacheEntry()
    : log_entry(log_entry_resolver.get_future())
{}

const LogEntryPtr & PrefetchedCacheEntry::getLogEntry() const
{
    return log_entry.get();
}

void PrefetchedCacheEntry::resolve(std::exception_ptr exception)
{
    log_entry_resolver.set_exception(exception);
}

void PrefetchedCacheEntry::resolve(LogEntryPtr log_entry_)
{
    log_entry_resolver.set_value(std::move(log_entry_));
}

LogEntryStorage::LogEntryStorage(const LogFileSettings & log_settings, KeeperContextPtr keeper_context_)
    : latest_logs_cache(log_settings.latest_logs_cache_size_threshold)
    , commit_logs_cache(log_settings.commit_logs_cache_size_threshold)
    , prefetch_queue(std::numeric_limits<uint64_t>::max())
    , keeper_context(std::move(keeper_context_))
    , log(getLogger("Changelog"))
{
    commit_logs_prefetcher = std::make_unique<ThreadFromGlobalPool>([this] { prefetchCommitLogs(); });
}

LogEntryStorage::~LogEntryStorage()
{
    shutdown();
}

void LogEntryStorage::prefetchCommitLogs()
{
    std::shared_ptr<PrefetchInfo> prefetch_info;
    while (prefetch_queue.pop(prefetch_info))
    {
        if (prefetch_info->cancel)
        {
            prefetch_info->done = true;
            prefetch_info->done.notify_all();
            continue;
        }

        auto current_index = prefetch_info->commit_prefetch_index_range.first;
        try
        {
            for (const auto & prefetch_file_info : prefetch_info->file_infos)
            {
                prefetch_file_info.file_description->withLock(
                    [&]
                    {
                        const auto & [changelog_description, position, count] = prefetch_file_info;
                        auto file = changelog_description->disk->readFile(changelog_description->path, getReadSettings());
                        file->seek(position, SEEK_SET);
                        LOG_TRACE(
                            log, "Prefetching {} log entries from path {}, from position {}", count, changelog_description->path, position);
                        ProfileEvents::increment(ProfileEvents::KeeperLogsPrefetchedEntries, count);

                        for (size_t i = 0; i < count; ++i)
                        {
                            if (prefetch_info->cancel)
                                break;

                            auto record = readChangelogRecord(*file, changelog_description->path);
                            auto entry = logEntryFromRecord(record);
                            if (current_index != record.header.index)
                                throw Exception(
                                    ErrorCodes::LOGICAL_ERROR,
                                    "Invalid index prefetched, expected {}, actual {}",
                                    current_index,
                                    record.header.index);

                            PrefetchedCacheEntryPtr prefetched_cache_entry;
                            {
                                SharedLockGuard lock(commit_logs_cache_mutex);
                                prefetched_cache_entry = commit_logs_cache.getPrefetchedCacheEntry(record.header.index);
                            }
                            prefetched_cache_entry->resolve(std::move(entry));
                            ++current_index;
                        }
                    });

                if (prefetch_info->cancel)
                    break;
            }
        }
        catch (...)
        {
            tryLogCurrentException(log, "While prefetching log entries");
            auto exception = std::current_exception();

            for (; current_index <= prefetch_info->commit_prefetch_index_range.second; ++current_index)
            {
                PrefetchedCacheEntryPtr prefetched_cache_entry;
                {
                    SharedLockGuard lock(commit_logs_cache_mutex);
                    prefetched_cache_entry = commit_logs_cache.getPrefetchedCacheEntry(current_index);
                }
                prefetched_cache_entry->resolve(exception);
            }
        }

        prefetch_info->done = true;
        prefetch_info->done.notify_all();
    }
}

void LogEntryStorage::startCommitLogsPrefetch(uint64_t last_committed_index) const
{
    if (keeper_context->isShutdownCalled())
        return;

    /// we don't start prefetch if there is no limit on latest logs cache
    if (latest_logs_cache.size_threshold == 0)
        return;

    /// commit logs is not empty and it's not next log
    if (!commit_logs_cache.empty() && commit_logs_cache.max_index_in_cache != last_committed_index)
        return;

    if (logs_location.empty())
        return;

    /// we are already prefetching some logs for commit
    if (current_prefetch_info && !current_prefetch_info->done)
        return;

    auto new_prefetch_info = std::make_shared<PrefetchInfo>();
    auto & [prefetch_from, prefetch_to] = new_prefetch_info->commit_prefetch_index_range;

    /// if there are no entries in commit cache we will start from the next log that will be committed
    /// otherwise we continue appending the commit cache from the latest entry stored in it
    size_t current_index = commit_logs_cache.empty() ? last_committed_index + 1 : commit_logs_cache.max_index_in_cache + 1;

    prefetch_from = current_index;

    size_t total_size = 0;
    std::vector<FileReadInfo> file_infos;
    FileReadInfo * current_file_info = nullptr;
    size_t next_position = 0;

    size_t max_index_for_prefetch = 0;
    if (!latest_logs_cache.empty())
        max_index_for_prefetch = latest_logs_cache.min_index_in_cache - 1;
    else
        max_index_for_prefetch = max_index_with_location;

    for (; current_index <= max_index_for_prefetch; ++current_index)
    {
        auto location_it = logs_location.find(current_index);
        if (location_it == logs_location.end())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Location of log entry with index {} is missing", current_index);

        const auto & [changelog_description, position, entry_size, size_in_file] = location_it->second;
        if (total_size == 0)
        {
            current_file_info = &file_infos.emplace_back(changelog_description, position, /* count */ 1);
            next_position = position + size_in_file;
        }
        else if (total_size + entry_size > commit_logs_cache.size_threshold)
            break;
        else if (changelog_description == current_file_info->file_description && position == next_position)
        {
            ++current_file_info->count;
            next_position += size_in_file;
        }
        else
        {
            current_file_info = &file_infos.emplace_back(changelog_description, position, /* count */ 1);
            next_position = position + size_in_file;
        }

        total_size += entry_size;
        commit_logs_cache.addEntry(current_index, entry_size, std::make_shared<PrefetchedCacheEntry>());
    }

    if (!file_infos.empty())
    {
        current_prefetch_info = std::move(new_prefetch_info);
        prefetch_to = current_index - 1;
        LOG_TRACE(log, "Will prefetch {} commit log entries [{} - {}]", prefetch_to - prefetch_from + 1, prefetch_from, prefetch_to);

        current_prefetch_info->file_infos = std::move(file_infos);
        auto inserted = prefetch_queue.push(current_prefetch_info);  /// NOLINT(clang-analyzer-deadcode.DeadStores)
        chassert(inserted);
    }
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

void LogEntryStorage::InMemoryCache::addEntry(uint64_t index, size_t size, CacheEntry log_entry)
{
    auto [_, inserted] = cache.emplace(index, std::move(log_entry));
    if (!inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert log with index {} which is already present in cache", index);

    updateStatsWithNewEntry(index, size);
}

void LogEntryStorage::InMemoryCache::addEntry(IndexToCacheEntryNode && node)
{
    auto index = node.key();
    auto entry_size = logEntrySize(getLogEntry(node.mapped()));

    auto result = cache.insert(std::move(node));
    if (!result.inserted)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to insert log with index {} which is already present in cache", index);

    updateStatsWithNewEntry(index, entry_size);
}

IndexToCacheEntryNode LogEntryStorage::InMemoryCache::popOldestEntry()
{
    auto node = cache.extract(min_index_in_cache);
    if (node.empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Couldn't find the oldest entry of index {} in logs cache", min_index_in_cache);
    ++min_index_in_cache;
    cache_size -= logEntrySize(getLogEntry(node.mapped()));
    return node;
}

bool LogEntryStorage::InMemoryCache::containsEntry(uint64_t index) const
{
    return !cache.empty() && index >= min_index_in_cache && index <= max_index_in_cache;
}

CacheEntry * LogEntryStorage::InMemoryCache::getCacheEntry(uint64_t index)
{
    if (!containsEntry(index))
        return nullptr;

    auto it = cache.find(index);
    if (it == cache.end())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Index {} missing from cache while it should be present", index);

    return &it->second;
}

const CacheEntry * LogEntryStorage::InMemoryCache::getCacheEntry(uint64_t index) const
{
    return const_cast<InMemoryCache &>(*this).getCacheEntry(index);
}

PrefetchedCacheEntryPtr LogEntryStorage::InMemoryCache::getPrefetchedCacheEntry(uint64_t index)
{
    auto * cache_entry = getCacheEntry(index);
    if (cache_entry == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Missing expected index {} in cache", index);

    return std::get<PrefetchedCacheEntryPtr>(*cache_entry);
}


LogEntryPtr LogEntryStorage::InMemoryCache::getEntry(uint64_t index) const
{
    const auto * cache_entry = getCacheEntry(index);
    if (cache_entry == nullptr)
        return nullptr;

    return getLogEntry(*cache_entry);
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

        cache_size -= logEntrySize(getLogEntry(it->second));
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

        cache_size -= logEntrySize(getLogEntry(it->second));
        cache.erase(it);
    }

    max_index_in_cache = index;
}

void LogEntryStorage::InMemoryCache::clear()
{
    cache.clear();
    cache_size = 0;
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
    return size_threshold == 0 || empty() || cache_size + log_entry_size < size_threshold;
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

bool LogEntryStorage::shouldMoveLogToCommitCache(uint64_t index, size_t log_entry_size)
{
    /// if commit logs cache is empty, we need it only if it's the next log to commit
    if (commit_logs_cache.empty())
        return keeper_context->lastCommittedIndex() + 1 == index;

    return commit_logs_cache.max_index_in_cache == index - 1 && commit_logs_cache.hasSpaceAvailable(log_entry_size);
}

void LogEntryStorage::updateTermInfoWithNewEntry(uint64_t index, uint64_t term)
{
    if (!log_term_infos.empty() && log_term_infos.back().term == term)
        return;

    log_term_infos.push_back(LogTermInfo{.term = term, .first_index = index});
}

void LogEntryStorage::addEntryWithLocation(uint64_t index, const LogEntryPtr & log_entry, LogLocation log_location)
{
    auto entry_size = logEntrySize(log_entry);
    while (!latest_logs_cache.hasSpaceAvailable(entry_size))
    {
        auto entry_handle = latest_logs_cache.popOldestEntry();
        size_t removed_entry_size = logEntrySize(getLogEntry(entry_handle.mapped()));
        {
            std::lock_guard lock(commit_logs_cache_mutex);
            if (shouldMoveLogToCommitCache(entry_handle.key(), removed_entry_size))
                commit_logs_cache.addEntry(std::move(entry_handle));
        }
    }
    latest_logs_cache.addEntry(index, entry_size, CacheEntry(log_entry));

    logs_location.emplace(index, std::move(log_location));

    if (logs_location.size() == 1)
        min_index_with_location = index;

    max_index_with_location = index;

    if (log_entry->get_val_type() == nuraft::conf)
    {
        latest_config = log_entry;
        latest_config_index = index;
        logs_with_config_changes.insert(index);
    }

    updateTermInfoWithNewEntry(index, log_entry->get_term());
}

void LogEntryStorage::cleanUpTo(uint64_t index)
{
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

    /// uncommitted logs should be compacted only if we received snapshot from leader
    if (current_prefetch_info && !current_prefetch_info->done)
    {
        auto [prefetch_from, prefetch_to] = current_prefetch_info->commit_prefetch_index_range;
        /// if we will clean some logs that are currently prefetched, stop prefetching
        /// and clean all logs from it
        if (index > prefetch_from)
        {
            current_prefetch_info->cancel = true;
            current_prefetch_info->done.wait(false);
        }

        std::lock_guard lock(commit_logs_cache_mutex);
        if (index > prefetch_from)
            commit_logs_cache.clear();

        /// start prefetching logs for committing at the current index
        /// the last log index in the snapshot should be the
        /// last log we cleaned up
        startCommitLogsPrefetch(index - 1);
    }
    else
    {
        std::lock_guard lock(commit_logs_cache_mutex);
        commit_logs_cache.cleanUpTo(index);
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
    latest_logs_cache.cleanAfter(index);

    if (!logs_location.empty() && index < max_index_with_location)
    {
        if (index < min_index_with_location)
        {
            logs_location.clear();
        }
        else
        {
            for (size_t i = index + 1; i <= max_index_with_location; ++i)
            {
                auto it = logs_location.find(i);
                if (it == logs_location.end())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Log entry with index {} unexpectedly missing from logs location", i);

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

    /// if we cleared all latest logs, there is a possibility we would need to clear commit logs
    if (latest_logs_cache.empty())
    {
        std::lock_guard lock(commit_logs_cache_mutex);
        /// we will clean everything after the index, if there is a prefetch in progress
        /// wait until we fetch everything until index
        /// afterwards we can stop prefetching of newer logs because they will be cleaned up
        commit_logs_cache.getEntry(index);
        if (current_prefetch_info && !current_prefetch_info->done)
        {
            auto [prefetch_from, prefetch_to] = current_prefetch_info->commit_prefetch_index_range;
            /// if we will clean some logs that are currently prefetched, stop prefetching
            if (index < prefetch_to)
            {
                current_prefetch_info->cancel = true;
                current_prefetch_info->done.wait(false);
            }
        }

        commit_logs_cache.cleanAfter(index);
        startCommitLogsPrefetch(keeper_context->lastCommittedIndex());
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
}

bool LogEntryStorage::contains(uint64_t index) const
{
    return logs_location.contains(index) || latest_logs_cache.containsEntry(index);
}

LogEntryPtr LogEntryStorage::getEntry(uint64_t index) const
{
    auto last_committed_index = keeper_context->lastCommittedIndex();
    {
        std::lock_guard lock(commit_logs_cache_mutex);
        commit_logs_cache.cleanUpTo(last_committed_index);
        startCommitLogsPrefetch(last_committed_index);
    }

    LogEntryPtr entry = nullptr;

    if (latest_config != nullptr && index == latest_config_index)
        return latest_config;

    if (first_log_entry != nullptr && index == first_log_index)
        return first_log_entry;

    if (auto entry_from_latest_cache = latest_logs_cache.getEntry(index))
    {
        ProfileEvents::increment(ProfileEvents::KeeperLogsEntryReadFromLatestCache);
        return entry_from_latest_cache;
    }

    {
        SharedLockGuard lock(commit_logs_cache_mutex);
        if (auto entry_from_commit_cache = commit_logs_cache.getEntry(index))
        {
            ProfileEvents::increment(ProfileEvents::KeeperLogsEntryReadFromCommitCache);
            return entry_from_commit_cache;
        }
    }

    if (auto it = logs_location.find(index); it != logs_location.end())
    {
        it->second.file_description->withLock(
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

    {
        std::lock_guard lock(commit_logs_cache_mutex);
        commit_logs_cache.clear();
    }

    logs_location.clear();
}

LogEntryPtr LogEntryStorage::getLatestConfigChange() const
{
    return latest_config;
}

uint64_t LogEntryStorage::termAt(uint64_t index) const
{
    uint64_t term_for_index = 0;
    for (const auto [term, first_index] : log_term_infos)
    {
        if (index < first_index)
            return term_for_index;

        term_for_index = term;
    }

    return term_for_index;
}

void LogEntryStorage::addLogLocations(std::vector<std::pair<uint64_t, LogLocation>> && indices_with_log_locations)
{
    /// if we have unlimited space in latest logs cache we don't need log location
    if (latest_logs_cache.size_threshold == 0)
        return;

    std::lock_guard lock(logs_location_mutex);
    unapplied_indices_with_log_locations.insert(
        unapplied_indices_with_log_locations.end(),
        std::make_move_iterator(indices_with_log_locations.begin()),
        std::make_move_iterator(indices_with_log_locations.end()));
}

void LogEntryStorage::refreshCache()
{
    /// if we have unlimited space in latest logs cache we don't need log location
    if (latest_logs_cache.size_threshold == 0)
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

        logs_location.emplace(index, std::move(log_location));
        max_index_with_location = index;
    }

    if (logs_location.empty())
        return;

    std::lock_guard lock(commit_logs_cache_mutex);
    while (latest_logs_cache.numberOfEntries() > 1 && latest_logs_cache.min_index_in_cache <= max_index_with_location
           && latest_logs_cache.cache_size > latest_logs_cache.size_threshold)
    {
        auto node = latest_logs_cache.popOldestEntry();
        auto log_entry_size = logEntrySize(getLogEntry(node.mapped()));
        if (shouldMoveLogToCommitCache(node.key(), log_entry_size))
            commit_logs_cache.addEntry(std::move(node));
    }
}

LogEntriesPtr LogEntryStorage::getLogEntriesBetween(uint64_t start, uint64_t end) const
{
    LogEntriesPtr ret = nuraft::cs_new<std::vector<nuraft::ptr<nuraft::log_entry>>>();
    ret->reserve(end - start);

    /// we rely on fact that changelogs need to be written sequentially with
    /// no other writes between
    std::optional<FileReadInfo> read_info;
    size_t next_position = 0;
    const auto set_new_file = [&](const auto & log_location)
    {
        read_info.emplace();
        read_info->file_description = log_location.file_description;
        read_info->position = log_location.position;
        read_info->count = 1;
        next_position = log_location.position + log_location.size_in_file;
    };

    const auto flush_file = [&]
    {
        if (!read_info)
            return;

        read_info->file_description->withLock(
            [&]
            {
                const auto & [file_description, start_position, count] = *read_info;
                LOG_TRACE(log, "Reading from path {} {} entries", file_description->path, read_info->count);
                auto file = file_description->disk->readFile(file_description->path, getReadSettings());
                file->seek(start_position, SEEK_SET);

                for (size_t i = 0; i < count; ++i)
                {
                    auto record = readChangelogRecord(*file, file_description->path);
                    ret->push_back(logEntryFromRecord(record));
                    ProfileEvents::increment(ProfileEvents::KeeperLogsEntryReadFromFile);
                }
            });

        read_info.reset();
    };

    SharedLockGuard commit_logs_lock(commit_logs_cache_mutex);
    for (size_t i = start; i < end; ++i)
    {
        if (auto commit_cache_entry = commit_logs_cache.getEntry(i))
        {
            flush_file();
            ret->push_back(std::move(commit_cache_entry));
        }
        else if (auto latest_cache_entry = latest_logs_cache.getEntry(i))
        {
            flush_file();
            ret->push_back(std::move(latest_cache_entry));
        }
        else
        {
            auto location_it = logs_location.find(i);
            if (location_it == logs_location.end())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Location of log entry with index {} is missing", i);

            const auto & log_location = location_it->second;

            if (!read_info)
                set_new_file(log_location);
            else if (read_info->file_description == log_location.file_description && next_position == log_location.position)
            {
                ++read_info->count;
                next_position += log_location.size_in_file;
            }
            else
            {
                flush_file();
                set_new_file(log_location);
            }
        }
    }

    flush_file();
    return ret;
}

void LogEntryStorage::getKeeperLogInfo(KeeperLogInfo & log_info) const
{
    log_info.latest_logs_cache_entries = latest_logs_cache.numberOfEntries();
    log_info.latest_logs_cache_size = latest_logs_cache.cache_size;

    SharedLockGuard lock(commit_logs_cache_mutex);
    log_info.commit_logs_cache_entries = commit_logs_cache.numberOfEntries();
    log_info.commit_logs_cache_size = commit_logs_cache.cache_size;
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

void LogEntryStorage::shutdown()
{
    if (std::exchange(is_shutdown, true))
        return;

    if (!prefetch_queue.isFinished())
        prefetch_queue.finish();

    if (current_prefetch_info)
    {
        current_prefetch_info->cancel = true;
        current_prefetch_info->done.wait(false);
    }

    if (commit_logs_prefetcher->joinable())
        commit_logs_prefetcher->join();
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
    LogEntryStorage entry_storage{log_file_settings, keeper_context};
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
    LoggerPtr log_, LogFileSettings log_file_settings, FlushSettings flush_settings_, KeeperContextPtr keeper_context_)
    : changelogs_detached_dir("detached")
    , rotate_interval(log_file_settings.rotate_interval)
    , compress_logs(log_file_settings.compress_logs)
    , log(log_)
    , entry_storage(log_file_settings, keeper_context_)
    , write_operations(std::numeric_limits<size_t>::max())
    , append_completion_queue(std::numeric_limits<size_t>::max())
    , keeper_context(std::move(keeper_context_))
    , flush_settings(flush_settings_)
{
    try
    {
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
try
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

    uint64_t last_read_index = 0;

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
                    max_log_id = last_commited_log_index == 0 ? 0 : last_commited_log_index - 1;
                    current_writer->rotate(max_log_id + 1);
                    initialized = true;
                    return;
                }
                if (changelog_description.from_log_index > start_to_read_from)
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
            else if (changelog_description.from_log_index > last_read_index && (changelog_description.from_log_index - last_read_index) > 1)
            {
                if (!last_log_read_result->error)
                {
                    LOG_ERROR(
                        log,
                        "Some records were lost, last found log index {}, while the next log index on disk is {}. Hopefully will receive "
                        "missing records from leader.",
                        last_read_index,
                        changelog_description.from_log_index);
                    removeAllLogsAfter(last_log_read_result->log_start_index);
                }
                break;
            }

            ChangelogReader reader(changelog_description_ptr);
            last_log_read_result = reader.readChangelog(entry_storage, start_to_read_from, log);

            if (last_log_read_result->last_read_index != 0)
                last_read_index = last_log_read_result->last_read_index;

            last_log_read_result->log_start_index = changelog_description.from_log_index;

            if (last_log_read_result->last_read_index != 0)
                max_log_id = last_log_read_result->last_read_index;

            /// How many entries we have in the last changelog
            uint64_t log_count = changelog_description.expectedEntriesCountInLog();

            /// Unfinished log
            last_log_is_not_complete = last_log_read_result->error || last_log_read_result->total_entries_read_from_log < log_count;
        }
    }

    const auto move_from_latest_logs_disks = [&](auto & description)
    {
        /// check if we need to move completed log to another disk
        auto latest_log_disk = getLatestLogDisk();
        auto disk = getDisk();

        if (latest_log_disk != disk && latest_log_disk == description->disk)
            moveChangelogBetweenDisks(latest_log_disk, description, disk, description->path, keeper_context);
    };

    /// we can have empty log (with zero entries) and last_log_read_result will be initialized
    if (!last_log_read_result || entry_storage.empty()) /// We just may have no logs (only snapshot or nothing)
    {
        /// Just to be sure they don't exist
        removeAllLogs();
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
        max_log_id = last_commited_log_index - 1;
    }
    else if (last_log_is_not_complete) /// if it's complete just start new one
    {
        assert(last_log_read_result != std::nullopt);
        assert(!existing_changelogs.empty());

        /// Continue to write into incomplete existing log if it didn't finish with error
        auto & description = existing_changelogs[last_log_read_result->log_start_index];

        const auto remove_invalid_logs = [&]
        {
            /// Actually they shouldn't exist, but to be sure we remove them
            removeAllLogsAfter(last_log_read_result->log_start_index);

            /// This log, even if it finished with error shouldn't be removed
            chassert(existing_changelogs.contains(last_log_read_result->log_start_index));
            chassert(existing_changelogs.find(last_log_read_result->log_start_index)->first == existing_changelogs.rbegin()->first);
        };

        if (last_log_read_result->last_read_index == 0) /// If it's broken or empty log then remove it
        {
            LOG_INFO(log, "Removing chagelog {} because it's empty", description->path);
            remove_invalid_logs();
            description->disk->removeFile(description->path);
            existing_changelogs.erase(last_log_read_result->log_start_index);
            entry_storage.cleanAfter(last_log_read_result->log_start_index - 1);
        }
        else if (last_log_read_result->error)
        {
            LOG_INFO(log, "Changelog {} read finished with error but some logs were read from it, file will not be removed", description->path);
            remove_invalid_logs();
            entry_storage.cleanAfter(last_log_read_result->last_read_index);
            description->broken_at_end = true;
            move_from_latest_logs_disks(description);
        }
        /// don't mix compressed and uncompressed writes
        else if (compress_logs == last_log_read_result->compressed_log)
        {
            initWriter(description);
        }
    }
    else if (last_log_read_result.has_value())
    {
        move_from_latest_logs_disks(existing_changelogs.at(last_log_read_result->log_start_index));
    }

    /// Start new log if we don't initialize writer from previous log. All logs can be "complete".
    if (!current_writer->isFileSet())
        current_writer->rotate(max_log_id + 1);

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

    initialized = true;
}
catch (...)
{
    tryLogCurrentException(__PRETTY_FUNCTION__);
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
    WriteOperation write_operation;
    bool batch_append_ok = true;
    size_t pending_appends = 0;
    bool try_batch_flush = false;

    const auto flush_logs = [&](const auto & flush)
    {
        LOG_TEST(log, "Flushing {} logs", pending_appends);

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
            if (try_batch_flush)
            {
                try_batch_flush = false;
                /// we have Flush request stored in write operation
                /// but we try to get new append operations
                /// if there are none, we apply the currently set Flush
                chassert(std::holds_alternative<Flush>(write_operation));
                if (!write_operations.tryPop(write_operation))
                {
                    chassert(batch_append_ok);
                    const auto & flush = std::get<Flush>(write_operation);
                    flush_logs(flush);
                    notify_append_completion();
                    if (!write_operations.pop(write_operation))
                        break;
                }
            }
            else if (!write_operations.pop(write_operation))
            {
                break;
            }

            assert(initialized);

            if (auto * append_log = std::get_if<AppendLog>(&write_operation))
            {
                if (!batch_append_ok)
                    continue;

                std::lock_guard writer_lock(writer_mutex);
                assert(current_writer);

                batch_append_ok = current_writer->appendRecord(buildRecord(append_log->index, append_log->log_entry));
                ++pending_appends;
            }
            else
            {
                const auto & flush = std::get<Flush>(write_operation);

                if (batch_append_ok)
                {
                    /// we can try batching more logs for flush
                    if (pending_appends < flush_settings.max_flush_batch_size)
                    {
                        try_batch_flush = true;
                        continue;
                    }
                    /// we need to flush because we have maximum allowed pending records
                    flush_logs(flush);
                }
                else
                {
                    std::lock_guard lock{durable_idx_mutex};
                    *flush.failed = true;
                }
                notify_append_completion();
                batch_append_ok = true;
            }
        }
    }
    catch (...)
    {
        tryLogCurrentException(log, "Write thread failed, aborting");
        std::abort();
    }
}


void Changelog::appendEntry(uint64_t index, const LogEntryPtr & log_entry)
{
    if (!initialized)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Changelog must be initialized before appending records");

    entry_storage.addEntry(index, log_entry);
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
                removeChangelogAsync(itr->second);
                itr = existing_changelogs.erase(itr);
            }
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
            changelog_description.deleted = true;

            itr = existing_changelogs.erase(itr);
        }
        else /// Files are ordered, so all subsequent should exist
            break;
    }

    entry_storage.cleanUpTo(up_to_log_index + 1);

    if (need_rotate)
        current_writer->rotate(up_to_log_index + 1);

    LOG_INFO(log, "Compaction up to {} finished new min index {}, new max index {}", up_to_log_index, getStartIndex(), max_log_id);
}

uint64_t Changelog::getNextEntryIndex() const
{
    return max_log_id + 1;
}

uint64_t Changelog::getStartIndex() const
{
    return entry_storage.empty() ? max_log_id + 1 : entry_storage.getFirstIndex();
}

LogEntryPtr Changelog::getLastEntry() const
{
    /// This entry treaded in special way by NuRaft
    static LogEntryPtr fake_entry = nuraft::cs_new<nuraft::log_entry>(0, nuraft::buffer::alloc(0));

    auto entry = entry_storage.getEntry(max_log_id);
    if (entry == nullptr)
        return fake_entry;

    return entry;
}

LogEntriesPtr Changelog::getLogEntriesBetween(uint64_t start, uint64_t end)
{
    return entry_storage.getLogEntriesBetween(start, end);
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
        if (i == 0 && cur_index >= entry_storage.getFirstIndex() && cur_index <= max_log_id)
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
    try
    {
        flush();
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
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
    ChangelogFileOperationPtr changelog_operation;
    while (changelog_operation_queue.pop(changelog_operation))
    {
        if (std::holds_alternative<RemoveChangelog>(changelog_operation->operation))
        {
            chassert(changelog_operation->changelog);
            const auto & changelog = *changelog_operation->changelog;
            try
            {
                changelog.disk->removeFile(changelog.path);
                LOG_INFO(log, "Removed changelog {} because of compaction.", changelog.path);
            }
            catch (Exception & e)
            {
                LOG_WARNING(log, "Failed to remove changelog {} in compaction, error message: {}", changelog.path, e.message());
            }
            catch (...)
            {
                tryLogCurrentException(log);
            }
        }
        else if (auto * move_operation = std::get_if<MoveChangelog>(&changelog_operation->operation))
        {
            const auto & changelog = changelog_operation->changelog;

            if (move_operation->new_disk == changelog->disk)
            {
                if (move_operation->new_path != changelog->path)
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

void Changelog::removeChangelogAsync(ChangelogFileDescriptionPtr changelog)
{
    modifyChangelogAsync(std::make_shared<ChangelogFileOperation>(std::move(changelog), RemoveChangelog{}));
}

void Changelog::moveChangelogAsync(ChangelogFileDescriptionPtr changelog, std::string new_path, DiskPtr new_disk)
{
    modifyChangelogAsync(
        std::make_shared<ChangelogFileOperation>(
            std::move(changelog), MoveChangelog{.new_path = std::move(new_path), .new_disk = std::move(new_disk)}));
}

void Changelog::setRaftServer(const nuraft::ptr<nuraft::raft_server> & raft_server_)
{
    assert(raft_server_);
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

        log_info.last_log_idx = max_log_id;
        log_info.last_log_term = termAt(log_info.last_log_idx);
    }

    entry_storage.getKeeperLogInfo(log_info);
}

}
