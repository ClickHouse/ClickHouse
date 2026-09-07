#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>

#include <array>
#include <atomic>
#include <filesystem>
#include <tuple>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/BlobStorageLog.h>
#include <Interpreters/Context.h>
#include <base/scope_guard.h>
#include <Common/BlobStorageLogWriter.h>
#include <Common/ObjectStorageKeyGenerator.h>
#include <Common/StackTrace.h>
#include <Common/Stopwatch.h>
#include <Common/filesystemHelpers.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>
#include <Common/FailPoint.h>
#include <Common/ErrnoException.h>

namespace fs = std::filesystem;

namespace DB
{

namespace FailPoints
{
    extern const char local_object_storage_network_error_during_remove[];
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_UNLINK;
    extern const int CANNOT_RMDIR;
    extern const int READONLY;
    extern const int FAULT_INJECTED;
    extern const int FILE_ALREADY_EXISTS;
    extern const int FILE_DOESNT_EXIST;
    extern const int CANNOT_OPEN_FILE;
    extern const int CANNOT_LINK;
    extern const int CANNOT_STAT;
    extern const int STALE_VERSION;
    extern const int SYSTEM_ERROR;
    extern const int PATH_ACCESS_DENIED;
}

namespace
{

struct timespec getMTime(const struct stat & file_stat)
{
#if defined(OS_DARWIN)
    return file_stat.st_mtimespec;
#else
    return file_stat.st_mtim;
#endif
}

bool isLaterThan(const struct timespec & left, const struct timespec & right)
{
    return std::tie(left.tv_sec, left.tv_nsec) > std::tie(right.tv_sec, right.tv_nsec);
}

String makeETag(const struct stat & file_stat)
{
    const auto mtime = getMTime(file_stat);
    return fmt::format(
        "{}.{:09}_{}_{}",
        static_cast<Int64>(mtime.tv_sec),
        static_cast<Int64>(mtime.tv_nsec),
        static_cast<Int64>(file_stat.st_ino),
        static_cast<Int64>(file_stat.st_size));
}

ObjectMetadata makeObjectMetadata(const struct stat & file_stat)
{
    ObjectMetadata object_metadata;
    object_metadata.size_bytes = file_stat.st_size;
    object_metadata.etag = makeETag(file_stat);
    object_metadata.last_modified = Poco::Timestamp::fromEpochTime(file_stat.st_mtime);
    return object_metadata;
}

/// The concurrent-disappearance class for a best-effort local listing: an entry
/// removed mid-stat (ENOENT) or whose parent path component was concurrently
/// replaced by a non-directory (ENOTDIR). Mirrors libc++'s own `__is_dne_error`.
/// Every other error (EACCES, EIO, ...) is a real failure and must propagate.
bool isVanishedEntryError(const std::error_code & error)
{
    return error == std::errc::no_such_file_or_directory || error == std::errc::not_a_directory;
}

/// Best-effort metadata stat for a path that has ALREADY been validated against
/// the key prefix. Uses the non-throwing `error_code` overloads and tolerates the
/// concurrent-disappearance class (see `isVanishedEntryError`), returning an empty
/// optional for a vanished entry. Every other error is propagated.
std::optional<ObjectMetadata> tryStatResolvedPath(const std::string & resolved_path)
{
    struct stat file_stat{};
    if (0 != ::stat(resolved_path.c_str(), &file_stat))
    {
        std::error_code error(errno, std::generic_category());
        if (isVanishedEntryError(error))
            return {};
        throw fs::filesystem_error("Got unexpected error while getting file metadata", resolved_path, error);
    }

    return makeObjectMetadata(file_stat);
}


}

LocalObjectStorage::LocalObjectStorage(LocalObjectStorageSettings settings_)
    : settings(std::move(settings_))
    , log(getLogger("LocalObjectStorage"))
{
    if (auto block_device_id = tryGetBlockDeviceId("/"); block_device_id.has_value())
        description = *block_device_id;
    else
        description = "/";

    if (!settings.read_only)
        fs::create_directories(settings.key_prefix);
}

String resolvePathRelativelyToBase(const String & path, const String & base_path)
{
    auto configured_base = fs::path(base_path).lexically_normal();

    auto is_inside = [&](const String & candidate)
    {
        return fileOrSymlinkPathStartsWith(candidate, configured_base.string())
            && pathStartsWith(candidate, configured_base.string());
    };

    if (is_inside(path))
        return path;

    auto combined = (configured_base / path).lexically_normal().string();
    if (is_inside(combined))
        return combined;

    auto path_canonical = fs::weakly_canonical(fs::path(path).lexically_normal());
    throw Exception(
        ErrorCodes::PATH_ACCESS_DENIED,
        "Path `{}` which was canonicalized to `{}` is outside the table path directory : `{}`",
        path,
        path_canonical.string(),
        configured_base.string());
}

String LocalObjectStorage::resolvePathRelativelyToKeyPrefix(const String & path) const
{
    return resolvePathRelativelyToBase(path, settings.key_prefix);
}

bool LocalObjectStorage::exists(const StoredObject & object) const
{
    auto resolved_path = resolvePathRelativelyToKeyPrefix(object.remote_path);
    return fs::exists(resolved_path);
}

ReadSettings LocalObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    auto modified_settings{read_settings};
    /// Other options might break assertions in AsynchronousBoundedReadBuffer.
    modified_settings.local_fs_settings.method = LocalFSReadMethod::pread;
    modified_settings.local_fs_settings.direct_io_threshold = 0; /// Disable.
    return IObjectStorage::patchSettings(modified_settings);
}

namespace
{

/// Wrapper around a read buffer that adds blob storage logging in the destructor.
///
/// Local reads (and HDFS) have no discrete "API call" boundary like S3 `GetObject` or
/// Azure `Download`, so we aggregate `elapsed_microseconds` and `bytes_read` across all
/// `nextImpl`/`readBigAt` calls and emit a single `Read` event per buffer lifetime.
/// S3 and Azure log each request/attempt separately as time-to-first-byte instead.
class ReadBufferFromFileWithLogging final : public ReadBufferFromFileDecorator
{
public:
    ReadBufferFromFileWithLogging(
        std::unique_ptr<ReadBufferFromFileBase> impl_,
        const String & file_path_,
        const String & bucket_,
        BlobStorageLogWriterPtr blob_log_)
        : ReadBufferFromFileDecorator(std::move(impl_))
        , file_path(file_path_)
        , bucket(bucket_)
        , blob_log(std::move(blob_log_))
    {
    }

    ~ReadBufferFromFileWithLogging() override
    {
        /// The destructor is implicitly `noexcept`. `addEvent` is potentially throwing
        /// (e.g. allocations inside `SystemLogQueue::push`), so wrap it in `try/catch`
        /// to avoid `std::terminate` if it throws during stack unwinding.
        if (blob_log && read_attempted && !read_failed)
        {
            try
            {
                blob_log->addEvent(
                    BlobStorageLogElement::EventType::Read,
                    /* bucket */ bucket,
                    /* remote_path */ file_path,
                    /* local_path */ {},
                    /* data_size */ bytes_read,
                    elapsed_microseconds,
                    /* error_code */ 0,
                    /* error_message */ {});
            }
            catch (...)
            {
                tryLogCurrentException(__PRETTY_FUNCTION__);
            }
        }
    }

    std::string getFileName() const override { return file_path; }

    bool supportsReadAt() override { return impl->supportsReadAt(); }

    size_t readBigAt(char * to, size_t n, size_t offset, const std::function<bool(size_t)> & progress_callback) const override
    {
        Stopwatch watch;
        read_attempted = true;
        try
        {
            size_t result = impl->readBigAt(to, n, offset, progress_callback);
            elapsed_microseconds += watch.elapsedMicroseconds();
            bytes_read += result;
            return result;
        }
        catch (...)
        {
            read_failed = true;
            throw;
        }
    }

    /// Forward methods that the base ReadBufferFromFileDecorator does not delegate.
    /// These are used when our wrapper is plugged into ReadBufferFromRemoteFSGather
    /// or AsynchronousBoundedReadBuffer (e.g. for the local_blob_storage disk type).
    size_t getFileOffsetOfBufferEnd() const override { return impl->getFileOffsetOfBufferEnd(); }
    void setReadUntilPosition(size_t position) override { impl->setReadUntilPosition(position); }
    void setReadUntilEnd() override { impl->setReadUntilEnd(); }
    bool supportsRightBoundedReads() const override { return impl->supportsRightBoundedReads(); }
    bool isSeekCheap() override { return impl->isSeekCheap(); }
    bool isContentCached(size_t offset, size_t size) override { return impl->isContentCached(offset, size); }

private:
    bool nextImpl() override
    {
        Stopwatch next_watch;
        read_attempted = true;
        try
        {
            bool result = ReadBufferFromFileDecorator::nextImpl();
            elapsed_microseconds += next_watch.elapsedMicroseconds();
            if (result)
                bytes_read += working_buffer.size();
            return result;
        }
        catch (...)
        {
            read_failed = true;
            throw;
        }
    }

    const String file_path;
    const String bucket;
    BlobStorageLogWriterPtr blob_log;
    mutable std::atomic<size_t> elapsed_microseconds = 0;
    mutable std::atomic<size_t> bytes_read = 0;
    mutable std::atomic<bool> read_attempted = false;
    mutable std::atomic<bool> read_failed = false;
};

/// Wrapper around WriteBufferFromFile that adds blob storage logging on finalize.
/// Inherits from WriteBufferFromFileDecorator to follow the established pattern.
class WriteBufferFromFileWithLogging final : public WriteBufferFromFileDecorator
{
public:
    WriteBufferFromFileWithLogging(
        const String & file_path_,
        size_t buf_size,
        const String & bucket_,
        BlobStorageLogWriterPtr blob_log_)
        : WriteBufferFromFileDecorator(std::make_unique<WriteBufferFromFile>(file_path_, buf_size))
        , file_path(file_path_)
        , bucket(bucket_)
        , blob_log(std::move(blob_log_))
    {
    }

    std::string getFileName() const override { return file_path; }

private:
    void finalizeImpl() override
    {
        WriteBufferFromFileDecorator::finalizeImpl();

        if (blob_log)
        {
            blob_log->addEvent(
                BlobStorageLogElement::EventType::Upload,
                /* bucket */ bucket,
                /* remote_path */ file_path,
                /* local_path */ {},
                /* data_size */ count(),
                /* elapsed_microseconds */ 0,
                /* error_code */ 0,
                /* error_message */ {});
        }
    }

    const String file_path;
    const String bucket;
    BlobStorageLogWriterPtr blob_log;
};

/// Give the version about to be published a modification time strictly later than
/// the version it replaces, so that no two versions of a path can ever share an etag.
///
/// The etag is `(mtime, inode, size)`, and not one of the three is a version counter:
///
///   - `rename` unlinks the replaced version, which frees its inode number for
///     immediate reuse - on ext4 the next file created in that directory typically
///     lands on it, so version N+2 gets the inode number of version N;
///   - two payloads of equal length share `st_size`;
///   - a kernel that stamps inode times from the coarse per-tick clock gives every
///     file written within one tick the same mtime down to the nanosecond.
///
/// When all three coincide, the etag of an older version compares equal to the etag
/// of the current one, so a writer holding the older etag passes the If-Match check
/// in `publishConditionally` and silently overwrites a newer version - a lost update,
/// which is precisely what a compare-and-swap must never allow. Publication is
/// serialized by the exclusive lock on the parent directory, so stamping every
/// incoming version past the one it replaces makes the mtime - and therefore the
/// etag - strictly increase across the whole version history of the path, which is
/// exactly the uniqueness the compare-and-swap needs.
void stampMTimeAfterReplacedVersion(const String & temp_path, const struct stat & replaced_stat)
{
    const struct timespec replaced_mtime = getMTime(replaced_stat);

    struct stat temp_stat{};
    if (0 != ::stat(temp_path.c_str(), &temp_stat))
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_STAT, temp_path, "Cannot stat file {}", temp_path);

    /// The payload was written after the replaced version had been published, so a
    /// clock of any usable resolution already separates the two on its own.
    if (isLaterThan(getMTime(temp_stat), replaced_mtime))
        return;

    /// A nanosecond is enough on every filesystem that keeps sub-second inode times
    /// (ext4 with the default inode size, xfs, btrfs, tmpfs). One that keeps whole
    /// seconds only truncates such a stamp back onto the replaced mtime, and is
    /// served by the second candidate instead.
    std::array<struct timespec, 2> candidates = {replaced_mtime, replaced_mtime};

    if (++candidates[0].tv_nsec >= 1'000'000'000)
    {
        candidates[0].tv_nsec = 0;
        ++candidates[0].tv_sec;
    }

    candidates[1].tv_nsec = 0;
    ++candidates[1].tv_sec;

    for (const auto & candidate : candidates)
    {
        struct timespec times[2];
        times[0].tv_sec = 0;
        times[0].tv_nsec = UTIME_OMIT; /// Leave the access time alone.
        times[1] = candidate;

        if (0 != ::utimensat(AT_FDCWD, temp_path.c_str(), times, 0))
            ErrnoException::throwFromPath(
                ErrorCodes::SYSTEM_ERROR, temp_path, "Cannot set the modification time of {}", temp_path);

        /// The filesystem is free to store a coarser time than the one requested,
        /// so read back what it actually kept instead of assuming the stamp landed.
        if (0 != ::stat(temp_path.c_str(), &temp_stat))
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_STAT, temp_path, "Cannot stat file {}", temp_path);

        if (isLaterThan(getMTime(temp_stat), replaced_mtime))
            return;
    }

    throw Exception(
        ErrorCodes::SYSTEM_ERROR,
        "Cannot advance the modification time of {} past {}.{:09} of the object it replaces: the filesystem does not keep "
        "modification times precisely enough to tell two versions of an object apart, so a conditional write cannot be "
        "performed safely on it",
        temp_path,
        static_cast<Int64>(replaced_mtime.tv_sec),
        static_cast<Int64>(replaced_mtime.tv_nsec));
}

void publishConditionally(const String & temp_path, const String & target_path, const std::optional<String> & if_match_etag)
{
    if (!if_match_etag.has_value())
    {
        if (0 == ::link(temp_path.c_str(), target_path.c_str()))
            return;

        if (errno == EEXIST)
            throw Exception(
                ErrorCodes::FILE_ALREADY_EXISTS,
                "Object {} already exists, PreconditionFailed for If-None-Match",
                target_path);

        ErrnoException::throwFromPath(ErrorCodes::CANNOT_LINK, target_path, "Cannot link {} to {}", temp_path, target_path);
    }

    const String parent_path = fs::path(target_path).parent_path();
    int dir_fd = ::open(parent_path.c_str(), O_RDONLY | O_DIRECTORY | O_CLOEXEC);
    if (dir_fd < 0)
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_OPEN_FILE, parent_path, "Cannot open directory {}", parent_path);

    SCOPE_EXIT({ [[maybe_unused]] int err = ::close(dir_fd); });

    if (0 != ::flock(dir_fd, LOCK_EX))
        ErrnoException::throwFromPath(ErrorCodes::SYSTEM_ERROR, parent_path, "Cannot lock directory {}", parent_path);

    struct stat file_stat{};
    if (0 != ::stat(target_path.c_str(), &file_stat))
    {
        if (errno == ENOENT)
            throw Exception(
                ErrorCodes::STALE_VERSION,
                "Object {} does not exist, PreconditionFailed for If-Match: {}",
                target_path, *if_match_etag);

        ErrnoException::throwFromPath(ErrorCodes::CANNOT_STAT, target_path, "Cannot stat file {}", target_path);
    }

    if (auto etag = makeETag(file_stat); etag != *if_match_etag)
        throw Exception(
            ErrorCodes::STALE_VERSION,
            "Object {} was modified concurrently (etag {}, expected {}), PreconditionFailed for If-Match",
            target_path, etag, *if_match_etag);

    stampMTimeAfterReplacedVersion(temp_path, file_stat);

    if (0 != ::rename(temp_path.c_str(), target_path.c_str()))
        ErrnoException::throwFromPath(ErrorCodes::SYSTEM_ERROR, target_path, "Cannot rename {} to {}", temp_path, target_path);
}

class WriteBufferToConditionallyPublishedFile final : public WriteBufferFromFileDecorator
{
public:
    WriteBufferToConditionallyPublishedFile(
        const String & target_path_,
        const String & temp_path_,
        size_t buf_size,
        std::optional<String> if_match_etag_,
        const String & bucket_,
        BlobStorageLogWriterPtr blob_log_)
        : WriteBufferFromFileDecorator(std::make_unique<WriteBufferFromFile>(temp_path_, buf_size, O_WRONLY | O_CREAT | O_EXCL))
        , target_path(target_path_)
        , temp_path(temp_path_)
        , if_match_etag(std::move(if_match_etag_))
        , bucket(bucket_)
        , blob_log(std::move(blob_log_))
    {
    }

    ~WriteBufferToConditionallyPublishedFile() override
    {
        removeTemporaryFile();
    }

    std::string getFileName() const override { return target_path; }

private:
    void finalizeImpl() override
    {
        WriteBufferFromFileDecorator::finalizeImpl();

        const size_t data_size = count();
        publishConditionally(temp_path, target_path, if_match_etag);
        removeTemporaryFile();

        if (blob_log)
        {
            blob_log->addEvent(
                BlobStorageLogElement::EventType::Upload,
                /* bucket */ bucket,
                /* remote_path */ target_path,
                /* local_path */ {},
                /* data_size */ data_size,
                /* elapsed_microseconds */ 0,
                /* error_code */ 0,
                /* error_message */ {});
        }
    }

    void cancelImpl() noexcept override
    {
        WriteBufferFromFileDecorator::cancelImpl();
        removeTemporaryFile();
    }

    void removeTemporaryFile() noexcept
    {
        if (std::exchange(temporary_file_removed, true))
            return;
        [[maybe_unused]] int err = ::unlink(temp_path.c_str());
    }

    const String target_path;
    const String temp_path;
    const std::optional<String> if_match_etag;
    const String bucket;
    BlobStorageLogWriterPtr blob_log;
    bool temporary_file_removed = false;
};

}

std::unique_ptr<ReadBufferFromFileBase> LocalObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    bool /* use_external_buffer */,
    bool /* restrict_seek */) const
{
    auto resolved_path = resolvePathRelativelyToKeyPrefix(object.remote_path);
    LOG_TEST(log, "Read object: {}", resolved_path);
    auto buf = createReadBufferFromFileBase(resolved_path, patchSettings(read_settings), read_hint);

    if (read_settings.remote_fs_settings.enable_blob_storage_log)
    {
        auto blob_storage_log = BlobStorageLogWriter::create(settings.disk_name);
        if (blob_storage_log)
        {
            blob_storage_log->local_path = object.local_path;
            return std::make_unique<ReadBufferFromFileWithLogging>(
                std::move(buf), resolved_path, settings.key_prefix, std::move(blob_storage_log));
        }
    }

    return buf;
}

std::unique_ptr<WriteBufferFromFileBase> LocalObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> /* attributes */,
    size_t buf_size,
    const WriteSettings & write_settings)
{
    throwIfReadonly();

    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LocalObjectStorage doesn't support append to files");

    auto resolved_path = resolvePathRelativelyToKeyPrefix(object.remote_path);
    LOG_TEST(log, "Write object: {}", resolved_path);

    /// Unlike real blob storage, in local fs we cannot create a file with non-existing prefix.
    /// So let's create it.
    fs::create_directories(fs::path(resolved_path).parent_path());

    auto blob_storage_log = BlobStorageLogWriter::create(settings.disk_name);
    if (blob_storage_log)
        blob_storage_log->local_path = object.local_path;

    const auto & if_none_match = write_settings.object_storage_write_if_none_match;
    const auto & if_match = write_settings.object_storage_write_if_match;

    if (!if_none_match.empty() && !if_match.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "If-None-Match and If-Match cannot be used together for object {}", object.remote_path);

    if (!if_none_match.empty() || !if_match.empty())
    {
        if (!if_none_match.empty() && if_none_match != "*")
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Local object storage supports only `*` for If-None-Match, got `{}`",
                if_none_match);

        std::optional<String> if_match_etag;
        if (!if_match.empty())
            if_match_etag = if_match;

        /// Both filenames have to come out of `resolved_path`, exactly like the
        /// unconditional write below: a key relative to `settings.key_prefix` would
        /// otherwise be staged and published under the server's working directory,
        /// giving conditional writes a different notion of a key than every other
        /// method of this storage.
        auto target_path = fs::path(resolved_path);
        auto temp_path = target_path.parent_path() / fmt::format(".tmp_{}_{}", target_path.filename().string(), getRandomASCIIString(8));

        return std::make_unique<WriteBufferToConditionallyPublishedFile>(
            resolved_path,
            temp_path,
            buf_size,
            std::move(if_match_etag),
            settings.key_prefix,
            std::move(blob_storage_log));
    }

    return std::make_unique<WriteBufferFromFileWithLogging>(
        resolved_path,
        buf_size,
        settings.key_prefix,
        std::move(blob_storage_log));
}

void LocalObjectStorage::removeObject(const StoredObject & object) const
{
    throwIfReadonly();
    auto resolved_path = resolvePathRelativelyToKeyPrefix(object.remote_path);

    /// For local object storage files are actually removed when "metadata" is removed.
    if (!exists(object))
        return;

    auto blob_storage_log = BlobStorageLogWriter::create(settings.disk_name);

    Stopwatch watch;
    Int32 error_code = 0;
    String error_message;

    if (0 != unlink(resolved_path.data()))
    {
        error_code = errno;
        error_message = errnoToString();
        auto elapsed = watch.elapsedMicroseconds();

        if (blob_storage_log)
            blob_storage_log->addEvent(
                BlobStorageLogElement::EventType::Delete,
                /* bucket */ settings.key_prefix,
                /* remote_path */ resolved_path,
                /* local_path */ object.local_path,
                /* data_size */ object.bytes_size,
                elapsed,
                error_code,
                error_message);

        ErrnoException::throwFromPath(ErrorCodes::CANNOT_UNLINK, resolved_path, "Cannot unlink file {}", resolved_path);
    }

    auto elapsed = watch.elapsedMicroseconds();

    if (blob_storage_log)
        blob_storage_log->addEvent(
            BlobStorageLogElement::EventType::Delete,
            /* bucket */ settings.key_prefix,
            /* remote_path */ resolved_path,
            /* local_path */ object.local_path,
            /* data_size */ object.bytes_size,
            elapsed,
            error_code,
            error_message);

    fs::path dir = fs::path(resolved_path).parent_path();
    fs::path root = fs::weakly_canonical(settings.key_prefix);
    while (dir.has_parent_path() && dir.has_relative_path() && dir != root && pathStartsWith(dir, root))
    {
        LOG_TEST(log, "Removing empty directory {}, has_parent_path: {}, has_relative_path: {}, root: {}, starts with root: {}",
            std::string(dir), dir.has_parent_path(), dir.has_relative_path(), std::string(root), pathStartsWith(dir, root));

        std::string dir_str = dir;
        if (0 != rmdir(dir_str.data()))
        {
            if (errno == ENOTDIR || errno == ENOTEMPTY)
                break;
            ErrnoException::throwFromPath(ErrorCodes::CANNOT_RMDIR, dir_str, "Cannot remove directory {}", dir_str);
        }

        dir = dir.parent_path();
    }
}

void LocalObjectStorage::removeObjects(const StoredObjects & objects) const
{
    throwIfReadonly();
    for (const auto & object : objects)
        removeObject(object);
}

void LocalObjectStorage::removeObjectIfExists(const StoredObject & object)
{
    removeObject(object);

    fiu_do_on(FailPoints::local_object_storage_network_error_during_remove, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected error after remove object {}", object.remote_path);
    });
}

void LocalObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    throwIfReadonly();
    for (const auto & object : objects)
        removeObjectIfExists(object);
}

std::optional<ObjectMetadata> LocalObjectStorage::tryGetObjectMetadata(const std::string & path, bool) const
{
    /// The same path resolution and the same metadata builder as `getObjectMetadata`:
    /// this method only differs from it in tolerating an object that does not exist,
    /// so a caller must not be able to observe a different file or a differently
    /// shaped etag depending on which of the two it called.
    auto resolved_path = resolvePathRelativelyToKeyPrefix(path);
    LOG_TEST(log, "Getting metadata for path: {}", resolved_path);

    return tryStatResolvedPath(resolved_path);
}

SmallObjectDataWithMetadata LocalObjectStorage::readSmallObjectAndGetObjectMetadata( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    size_t max_size_bytes,
    std::optional<size_t>) const
{
    auto resolved_path = resolvePathRelativelyToKeyPrefix(object.remote_path);
    LOG_TEST(log, "Read small object: {}", resolved_path);

    /// Opening the file here instead of delegating to `readObject`: the etag must come
    /// from an `fstat` of the very descriptor the data is read from, so that the data
    /// and the etag returned together describe one and the same version of the object.
    /// A separate `stat` could observe a newer version and hand the caller the etag of
    /// a version whose content it never saw, which would let the following `If-Match`
    /// write pass its check and lose that version - the very update the compare-and-swap
    /// exists to protect. The read-side wrappers `readObject` attaches are applied below.
    auto patched_settings = patchSettings(read_settings);
    auto file_in = std::make_unique<ReadBufferFromFile>(resolved_path, std::clamp<size_t>(max_size_bytes, 1, DBMS_DEFAULT_BUFFER_SIZE));

    struct stat file_stat{};
    if (0 != ::fstat(file_in->getFD(), &file_stat))
        ErrnoException::throwFromPath(ErrorCodes::CANNOT_STAT, resolved_path, "Cannot stat file {}", resolved_path);

    std::unique_ptr<ReadBufferFromFileBase> in = std::move(file_in);

    if (patched_settings.remote_fs_settings.enable_blob_storage_log)
    {
        auto blob_storage_log = BlobStorageLogWriter::create(settings.disk_name);
        if (blob_storage_log)
        {
            blob_storage_log->local_path = object.local_path;
            in = std::make_unique<ReadBufferFromFileWithLogging>(
                std::move(in), resolved_path, settings.key_prefix, std::move(blob_storage_log));
        }
    }

    SmallObjectDataWithMetadata result;
    WriteBufferFromString out(result.data);
    copyDataMaxBytes(*in, out, max_size_bytes);
    out.finalize();

    result.metadata = makeObjectMetadata(file_stat);
    return result;
}


ObjectMetadata LocalObjectStorage::getObjectMetadata(const std::string & path, bool) const
{
    auto resolved_path = resolvePathRelativelyToKeyPrefix(path);
    LOG_TEST(log, "Getting metadata for path: {}", resolved_path);

    /// Every etag of this storage has to come out of `makeETag`: a caller feeds the
    /// etag it read back into `If-Match`, and a token in any other shape can only
    /// ever compare unequal there, turning a conditional write into an unconditional
    /// `PreconditionFailed`.
    struct stat file_stat{};
    if (0 != ::stat(resolved_path.c_str(), &file_stat))
    {
        const int stat_errno = errno;
        const bool does_not_exist = isVanishedEntryError(std::error_code(stat_errno, std::generic_category()));
        ErrnoException::throwFromPathWithErrno(
            does_not_exist ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_STAT,
            resolved_path,
            stat_errno,
            "Cannot get metadata of file {}",
            resolved_path);
    }

    return makeObjectMetadata(file_stat);
}

void LocalObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t/* max_keys */) const
{
    /// A path with an embedded NUL is malformed: libc truncates every syscall
    /// argument at the NUL while our `std::string`/`fs::path` keep the full
    /// value, so the traversal below would re-open the same truncated directory
    /// and queue ever-longer NUL-bearing child paths that never converge (an
    /// unbounded loop for a directory that holds only subdirectories). A
    /// `readdir` entry name never contains a NUL, so this single up-front check
    /// guarantees no path derived during traversal can reintroduce one.
    if (path.contains('\0'))
        throw fs::filesystem_error(
            "Path contains an embedded NUL byte", path,
            std::make_error_code(std::errc::invalid_argument));

    auto resolved_path = resolvePathRelativelyToKeyPrefix(path);
    if (!fs::exists(resolved_path) || !fs::is_directory(resolved_path))
        return;

    /// Listing is a best-effort snapshot driven with the non-throwing
    /// `error_code` overloads. Tolerate ONLY the concurrent-disappearance class
    /// (see `isVanishedEntryError`) - such an entry is simply omitted, mirroring
    /// how a remote object store omits a concurrently-deleted object. Any other
    /// error (EACCES, EIO, ...) is propagated, so a caller never reads a
    /// silently truncated listing. The same tolerance is applied by
    /// `tryGetObjectMetadata` below for the per-entry metadata stat.
    auto throw_unless_vanished = [&](const std::error_code & e, const fs::path & at)
    {
        if (!isVanishedEntryError(e))
            throw fs::filesystem_error("Cannot list local object storage directory", at, e);
    };

    /// We descend with an explicit stack of non-recursive `directory_iterator`s
    /// rather than a single `recursive_directory_iterator`. The recursive
    /// iterator opens each child directory with `opendir` while incrementing and,
    /// if that `opendir` fails (e.g. the directory was concurrently removed), it
    /// resets itself to `end()` - silently dropping every later, still-present
    /// sibling. Listing only the open directory at a time lets a vanished
    /// directory skip just its own subtree while the remaining entries are still
    /// reported. Each directory is fully drained before any subdirectory is
    /// opened, so an invalid path (e.g. a NUL-truncated argument) fails fast on
    /// the per-entry stat instead of recursing.
    std::vector<fs::path> pending_dirs;
    pending_dirs.emplace_back(resolved_path);

    while (!pending_dirs.empty())
    {
        const fs::path dir = std::move(pending_dirs.back());
        pending_dirs.pop_back();

        std::error_code ec;
        fs::directory_iterator it(dir, ec);
        if (ec)
        {
            /// The directory itself vanished (or a path component was replaced)
            /// before we could open it: omit only this subtree, keep the rest.
            throw_unless_vanished(ec, dir);
            continue;
        }

        const fs::directory_iterator end;
        while (it != end)
        {
            const fs::path entry_path = it->path();
            const bool is_dir = it->is_directory(ec); /// follows symlinks
            if (ec)
            {
                throw_unless_vanished(ec, entry_path); /// entry vanished before we could stat it: skip it
            }
            else if (is_dir)
            {
                /// Descend only into real subdirectories, never into symlinks,
                /// matching the no-follow-symlink default of the recursive
                /// iterator (avoids cycles). A symlink-to-directory is neither
                /// descended into nor reported as an object. The symlink probe
                /// is the fourth stat in this path: route its error through the
                /// same disappearance filter so a real error (EACCES, EIO) is
                /// not silently dropped while a vanished entry is skipped.
                std::error_code sym_ec;
                const bool is_symlink = it->is_symlink(sym_ec);
                if (sym_ec)
                    throw_unless_vanished(sym_ec, entry_path);
                else if (!is_symlink)
                    pending_dirs.push_back(entry_path);
            }
            else
            {
                /// `entry_path` is produced by descending the already-validated
                /// `resolved_path` and the walk never follows symlinks, so it is
                /// guaranteed to be under the key prefix. Stat it directly instead
                /// of routing through `tryGetObjectMetadata`, whose per-entry
                /// re-resolution (`fs::relative` / `fs::weakly_canonical`) uses
                /// throwing filesystem primitives that abort the whole listing
                /// when a churned entry vanishes mid-resolution.
                if (auto metadata = tryStatResolvedPath(entry_path))
                    children.emplace_back(std::make_shared<RelativePathWithMetadata>(entry_path, std::move(*metadata)));
            }

            it.increment(ec);
            if (ec)
            {
                /// `increment` resets the iterator to end() on error; a vanished
                /// entry only affects this directory, the worklist preserves the rest.
                throw_unless_vanished(ec, dir);
                break;
            }
        }
    }
}

bool LocalObjectStorage::existsOrHasAnyChild(const std::string & path) const
{
    auto resolved_path = resolvePathRelativelyToKeyPrefix(path);
    /// Unlike real object storage, existence of a prefix path can be checked by
    /// just checking existence of this prefix directly, so simple exists is enough here.
    return exists(StoredObject(resolved_path));
}

void LocalObjectStorage::copyObject( // NOLINT
    const StoredObject & object_from,
    const StoredObject & object_to,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    std::optional<ObjectAttributes> /* object_to_attributes */)
{
    throwIfReadonly();
    auto in = readObject(object_from, read_settings);
    auto out = writeObject(object_to, WriteMode::Rewrite, /* attributes= */ {}, /* buf_size= */ DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    copyData(*in, *out);
    out->finalize();
}

void LocalObjectStorage::shutdown()
{
}

void LocalObjectStorage::startup()
{
}

void LocalObjectStorage::throwIfReadonly() const
{
    if (settings.read_only)
        throw Exception(ErrorCodes::READONLY, "Local object storage `{}` is readonly", getName());
}

ObjectStorageKeyGeneratorPtr LocalObjectStorage::createKeyGenerator() const
{
    return createObjectStorageKeyGeneratorByPrefix(settings.key_prefix);
}

}
