#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>

#include <atomic>
#include <filesystem>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
#include <IO/ReadBufferFromFileDecorator.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromFileDecorator.h>
#include <IO/copyData.h>
#include <Interpreters/BlobStorageLog.h>
#include <Interpreters/Context.h>
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

bool LocalObjectStorage::exists(const StoredObject & object) const
{
    return fs::exists(object.remote_path);
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

}

std::unique_ptr<ReadBufferFromFileBase> LocalObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint,
    bool /* use_external_buffer */,
    bool /* restrict_seek */) const
{
    LOG_TEST(log, "Read object: {}", object.remote_path);
    auto buf = createReadBufferFromFileBase(object.remote_path, patchSettings(read_settings), read_hint);

    if (read_settings.remote_fs_settings.enable_blob_storage_log)
    {
        auto blob_storage_log = BlobStorageLogWriter::create(settings.disk_name);
        if (blob_storage_log)
        {
            blob_storage_log->local_path = object.local_path;
            return std::make_unique<ReadBufferFromFileWithLogging>(
                std::move(buf), object.remote_path, settings.key_prefix, std::move(blob_storage_log));
        }
    }

    return buf;
}

std::unique_ptr<WriteBufferFromFileBase> LocalObjectStorage::writeObject( /// NOLINT
    const StoredObject & object,
    WriteMode mode,
    std::optional<ObjectAttributes> /* attributes */,
    size_t buf_size,
    const WriteSettings & /* write_settings */)
{
    throwIfReadonly();

    if (mode != WriteMode::Rewrite)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "LocalObjectStorage doesn't support append to files");

    LOG_TEST(log, "Write object: {}", object.remote_path);

    /// Unlike real blob storage, in local fs we cannot create a file with non-existing prefix.
    /// So let's create it.
    fs::create_directories(fs::path(object.remote_path).parent_path());

    auto blob_storage_log = BlobStorageLogWriter::create(settings.disk_name);
    if (blob_storage_log)
        blob_storage_log->local_path = object.local_path;

    return std::make_unique<WriteBufferFromFileWithLogging>(
        object.remote_path,
        buf_size,
        settings.key_prefix,
        std::move(blob_storage_log));
}

void LocalObjectStorage::removeObject(const StoredObject & object) const
{
    throwIfReadonly();

    /// For local object storage files are actually removed when "metadata" is removed.
    if (!exists(object))
        return;

    auto blob_storage_log = BlobStorageLogWriter::create(settings.disk_name);

    Stopwatch watch;
    Int32 error_code = 0;
    String error_message;

    if (0 != unlink(object.remote_path.data()))
    {
        error_code = errno;
        error_message = errnoToString();
        auto elapsed = watch.elapsedMicroseconds();

        if (blob_storage_log)
            blob_storage_log->addEvent(
                BlobStorageLogElement::EventType::Delete,
                /* bucket */ settings.key_prefix,
                /* remote_path */ object.remote_path,
                /* local_path */ object.local_path,
                /* data_size */ object.bytes_size,
                elapsed,
                error_code,
                error_message);

        ErrnoException::throwFromPath(ErrorCodes::CANNOT_UNLINK, object.remote_path, "Cannot unlink file {}", object.remote_path);
    }

    auto elapsed = watch.elapsedMicroseconds();

    if (blob_storage_log)
        blob_storage_log->addEvent(
            BlobStorageLogElement::EventType::Delete,
            /* bucket */ settings.key_prefix,
            /* remote_path */ object.remote_path,
            /* local_path */ object.local_path,
            /* data_size */ object.bytes_size,
            elapsed,
            error_code,
            error_message);

    /// Remove empty directories.
    fs::path dir = fs::path(object.remote_path).parent_path();
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
    throwIfReadonly();
    if (exists(object))
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

namespace
{
/// The concurrent-disappearance class for a best-effort local listing: an entry
/// removed mid-stat (ENOENT) or whose parent path component was concurrently
/// replaced by a non-directory (ENOTDIR). Mirrors libc++'s own `__is_dne_error`.
/// Every other error (EACCES, EIO, ...) is a real failure and must propagate.
bool isVanishedEntryError(const std::error_code & error)
{
    return error == std::errc::no_such_file_or_directory || error == std::errc::not_a_directory;
}
}

ObjectMetadata LocalObjectStorage::getObjectMetadata(const std::string & path, bool) const
{
    ObjectMetadata object_metadata;
    LOG_TEST(log, "Getting metadata for path: {}", path);

    auto time = fs::last_write_time(path);

    object_metadata.size_bytes = fs::file_size(path);
    object_metadata.etag = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(time.time_since_epoch()).count());
    object_metadata.last_modified = Poco::Timestamp::fromEpochTime(
        std::chrono::duration_cast<std::chrono::seconds>(time.time_since_epoch()).count());
    return object_metadata;
}

std::optional<ObjectMetadata> LocalObjectStorage::tryGetObjectMetadata(const std::string & path, bool) const
{
    ObjectMetadata object_metadata;
    LOG_TEST(log, "Getting metadata for path: {}", path);

    std::error_code error;
    auto time = fs::last_write_time(path, error);
    if (error)
    {
        if (isVanishedEntryError(error))
            return {};
        throw fs::filesystem_error("Got unexpected error while getting last write time", path, error);
    }

    object_metadata.size_bytes = fs::file_size(path, error);
    if (error)
    {
        /// The entry may vanish between the two stat calls (concurrent removal),
        /// or a parent path component may be concurrently replaced by a file.
        if (isVanishedEntryError(error))
            return {};
        throw fs::filesystem_error("Got unexpected error while getting file size", path, error);
    }

    object_metadata.etag = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(time.time_since_epoch()).count());
    object_metadata.last_modified = Poco::Timestamp::fromEpochTime(
        std::chrono::duration_cast<std::chrono::seconds>(time.time_since_epoch()).count());
    return object_metadata;
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
    if (path.find('\0') != std::string::npos)
        throw fs::filesystem_error(
            "Path contains an embedded NUL byte", path,
            std::make_error_code(std::errc::invalid_argument));

    if (!fs::exists(path) || !fs::is_directory(path))
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
    pending_dirs.emplace_back(path);

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
                if (auto metadata = tryGetObjectMetadata(entry_path, /*with_tags=*/ false))
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
    /// Unlike real object storage, existence of a prefix path can be checked by
    /// just checking existence of this prefix directly, so simple exists is enough here.
    return exists(StoredObject(path));
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
