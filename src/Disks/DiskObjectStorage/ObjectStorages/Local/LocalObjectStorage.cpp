#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>

#include <filesystem>
#include <Disks/IO/AsynchronousBoundedReadBuffer.h>
#include <Disks/IO/ReadBufferFromRemoteFSGather.h>
#include <Disks/IO/createReadBufferFromFileBase.h>
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
    extern const int PATH_ACCESS_DENIED;
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
    auto resolved_path = resolvePathRelativelyToKeyPrefix(object.remote_path);
    return fs::exists(resolved_path);
}

ReadSettings LocalObjectStorage::patchSettings(const ReadSettings & read_settings) const
{
    auto modified_settings{read_settings};
    /// Other options might break assertions in AsynchronousBoundedReadBuffer.
    modified_settings.local_fs_method = LocalFSReadMethod::pread;
    modified_settings.direct_io_threshold = 0; /// Disable.
    return IObjectStorage::patchSettings(modified_settings);
}

std::unique_ptr<ReadBufferFromFileBase> LocalObjectStorage::readObject( /// NOLINT
    const StoredObject & object,
    const ReadSettings & read_settings,
    std::optional<size_t> read_hint) const
{
    auto resolved_path = resolvePathRelativelyToKeyPrefix(object.remote_path);
    LOG_TEST(log, "Read object: {}", resolved_path);
    return createReadBufferFromFileBase(resolved_path, patchSettings(read_settings), read_hint);
}

namespace
{

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

    auto resolved_path = resolvePathRelativelyToKeyPrefix(object.remote_path);
    LOG_TEST(log, "Write object: {}", resolved_path);

    /// Unlike real blob storage, in local fs we cannot create a file with non-existing prefix.
    /// So let's create it.
    fs::create_directories(fs::path(resolved_path).parent_path());
    auto blob_storage_log = BlobStorageLogWriter::create(settings.disk_name);
    if (blob_storage_log)
        blob_storage_log->local_path = object.local_path;

    return std::make_unique<WriteBufferFromFileWithLogging>(resolved_path, buf_size, settings.key_prefix, std::move(blob_storage_log));
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

    /// Remove empty directories.
    fs::path dir = fs::path(resolvePathRelativelyToKeyPrefix(fs::path(resolved_path).parent_path()));
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

        dir = fs::path(resolvePathRelativelyToKeyPrefix(fs::path(dir).parent_path()));
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
    resolvePathRelativelyToKeyPrefix(object.remote_path);
    if (exists(object))
        removeObject(object);

    fiu_do_on(FailPoints::local_object_storage_network_error_during_remove, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injected error after remove object {}", object.remote_path);
    });
}

String resolvePathRelativelyToBase(const String & path, const String & base_path)
{
    auto norm_base = std::filesystem::path(base_path).lexically_normal();
    auto norm_base_canonical = std::filesystem::weakly_canonical(norm_base);
    auto combined = (norm_base_canonical / path).lexically_normal();
    auto combined_canonical = std::filesystem::weakly_canonical(combined);

    auto rel = combined.lexically_relative(norm_base_canonical);
    auto rel_canonical = combined_canonical.lexically_relative(norm_base_canonical);

    // Checking that both symbolic link and relative path traversal are not used to access files outside of the base path.
    if ((!rel.empty() && rel.begin()->string() == "..") || !(pathStartsWith(combined, norm_base_canonical))
        || (!rel_canonical.empty() && rel_canonical.begin()->string() == "..") || !pathStartsWith(combined_canonical, norm_base_canonical))
    {
        throw Exception(
            ErrorCodes::PATH_ACCESS_DENIED,
            "Path `{}` which was resolved to `{}` and canonicalized to `{}` is outside the table path directory : `{}`",
            path,
            combined.string(),
            combined_canonical.string(),
            norm_base_canonical.string());
    }

    // We return canonical path to avoid TOCTOU attack
    return combined_canonical.string();
}

String LocalObjectStorage::resolvePathRelativelyToKeyPrefix(const String & path) const
{
    return resolvePathRelativelyToBase(path, settings.key_prefix);
}

void LocalObjectStorage::removeObjectsIfExist(const StoredObjects & objects)
{
    throwIfReadonly();
    for (const auto & object : objects)
        removeObjectIfExists(object);
}

ObjectMetadata LocalObjectStorage::getObjectMetadata(const std::string & path, bool) const
{
    auto resolved_path = resolvePathRelativelyToKeyPrefix(path);
    ObjectMetadata object_metadata;
    LOG_TEST(log, "Getting metadata for path: {}", resolved_path);

    auto time = fs::last_write_time(resolved_path);
    object_metadata.size_bytes = fs::file_size(resolved_path);
    object_metadata.etag = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(time.time_since_epoch()).count());
    object_metadata.last_modified = Poco::Timestamp::fromEpochTime(
        std::chrono::duration_cast<std::chrono::seconds>(time.time_since_epoch()).count());
    return object_metadata;
}

std::optional<ObjectMetadata> LocalObjectStorage::tryGetObjectMetadata(const std::string & path, bool) const
{
    auto resolved_path = resolvePathRelativelyToKeyPrefix(path);
    ObjectMetadata object_metadata;
    LOG_TEST(log, "Getting metadata for path: {}", resolved_path);

    std::error_code error;
    auto time = fs::last_write_time(resolved_path, error);
    if (error)
    {
        if (error == std::errc::no_such_file_or_directory)
            return {};
        throw fs::filesystem_error("Got unexpected error while getting last write time", resolved_path, error);
    }

    /// no_such_file_or_directory is ignored only for last_write_time for consistency
    object_metadata.size_bytes = fs::file_size(resolved_path);
    object_metadata.etag = std::to_string(std::chrono::duration_cast<std::chrono::nanoseconds>(time.time_since_epoch()).count());
    object_metadata.last_modified = Poco::Timestamp::fromEpochTime(
        std::chrono::duration_cast<std::chrono::seconds>(time.time_since_epoch()).count());
    return object_metadata;
}

void LocalObjectStorage::listObjects(const std::string & path, RelativePathsWithMetadata & children, size_t/* max_keys */) const
{
    auto resolved_path = resolvePathRelativelyToKeyPrefix(path);
    if (!fs::exists(resolved_path) || !fs::is_directory(resolved_path))
        return;

    for (const auto & entry : fs::directory_iterator(resolved_path))
    {
        if (entry.is_directory())
        {
            listObjects(entry.path(), children, 0);
            continue;
        }

        children.emplace_back(std::make_shared<RelativePathWithMetadata>(entry.path(), getObjectMetadata(entry.path(), false)));
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
