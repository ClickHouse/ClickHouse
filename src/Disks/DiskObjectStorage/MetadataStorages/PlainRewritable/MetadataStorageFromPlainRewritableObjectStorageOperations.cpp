#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>
#include <IO/WriteBufferFromFileBase.h>
#include <IO/copyData.h>

#include <filesystem>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <fmt/ranges.h>
#include <Poco/Timestamp.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/LockMemoryExceptionInThread.h>
#include <Common/MemoryTrackerBlockerInThread.h>
#include <Common/SharedLockGuard.h>
#include <Common/getRandomASCIIString.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FILE_DOESNT_EXIST;
    extern const int FILE_ALREADY_EXISTS;
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int DIRECTORY_ALREADY_EXISTS;
    extern const int INCORRECT_DATA;
    extern const int FAULT_INJECTED;
    extern const int LOGICAL_ERROR;
    extern const int CANNOT_RMDIR;
    extern const int CANNOT_CREATE_DIRECTORY;
    extern const int AZURE_BLOB_STORAGE_ERROR;
    extern const int S3_ERROR;
    extern const int FILE_CHANGED_DURING_READ;
};

namespace FailPoints
{
    extern const char plain_object_storage_write_fail_on_directory_create[];
    extern const char plain_object_storage_write_fail_on_directory_move[];
    extern const char plain_object_storage_copy_fail_on_file_move[];
    extern const char plain_object_storage_copy_temp_source_file_fail_on_file_move[];
    extern const char plain_object_storage_copy_temp_target_file_fail_on_file_move[];
}

namespace
{

/// The object storages whose generations can be pinned: their endpoints name a generation with an
/// `ETag`, take it back as a precondition of a read, of a copy and of a delete (`If-Match`), and
/// refuse a create-if-absent write (`If-None-Match: *`) of a key that a blob is at.
bool pinsGenerations(const IObjectStorage & object_storage)
{
    const auto type = object_storage.getType();
    return type == ObjectStorageType::Azure || type == ObjectStorageType::S3;
}

/// The error an operation is refused with when the endpoint of a pinning object storage will not
/// name the generation of a blob.
int errorCodeOfAnUnnamedGeneration(const IObjectStorage & object_storage)
{
    return object_storage.getType() == ObjectStorageType::Azure ? ErrorCodes::AZURE_BLOB_STORAGE_ERROR : ErrorCodes::S3_ERROR;
}

/// Names the generation that is at `remote_path` right now, together with its size, or nothing
/// when the blob is not there or the endpoint reports no `ETag` for it.
std::optional<StoredObject> nameTheGenerationThatIsThereNow(IObjectStorage & object_storage, const std::filesystem::path & remote_path)
{
    auto metadata = object_storage.tryGetObjectMetadata(remote_path, /*with_tags=*/ false);
    if (!metadata || metadata->etag.empty())
        return {};

    StoredObject object(remote_path);
    object.bytes_size = metadata->size_bytes;
    object.etag = metadata->etag;
    return object;
}

}

StoredObject pinToTheGenerationThatIsThereNow(IObjectStorage & object_storage, const std::filesystem::path & remote_path)
{
    StoredObject object(remote_path);

    if (!pinsGenerations(object_storage))
        return object;

    auto metadata = object_storage.tryGetObjectMetadata(remote_path, /*with_tags=*/ false);
    /// The blob of a file the metadata says exists is not there. Returning it unpinned would not
    /// keep the copy from happening: `copyObject` makes a `HEAD` of its own for a source without an
    /// `ETag`, so a blob recreated between the two probes would be copied as the generation that
    /// `HEAD` finds, while the delete that follows would still address the blob by path alone and
    /// take away whatever is there by then. Fail closed, for the same reason the case of an endpoint
    /// that reports no generation does: this operation may only move a generation it has named.
    if (!metadata)
        throw Exception(
            ErrorCodes::FILE_DOESNT_EXIST,
            "Blob {} was not moved: it does not exist, so the move cannot be pinned to the generation "
            "of the blob that is being moved",
            remote_path.string());

    /// The endpoint reports no generation for the blob, so nothing here can be pinned: the copy
    /// would take whatever is there when it runs and the delete would remove whatever is there when
    /// it runs. Refuse the move instead of losing a rewritten file, the same way `copyObject` and
    /// the `ObjectStorageQueue` post-processing refuse an unpinnable object.
    if (metadata->etag.empty())
        throw Exception(
            errorCodeOfAnUnnamedGeneration(object_storage),
            "Blob {} was not moved: the endpoint reports no `ETag` for it, so the move cannot be "
            "pinned to the generation of the blob that is being moved",
            remote_path.string());

    object.bytes_size = metadata->size_bytes;
    object.etag = metadata->etag;
    return object;
}

/// Declared in the header, where the contract is documented.
void refuseAGenerationOfAnotherSize(const StoredObject & generation, size_t recorded_size, const std::filesystem::path & path)
{
    /// Not named, not measured: the object storage does not pin (see `pinToTheGenerationThatIsThereNow`).
    if (generation.etag.empty())
        return;

    if (generation.bytes_size != recorded_size)
        throw Exception(
            ErrorCodes::FILE_CHANGED_DURING_READ,
            "Blob {} of the file '{}' is {} bytes long, while the metadata of the file records {} bytes: the blob "
            "was written over since the file was recorded, and the generation that is there now is not the file",
            generation.remote_path,
            path.string(),
            generation.bytes_size,
            recorded_size);
}

/// Declared in the header, where the contract is documented.
bool restoreTheSavedBlobWithoutWritingOver(
    IObjectStorage & object_storage,
    const std::filesystem::path & remote_tmp_path,
    const std::filesystem::path & remote_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings)
{
    auto log = getLogger("PlainRewritableRollback");

    /// An object storage that does not pin restores by key, the way its execute side deletes and
    /// writes by key.
    if (!pinsGenerations(object_storage))
    {
        object_storage.copyObject(StoredObject(remote_tmp_path), StoredObject(remote_path), read_settings, write_settings);
        return true;
    }

    /// The saved blob is read pinned to its own generation, so that a restore cannot stitch together
    /// what this transaction saved aside with something written over the temporary key since.
    auto saved = nameTheGenerationThatIsThereNow(object_storage, remote_tmp_path);
    if (!saved)
    {
        LOG_WARNING(
            log,
            "The blob saved aside at {} cannot be named (it is not there, or the endpoint reports no "
            "`ETag` for it), so it is not restored to {}",
            remote_tmp_path.string(),
            remote_path.string());
        return false;
    }

    /// The restore creates the destination and never replaces one: `If-None-Match: *` makes the
    /// endpoint refuse the write when a blob is at the key (Azure answers `409 Conflict`, S3 `412
    /// Precondition Failed`), so a writer that recreated the key at any moment - including after a
    /// probe of the key would have found it free - keeps its generation. There is no way to express
    /// this through `copyObject`, which writes by key alone.
    WriteSettings create_if_absent = write_settings;
    create_if_absent.object_storage_write_if_none_match = "*";

    std::unique_ptr<WriteBufferFromFileBase> out;
    try
    {
        auto in = object_storage.readObject(*saved, read_settings);
        out = object_storage.writeObject(
            StoredObject(remote_path), WriteMode::Rewrite, /* attributes */ {}, DBMS_DEFAULT_BUFFER_SIZE, create_if_absent);
        copyData(*in, *out);
        out->finalize();
        return true;
    }
    catch (...)
    {
        if (out)
            out->cancel();

        /// Every failure of the restore is treated the same way, because they all mean that the
        /// generation this transaction saved aside is not at the key: the write was refused because
        /// somebody else got the key first, or it did not go through at all. The saved blob stays in
        /// the bucket, and its path is logged, rather than the restore being retried by a write that
        /// would be free to replace whatever is there.
        tryLogCurrentException(log, fmt::format(
            "The blob saved aside at {} was not restored to {}; it is left in the bucket",
            remote_tmp_path.string(), remote_path.string()));
        return false;
    }
}

std::optional<StoredObject> nameTheGenerationThatWasJustWritten(
    const IObjectStorage & object_storage, const std::filesystem::path & remote_path, const String & etag_the_copy_reported, size_t bytes_size)
{
    StoredObject object(remote_path);
    object.bytes_size = bytes_size;

    if (!pinsGenerations(object_storage))
        return object;

    if (etag_the_copy_reported.empty())
        return {};

    object.etag = etag_the_copy_reported;
    return object;
}

MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation::MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation(
    std::shared_ptr<Preconditions> preconditions_,
    std::shared_ptr<FsSnapshot> fs_tree_)
    : preconditions(std::move(preconditions_))
    , fs_tree(std::move(fs_tree_))
{
}

void MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation::execute()
{
    preconditions->runChecks(fs_tree);
}

MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
    bool recursive_,
    std::filesystem::path path_,
    std::string directory_remote_path_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : recursive(recursive_)
    , path(std::move(path_))
    , directory_remote_path(std::move(directory_remote_path_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
{
    chassert(path.empty() || path.string().ends_with('/'));
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::execute()
{
    if (fs_tree->getDirectoryRemoteInfo(path))
        return;

    if (fs_tree->existsFile(path))
        throw Exception(ErrorCodes::CANNOT_CREATE_DIRECTORY, "File '{}' already exists", path.parent_path());

    if (!recursive)
        if (!fs_tree->existsDirectory(path.parent_path().parent_path()))
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path.parent_path().parent_path());

    auto metadata_object_key = layout->constructDirectoryObjectKey(directory_remote_path);

    if (fs_tree->existsDirectory(path))
        LOG_TRACE(
            getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation"),
            "Materializing virtual directory '{}' with remote path='{}'",
            path,
            metadata_object_key);
    else
        LOG_TRACE(
            getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation"),
            "Creating metadata for directory '{}' with remote path='{}'",
            path,
            metadata_object_key);

    auto metadata_object = StoredObject(metadata_object_key, path);

    write_attempted = true;
    auto buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ 128,
        /*settings*/ getWriteSettings());

    writeString(path.string(), *buf);
    fiu_do_on(FailPoints::plain_object_storage_write_fail_on_directory_create, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when creating '{}' directory", path);
    });
    buf->finalize();

    ProfileEvents::increment(metrics->directory_created);
    auto metadata = object_storage->getObjectMetadata(metadata_object.remote_path, /*with_tags=*/ false);
    fs_tree->recordDirectoryPath(path, DirectoryRemoteInfo{directory_remote_path, metadata.etag, metadata.last_modified.epochTime(), {}});
}

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::undo()
{
    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation"), "Reversing directory creation for path '{}'", path);

    if (write_attempted)
    {
        auto metadata_object_key = layout->constructDirectoryObjectKey(directory_remote_path);
        object_storage->removeObjectIfExists(StoredObject(metadata_object_key, path));
    }
}

MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
{
    chassert(path_from.empty() || path_from.string().ends_with('/'));
    chassert(path_to.empty() || path_to.string().ends_with('/'));
    chassert(metrics);
}

std::unique_ptr<WriteBufferFromFileBase> MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::createWriteBuf(
    const DirectoryRemoteInfo & remote_info, std::optional<std::string> expected_content)
{
    auto metadata_object_key = layout->constructDirectoryObjectKey(remote_info.remote_path);
    StoredObject metadata_object(metadata_object_key);

    if (expected_content)
    {
        chassert(expected_content.value().ends_with('/'));
        LockMemoryExceptionInThread temporarily_lock_exceptions;

        std::string data;
        auto read_settings = getReadSettings();
        read_settings.useForSmallRemoteRead(1024);

        auto read_buf = object_storage->readObject(metadata_object, read_settings);
        readStringUntilEOF(data, *read_buf);
        if (data != expected_content.value())
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Incorrect data for object key {}, expected {}, got {}",
                metadata_object_key,
                expected_content.value(),
                data);
    }

    auto write_buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ 128,
        /*settings*/ getWriteSettings());

    return write_buf;
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::rewriteSingleDirectory(const std::filesystem::path & from, const std::filesystem::path & to, WriteBuffer & buffer)
{
    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Rewriting '{}' to '{}'", from, to);

    writeString(to.string(), buffer);

    fiu_do_on(FailPoints::plain_object_storage_write_fail_on_directory_move,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", from, to);
    });

    buffer.finalize();

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Updated '{}' to '{}'", from, to);
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::execute()
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    constexpr bool validate_content = true;
#else
    constexpr bool validate_content = false;
#endif

    if (!fs_tree->existsDirectory(path_from))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path_from);
    else if (fs_tree->existsDirectory(path_to))
        throw Exception(ErrorCodes::DIRECTORY_ALREADY_EXISTS, "Directory '{}' already exists", path_to);
    else if (normalizePath(path_from).empty())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't move root folder");

    from_tree_info = fs_tree->getSubtreeRemoteInfo(path_from);

    for (const auto & [subdir, remote_info] : from_tree_info)
    {
        auto sub_path_to = path_to / subdir / "";
        auto sub_path_from = path_from / subdir / "";

        if (!remote_info.has_value())
        {
            LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Directory '{}' is virtual will not update in object storage", sub_path_from);
            continue;
        }

        auto write_buf = createWriteBuf(remote_info.value(), /*expected_content*/validate_content ? std::make_optional(sub_path_from) : std::nullopt);

        changed_paths.insert(sub_path_from);
        rewriteSingleDirectory(sub_path_from, sub_path_to, *write_buf);
    }

    fs_tree->moveDirectory(path_from, path_to);
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::undo()
{
    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Reversing directory move from '{}' to '{}'", path_from, path_to);

    for (const auto & [subdir, remote_info] : from_tree_info)
    {
        auto sub_path_to = path_to / subdir / "";
        auto sub_path_from = path_from / subdir / "";

        if (!changed_paths.contains(sub_path_from))
            continue;

        auto write_buf = createWriteBuf(remote_info.value(), /*expected_content*/std::nullopt);
        rewriteSingleDirectory(sub_path_to, sub_path_from, *write_buf);
    }
}

MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
    std::filesystem::path path_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : path(std::move(path_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
{
    chassert(path.empty() || path.string().ends_with('/'));
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::execute()
{
    if (!fs_tree->existsDirectory(path))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path);
    else if (auto children = fs_tree->listDirectory(path); !children.empty())
        throw Exception(ErrorCodes::CANNOT_RMDIR, "Directory '{}' is not empty. Children: [{}]", path, fmt::join(children, ", "));
    else if (normalizePath(path).empty())
        return;

    info = fs_tree->getDirectoryRemoteInfo(path).value();

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation"), "Removing directory '{}'", path);

    remove_attempted = true;
    auto metadata_object_key = layout->constructDirectoryObjectKey(info.remote_path);
    auto metadata_object = StoredObject(/*remote_path*/ metadata_object_key, /*local_path*/ path, path.string().length());
    object_storage->removeObjectIfExists(metadata_object);

    fs_tree->removeDirectory(path);
    ProfileEvents::increment(metrics->directory_removed);
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::undo()
{
    if (!remove_attempted)
        return;

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation"), "Reversing directory removal for '{}'", path);

    auto metadata_object_key = layout->constructDirectoryObjectKey(info.remote_path);
    auto metadata_object = StoredObject(metadata_object_key, path);

    auto buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ 128,
        /*settings*/ DB::getWriteSettings());
    writeString(path.string(), *buf);
    buf->finalize();
}

MetadataStorageFromPlainObjectStorageWriteFileOperation::MetadataStorageFromPlainObjectStorageWriteFileOperation(
    std::string path_,
    StoredObject object_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : path(std::move(path_))
    , object(std::move(object_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
{
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageWriteFileOperation::execute()
{
    LOG_TEST(getLogger("MetadataStorageFromPlainObjectStorageWriteFileOperation"), "Creating metadata for a file '{}', size: {}", path, object.bytes_size);

    if (fs_tree->existsFile(path))
        fs_tree->removeFile(path);

    fs_tree->recordFile(path, {object.bytes_size, std::time(nullptr)});
}

MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation(
    std::filesystem::path path_,
    bool if_exists_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    StoredObjects & removed_objects_)
    : path(std::move(path_))
    , if_exists(if_exists_)
    , fs_tree(std::move(fs_tree_))
    , object_storage(object_storage_)
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , removed_objects(removed_objects_)
{
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::execute()
{
    LOG_TEST(
        getLogger("MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation"),
        "Unlinking metadata for a write '{}'",
        path);

    if (!fs_tree->existsFile(path))
    {
        if (if_exists)
            return;

        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File '{}' does not exist", path);
    }

    const auto normalized_path_from = normalizePath(path);
    const auto directory_remote_path_from = fs_tree->getDirectoryRemoteInfo(normalized_path_from.parent_path())->remote_path;
    remote_source_path = layout->constructFileObjectKey(directory_remote_path_from, normalized_path_from.filename());
    remote_tmp_path = layout->constructScratchFileObjectKey(getRandomASCIIString(16));

    /// The blob is copied aside and then deleted: both requests must be about the same generation
    /// of it, or the delete would take away content that was never copied.
    const StoredObject source = pinToTheGenerationThatIsThereNow(*object_storage, remote_source_path);

    copy_started = true;
    object_storage->copyObject(source, StoredObject(remote_tmp_path), getReadSettings(), getWriteSettings());

    remove_started = true;
    try
    {
        object_storage->removeObjectIfExists(source);
    }
    catch (...)
    {
        /// The blob is not the generation that was copied aside any more, so it was left in place:
        /// the file still holds content that this operation has never seen, and restoring the copy
        /// over it would be the very loss the pinning prevents.
        if (getCurrentExceptionCode() == ErrorCodes::FILE_CHANGED_DURING_READ)
            source_was_left_in_place = true;
        throw;
    }

    fs_tree->removeFile(path);
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::undo()
{
    if (!copy_started)
        return;

    if (remove_started && !source_was_left_in_place)
    {
        /// The source blob was deleted, so its key is free and another writer may have recreated
        /// it. That blob is a generation this operation has never seen, and the copy it saved
        /// aside is the generation before it: restoring over it would lose the newer one. The
        /// restore therefore only creates the key and never replaces it, and a restore that did not
        /// happen leaves the saved blob in the bucket.
        if (!restoreTheSavedBlobWithoutWritingOver(
                *object_storage, remote_tmp_path, remote_source_path, getReadSettings(), getWriteSettings()))
        {
            LOG_WARNING(
                getLogger("MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation"),
                "Not restoring the blob of the file '{}': a blob that this transaction never saw is "
                "at {}, or the restore of it did not go through. The blob that was deleted is left "
                "in the bucket at {} instead of being restored over it",
                path,
                remote_source_path.string(),
                remote_tmp_path.string());
            return;
        }
    }

    object_storage->removeObjectIfExists(StoredObject(remote_tmp_path));
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::finalize()
{
    removed_objects.push_back(StoredObject(remote_source_path));

    if (copy_started)
        object_storage->removeObjectIfExists(StoredObject(remote_tmp_path));
}

MetadataStorageFromPlainObjectStorageCopyFileOperation::MetadataStorageFromPlainObjectStorageCopyFileOperation(
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
{
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageCopyFileOperation::execute()
{
    LOG_TEST(getLogger("MetadataStorageFromPlainObjectStorageCopyFileOperation"), "Copying file from '{}' to '{}'", path_from, path_to);

    if (!fs_tree->existsFile(path_from))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the source path '{}' does not exist", path_from);
    else if (!fs_tree->existsDirectory(path_to.parent_path()))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path_to.parent_path());
    else if (!fs_tree->getDirectoryRemoteInfo(path_to.parent_path()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", path_to.parent_path());
    else if (fs_tree->existsFile(path_to))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

    const auto normalized_path_from = normalizePath(path_from);
    const auto directory_remote_path_from = fs_tree->getDirectoryRemoteInfo(normalized_path_from.parent_path())->remote_path;
    remote_path_from = layout->constructFileObjectKey(directory_remote_path_from, normalized_path_from.filename());

    const auto normalized_path_to = normalizePath(path_to);
    const auto directory_remote_path_to = fs_tree->getDirectoryRemoteInfo(normalized_path_to.parent_path())->remote_path;
    remote_path_to = layout->constructFileObjectKey(directory_remote_path_to, normalized_path_to.filename());

    /// The copy is pinned to the generation of the source named here, and the file is recorded at
    /// `path_to` with the metadata of `path_from`, so that generation has to be the one the metadata
    /// describes: a blob of another size was written over the file out of band, and a link recorded
    /// with the size of the old generation would be read short of its end from then on. Such a link
    /// is refused before anything is written (see `refuseAGenerationOfAnotherSize`).
    const FileRemoteInfo file_from_remote_info = fs_tree->getFileRemoteInfo(path_from).value();
    const StoredObject source = pinToTheGenerationThatIsThereNow(*object_storage, remote_path_from);
    refuseAGenerationOfAnotherSize(source, file_from_remote_info.bytes_size, path_from);

    const String etag_the_copy_reported
        = object_storage->copyObject(source, StoredObject(remote_path_to), getReadSettings(), getWriteSettings());

    /// The destination blob is there from now on, so `undo` has to take it back out - and only it:
    /// the generation the copy wrote is the one the response to the copy named, and the delete in
    /// `undo` is pinned to it, so it cannot take away a generation another writer has put at the
    /// same key since - not even one put there right after the copy, because no request of this
    /// operation looks at the key again. A copy that threw before writing anything leaves nothing
    /// for `undo` to remove, which is why the flag is set here rather than before the copy.
    copied_to_destination = true;
    destination = StoredObject(remote_path_to);
    if (auto named = nameTheGenerationThatWasJustWritten(*object_storage, remote_path_to, etag_the_copy_reported, source.bytes_size))
    {
        destination = std::move(*named);
        destination_generation_is_named = true;
    }
    else
        throw Exception(
            errorCodeOfAnUnnamedGeneration(*object_storage),
            "Cannot copy '{}' to '{}': the endpoint reported no `ETag` for the blob at {} that the "
            "copy has just written, so a rollback of this copy cannot delete exactly that "
            "generation. The copy is refused here, before the file is recorded, and the rollback "
            "leaves the blob the copy wrote at its key rather than deleting whatever is at the key "
            "(see `undo`)",
            path_from.string(),
            path_to.string(),
            remote_path_to.string());

    fs_tree->recordFile(path_to, file_from_remote_info);
}

void MetadataStorageFromPlainObjectStorageCopyFileOperation::undo()
{
    if (!copied_to_destination)
        return;

    auto log = getLogger("MetadataStorageFromPlainObjectStorageCopyFileOperation");

    if (!destination_generation_is_named)
    {
        /// The generation the copy wrote was never named, so the only delete available here is one
        /// by key, and that one is not made: a delete by key cannot tell the blob this copy wrote
        /// from a generation another writer has put at the key since, and taking away a generation
        /// this transaction never wrote is the loss the pinned deletes of these operations exist to
        /// prevent. The blob is left where it is and its path is logged instead. The cost is that
        /// `load` rebuilds the files of a directory from every blob under the key of the directory,
        /// so that blob comes back as the file `path_to` on the next start of the server although
        /// the transaction never committed - a copy of the source at a key that was free by the
        /// metadata of this disk, which is recoverable by hand, while a deleted generation is not.
        LOG_ERROR(
            log,
            "Leaving the blob at {} that the copy of '{}' to '{}' wrote: the generation it holds "
            "could not be named, so a delete could only be made by the key alone and could take "
            "away a blob another writer has put there since. The blob is loaded as the file '{}' on "
            "the next start unless it is removed by hand",
            remote_path_to.string(),
            path_from,
            path_to,
            path_to);
        return;
    }

    LOG_WARNING(
        log,
        "Removing file '{}' that was copied from '{}",
        path_to,
        path_from);

    /// `destination` names the generation this copy wrote, so a blob that another writer has put at
    /// the same key since is refused with `FILE_CHANGED_DURING_READ` and stays.
    try
    {
        object_storage->removeObjectIfExists(destination);
    }
    catch (const Exception & e)
    {
        if (e.code() != ErrorCodes::FILE_CHANGED_DURING_READ)
            throw;

        LOG_WARNING(
            log,
            "Not removing the blob at {} that the copy of '{}' wrote: another writer has replaced "
            "that generation since, and it was never seen here",
            remote_path_to.string(),
            path_to);
    }
}

MetadataStorageFromPlainObjectStorageMoveFileOperation::MetadataStorageFromPlainObjectStorageMoveFileOperation(
    bool replaceable_,
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    StoredObjects & removed_objects_)
    : replaceable(replaceable_)
    , path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , removed_objects(removed_objects_)
{
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::execute()
{
    LOG_TEST(
        getLogger("MetadataStorageFromPlainObjectStorageMoveFileOperation"),
        "Moving file (replaceable = {}) from '{}' to '{}'",
        replaceable,
        path_from,
        path_to);

    if (!fs_tree->existsFile(path_from))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File '{}' does not exist", path_from);
    else if (!fs_tree->existsDirectory(path_to.parent_path()))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path_to.parent_path());
    else if (!fs_tree->getDirectoryRemoteInfo(path_to.parent_path()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", path_to.parent_path());

    const auto normalized_path_from = normalizePath(path_from);
    const auto normalized_path_to = normalizePath(path_to);
    const auto directory_remote_path_from = fs_tree->getDirectoryRemoteInfo(normalized_path_from.parent_path())->remote_path;
    const auto directory_remote_path_to = fs_tree->getDirectoryRemoteInfo(normalized_path_to.parent_path())->remote_path;

    remote_path_from = layout->constructFileObjectKey(directory_remote_path_from, normalized_path_from.filename());
    remote_path_to = layout->constructFileObjectKey(directory_remote_path_to, normalized_path_to.filename());
    tmp_remote_path_from = layout->constructScratchFileObjectKey(getRandomASCIIString(16));
    tmp_remote_path_to = layout->constructScratchFileObjectKey(getRandomASCIIString(16));
    file_from_remote_info = fs_tree->getFileRemoteInfo(path_from).value();
    const auto read_settings = getReadSettingsForMetadata();
    const auto write_settings = getWriteSettingsForMetadata();

    /// Every request that touches the blob of the source - the copy aside, the copy to the
    /// destination and the delete - is pinned to the generation named here, so the move carries
    /// one generation of the file and deletes exactly the one it carried. It is named before
    /// anything is written, because the file is recorded at `path_to` with the metadata of
    /// `path_from`, and the generation has to be the one that metadata describes: a blob of
    /// another size was written over the file out of band, and a file recorded with the size of
    /// the old generation would be read short of its end from then on. Such a move is refused
    /// here, with nothing to undo yet (see `refuseAGenerationOfAnotherSize`).
    source = pinToTheGenerationThatIsThereNow(*object_storage, remote_path_from);
    refuseAGenerationOfAnotherSize(source, file_from_remote_info->bytes_size, path_from);

    if (fs_tree->existsFile(path_to))
    {
        if (!replaceable)
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

        fiu_do_on(FailPoints::plain_object_storage_copy_temp_target_file_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });

        /// The blob of the target is copied aside and then deleted; both requests must be about
        /// the same generation of it (see `pinToTheGenerationThatIsThereNow`).
        const StoredObject target = pinToTheGenerationThatIsThereNow(*object_storage, remote_path_to);

        object_storage->copyObject(
            /*object_from=*/target,
            /*object_to=*/StoredObject(tmp_remote_path_to),
            read_settings,
            write_settings);
        moved_existing_target_file = true;

        fs_tree->removeFile(path_to);
        fs_tree->recordFile(path_to, file_from_remote_info.value());

        try
        {
            object_storage->removeObjectIfExists(target);
        }
        catch (...)
        {
            /// The target blob is a generation this operation has never copied aside, so it was
            /// left in place and must not be overwritten by the copy that was made of the
            /// generation before it.
            if (getCurrentExceptionCode() == ErrorCodes::FILE_CHANGED_DURING_READ)
                target_was_left_in_place = true;
            throw;
        }
    }
    else
    {
        fs_tree->recordFile(path_to, file_from_remote_info.value());
    }

    {
        fiu_do_on(FailPoints::plain_object_storage_copy_temp_source_file_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });

        /// The copy aside is pinned to the generation of the source named above.
        object_storage->copyObject(
            /*object_from=*/source,
            /*object_to=*/StoredObject(tmp_remote_path_from),
            read_settings,
            write_settings);
        moved_existing_source_file = true;
    }

    {
        fiu_do_on(FailPoints::plain_object_storage_copy_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });
        const String etag_the_copy_reported = object_storage->copyObject(
            /*object_from=*/source, /*object_to=*/StoredObject(remote_path_to), read_settings, write_settings);

        /// The destination blob is there from now on, whatever happens next, so `undo` has to take
        /// it back out: a blob of a move that was never committed resurrects `path_to` on restart,
        /// because the directory is rebuilt from the blobs that are in the bucket. The generation
        /// the copy wrote is the one the response to the copy named, and the delete in `undo` is
        /// pinned to it; no request of this operation looks at the key again.
        copied_to_destination = true;
        destination = StoredObject(remote_path_to);
        if (auto named = nameTheGenerationThatWasJustWritten(*object_storage, remote_path_to, etag_the_copy_reported, source.bytes_size))
        {
            destination = std::move(*named);
            destination_generation_is_named = true;
        }
        else
            throw Exception(
                errorCodeOfAnUnnamedGeneration(*object_storage),
                "Cannot move '{}' to '{}': the endpoint reported no `ETag` for the blob at {} that "
                "the copy has just written, so a rollback of this move cannot delete exactly that "
                "generation. The move is refused here, before the source is deleted, and the "
                "rollback leaves the blob the copy wrote at its key rather than deleting whatever "
                "is at the key (see `undo`)",
                path_from.string(),
                path_to.string(),
                remote_path_to.string());

        try
        {
            object_storage->removeObjectIfExists(source);
        }
        catch (...)
        {
            /// The source blob holds a generation that was never moved, so it stayed where it is
            /// and the copy of the previous generation must not be restored over it.
            if (getCurrentExceptionCode() == ErrorCodes::FILE_CHANGED_DURING_READ)
                source_was_left_in_place = true;
            throw;
        }
    }

    fs_tree->removeFile(path_from);
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::undo()
{
    const auto read_settings = getReadSettings();
    const auto write_settings = getWriteSettings();

    auto log = getLogger("MetadataStorageFromPlainObjectStorageMoveFileOperation");

    /// The copy to the destination is undone whether or not the delete of the source that follows
    /// it succeeded: `copied_to_destination` is set between the two.
    if (copied_to_destination && !destination_generation_is_named)
    {
        /// The move was refused because the generation the copy wrote could not be named, so the
        /// only delete available here is one by key, and it is not made, for the reason given in
        /// the copy operation: it could take away a generation another writer has put at the key
        /// since the copy, which is the loss the pinned deletes exist to prevent. The blob is left
        /// at the key and logged. It is loaded as the file `path_to` on the next start although the
        /// move never committed, and in the replaceable case the target this move had set aside is
        /// not restored over it (see `restoreTheSavedBlobWithoutWritingOver` below) and stays under
        /// its scratch key, which the restore logs as well.
        LOG_ERROR(
            log,
            "Leaving the blob at {} that the move of '{}' to '{}' wrote: the generation it holds "
            "could not be named, so a delete could only be made by the key alone and could take "
            "away a blob another writer has put there since. The blob is loaded as the file '{}' on "
            "the next start unless it is removed by hand",
            remote_path_to.string(),
            path_from,
            path_to,
            path_to);
    }
    else if (copied_to_destination)
    {
        LOG_WARNING(
            log,
            "Removing file '{}' that was moved (replaceable = {}) from '{}",
            path_to,
            replaceable,
            path_from);

        /// `destination` names the generation this move wrote, so a blob that another writer has
        /// put at the same key since is refused with `FILE_CHANGED_DURING_READ` and stays. The
        /// destination is then left as that writer made it, which is the fail-closed outcome: the
        /// blob this move wrote is gone from the metadata of this transaction either way.
        try
        {
            object_storage->removeObjectIfExists(destination);
        }
        catch (const Exception & e)
        {
            if (e.code() != ErrorCodes::FILE_CHANGED_DURING_READ)
                throw;

            LOG_WARNING(
                log,
                "Not removing the blob at {} that the move of '{}' wrote: another writer has "
                "replaced that generation since, and it was never seen here",
                remote_path_to.string(),
                path_to);
        }
    }

    if (moved_existing_source_file)
    {
        /// The same rule as in the unlink operation: the copy that was saved aside is only put back
        /// over a key that nobody has taken over since this move emptied it, and otherwise it is
        /// left in the bucket rather than overwriting a generation this move never saw.
        const bool restored = source_was_left_in_place
            || restoreTheSavedBlobWithoutWritingOver(
                *object_storage, tmp_remote_path_from, remote_path_from, read_settings, write_settings);

        if (!restored)
        {
            LOG_WARNING(
                log,
                "Not restoring the source blob of the move of '{}': a blob that this move never "
                "carried is at {}, or the restore of it did not go through. The blob that was "
                "deleted is left in the bucket at {}",
                path_from,
                remote_path_from.string(),
                tmp_remote_path_from.string());
        }
        else
        {
            object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_from));
        }
    }

    if (moved_existing_target_file)
    {
        /// The target that this move replaced is put back only over a key that is free. It is not
        /// free when the delete above was refused, or when another writer has taken the key over,
        /// and in both cases what is there is a generation this move never carried.
        const bool restored = target_was_left_in_place
            || restoreTheSavedBlobWithoutWritingOver(
                *object_storage, tmp_remote_path_to, remote_path_to, read_settings, write_settings);

        if (!restored)
        {
            LOG_WARNING(
                log,
                "Not restoring the blob that the move of '{}' replaced: a blob that this move never "
                "carried is at {}, or the restore of it did not go through. The blob that was "
                "replaced is left in the bucket at {}",
                path_to,
                remote_path_to.string(),
                tmp_remote_path_to.string());
        }
        else
        {
            object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_to));
        }
    }
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::finalize()
{
    removed_objects.push_back(StoredObject(remote_path_from));

    if (moved_existing_source_file)
        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_from));

    if (moved_existing_target_file)
        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_to));
}

MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation(
    std::filesystem::path path_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    StoredObjects & removed_objects_)
    : path(std::move(path_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , removed_objects(removed_objects_)
    , log(getLogger("MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation"))
{
    chassert(metrics);
    tmp_path = getRandomASCIIString(16);
    move_to_tmp_op = std::make_unique<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation>(path / "", tmp_path / "", fs_tree, object_storage, layout, metrics);
}

void MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::execute()
{
    /// Unfortunately we are able to create merge tree unlinked from database directory.
    /// In this case during the dropAllData method removeRecursive can be called pointing to the root folder.
    /// I don't know what to do in this case, so right now it is a no-op.
    if (normalizePath(path).empty())
        return;

    if (fs_tree->existsDirectory(path))
    {
        move_tried = true;
        move_to_tmp_op->execute();

        subtree_remote_info = fs_tree->getSubtreeRemoteInfo(tmp_path);
        fs_tree->removeDirectory(tmp_path);
    }
}

void MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::undo()
{
    if (move_tried)
    {
        move_to_tmp_op->undo();
    }
}

void MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::finalize()
{
    if (!move_tried)
        return;

    StoredObjects objects_to_remove;
    for (const auto & [subdir, remote_info] : subtree_remote_info)
    {
        auto subdir_path = tmp_path / subdir;

        if (!remote_info.has_value())
        {
            LOG_TRACE(log, "Directory '{}' is virtual, will not cleanup remote", subdir_path);
            continue;
        }

        LOG_TRACE(log, "Removing directory '{}'", subdir_path);

        auto metadata_object_key = layout->constructDirectoryObjectKey(remote_info->remote_path);
        objects_to_remove.emplace_back(metadata_object_key, path);

        /// We also need to remove all files inside each of the subdirectories.
        for (const auto & [filename, file_info] : remote_info->files)
        {
            auto file_path = subdir_path / filename;

            LOG_TRACE(log, "Removing file '{}'", file_path);

            auto file_object_key = layout->constructFileObjectKey(remote_info->remote_path, filename);
            objects_to_remove.emplace_back(file_object_key, file_path);
        }
    }

    object_storage->removeObjectsIfExist(objects_to_remove);
    removed_objects.append_range(objects_to_remove);
}

}
