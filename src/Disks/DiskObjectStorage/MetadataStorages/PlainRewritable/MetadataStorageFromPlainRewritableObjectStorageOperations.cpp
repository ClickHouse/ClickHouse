#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/PrefixPath.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>

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

/// The logical path of a directory as it is written to `prefix.path`.
std::string getLogicalDirectoryPath(const NormalizedPath & directory)
{
    if (directory.empty())
        return "/";
    return directory.string() + "/";
}

/// Rewrites the `prefix.path` object of a directory with the given contents.
void writeDirectoryMetadata(
    IObjectStorage & object_storage,
    const PlainRewritableLayout & layout,
    const NormalizedPath & directory,
    const DirectoryRemoteInfo & info)
{
    auto metadata_object_key = layout.constructDirectoryObjectKey(info.remote_path);
    LOG_TRACE(
        getLogger("MetadataStorageFromPlainRewritableObjectStorage"),
        "Rewriting metadata for directory '{}' with remote path='{}', {} files listed explicitly",
        directory.string(),
        metadata_object_key,
        info.has_explicit_file_list ? info.files.size() : 0);

    auto buf = object_storage.writeObject(
        StoredObject(metadata_object_key, directory.string()),
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ 128,
        /*settings*/ getWriteSettings());

    writeString(serializePrefixPath(getLogicalDirectoryPath(directory), info), *buf);
    buf->finalize();
}

DirectoryRemoteInfo getDirectoryInfoOrThrow(const FsSnapshot & fs_tree, const NormalizedPath & directory)
{
    auto info = fs_tree.getDirectoryRemoteInfo(directory);
    if (!info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist or is virtual", directory.string());
    return std::move(*info);
}

/// The blob is removed only when its last link is gone. The link count is tracked only for blobs that are referenced,
/// so the count is not decremented for the removed blob: a new blob may appear under the same key later.
std::optional<StoredObject> removeLinkAndGetBlobToRemove(
    FsSnapshot & fs_tree, const PlainRewritableLayout & layout, const std::string & blob_key)
{
    if (fs_tree.getBlobLinkCount(blob_key) == 1)
        return StoredObject(layout.constructBlobObjectKey(blob_key));

    fs_tree.removeBlobLink(blob_key);
    return std::nullopt;
}

}

bool isMetadataOnlyMove(
    const FsSnapshot & fs_tree,
    const DirectoryRemoteInfo & directory_from,
    const DirectoryRemoteInfo & directory_to,
    const std::string & blob_key_from,
    const std::optional<std::string> & blob_key_of_existing_target)
{
    /// A directory with the implicit file list cannot lose a file whose blob stays under its prefix (the blob would be discovered
    /// as a file again), nor gain a file whose blob lives elsewhere. And a shared blob cannot be moved or overwritten.
    return directory_from.has_explicit_file_list
        || directory_to.has_explicit_file_list
        || fs_tree.getBlobLinkCount(blob_key_from) > 1
        || (blob_key_of_existing_target && fs_tree.getBlobLinkCount(*blob_key_of_existing_target) > 1);
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
    const DirectoryRemoteInfo & remote_info, std::optional<std::string> expected_logical_path)
{
    auto metadata_object_key = layout->constructDirectoryObjectKey(remote_info.remote_path);
    StoredObject metadata_object(metadata_object_key);

    if (expected_logical_path)
    {
        chassert(expected_logical_path.value().ends_with('/'));
        LockMemoryExceptionInThread temporarily_lock_exceptions;

        std::string data;
        auto read_settings = getReadSettings();
        read_settings.useForSmallRemoteRead(1024);

        auto read_buf = object_storage->readObject(metadata_object, read_settings);
        readStringUntilEOF(data, *read_buf);
        const auto parsed = parsePrefixPath(data);
        if (normalizePath(parsed.logical_path) != normalizePath(expected_logical_path.value())
            || parsed.has_explicit_file_list != remote_info.has_explicit_file_list)
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Incorrect data for object key {}, expected logical path {} (explicit file list: {}), got {} (explicit file list: {})",
                metadata_object_key,
                expected_logical_path.value(),
                remote_info.has_explicit_file_list,
                parsed.logical_path,
                parsed.has_explicit_file_list);
    }

    auto write_buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ 128,
        /*settings*/ getWriteSettings());

    return write_buf;
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::rewriteSingleDirectory(
    const std::filesystem::path & from, const std::filesystem::path & to, const DirectoryRemoteInfo & remote_info, WriteBuffer & buffer)
{
    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Rewriting '{}' to '{}'", from, to);

    writeString(serializePrefixPath(to.string(), remote_info), buffer);

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

        auto write_buf = createWriteBuf(remote_info.value(), /*expected_logical_path*/validate_content ? std::make_optional(sub_path_from) : std::nullopt);

        changed_paths.insert(sub_path_from);
        rewriteSingleDirectory(sub_path_from, sub_path_to, remote_info.value(), *write_buf);
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

        auto write_buf = createWriteBuf(remote_info.value(), /*expected_logical_path*/std::nullopt);
        rewriteSingleDirectory(sub_path_to, sub_path_from, remote_info.value(), *write_buf);
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
    writeString(serializePrefixPath(path.string(), info), *buf);
    buf->finalize();
}

MetadataStorageFromPlainObjectStorageWriteFileOperation::MetadataStorageFromPlainObjectStorageWriteFileOperation(
    std::string path_,
    StoredObject object_,
    std::string blob_key_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    StoredObjects & removed_objects_)
    : path(std::move(path_))
    , object(std::move(object_))
    , blob_key(std::move(blob_key_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , removed_objects(removed_objects_)
{
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageWriteFileOperation::execute()
{
    LOG_TEST(getLogger("MetadataStorageFromPlainObjectStorageWriteFileOperation"), "Creating metadata for a file '{}', size: {}, blob key: '{}'", path, object.bytes_size, blob_key);

    const auto normalized_path = normalizePath(path);
    const auto directory = normalized_path.parent_path();
    const auto file_name = normalized_path.filename().string();

    auto directory_info = getDirectoryInfoOrThrow(*fs_tree, directory);
    const std::string default_blob_key = getDefaultBlobKey(directory_info.remote_path, file_name);
    /// `generateObjectKeyForPath` chose the key: the default location `<remote path>/<file name>` for an implicit
    /// directory or a fresh name, and a random name when the file already exists in an explicit directory or shares
    /// its blob (so a new file never clobbers a blob still linked from elsewhere).
    const std::string new_blob_key = blob_key.empty() ? default_blob_key : blob_key;

    if (const auto it = directory_info.files.find(file_name); it != directory_info.files.end())
    {
        const auto existing_blob_key = getBlobKey(directory_info, file_name, it->second);
        /// Rewriting a file to a new blob drops the link to the old one; the old blob is removed if it was the last link.
        /// When the key is reused (an in-place overwrite in the default location) the caller has already replaced the
        /// blob's content, so there is nothing to remove and the link count is unchanged.
        if (existing_blob_key != new_blob_key)
            replaced_blob = removeLinkAndGetBlobToRemove(*fs_tree, *layout, existing_blob_key);

        fs_tree->removeFile(path);
    }

    fs_tree->recordFile(path, FileRemoteInfo{.bytes_size = object.bytes_size, .last_modified = std::time(nullptr), .blob_key = new_blob_key});

    /// An explicit directory does not list its blobs, so the new file must be added to `prefix.path`. A blob outside
    /// of the default location cannot be discovered by listing either, so it forces the directory to the explicit form.
    if (!directory_info.has_explicit_file_list && new_blob_key == default_blob_key)
        return;

    previous_directory_info = directory_info;
    fs_tree->markDirectoryExplicit(directory);

    prefix_path_written = true;
    writeDirectoryMetadata(*object_storage, *layout, directory, getDirectoryInfoOrThrow(*fs_tree, directory));
}

void MetadataStorageFromPlainObjectStorageWriteFileOperation::undo()
{
    if (!prefix_path_written)
        return;

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageWriteFileOperation"), "Reversing the metadata rewrite for the directory of '{}'", path);
    writeDirectoryMetadata(*object_storage, *layout, normalizePath(path).parent_path(), previous_directory_info.value());
}

void MetadataStorageFromPlainObjectStorageWriteFileOperation::finalize()
{
    if (!replaced_blob)
        return;

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageWriteFileOperation"), "Removing the replaced blob '{}' of the file '{}'", replaced_blob->remote_path, path);
    object_storage->removeObjectIfExists(*replaced_blob);
    removed_objects.push_back(*replaced_blob);
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

    const auto normalized_path = normalizePath(path);
    const auto directory = normalized_path.parent_path();
    const auto file_name = normalized_path.filename().string();

    auto directory_info = getDirectoryInfoOrThrow(*fs_tree, directory);
    const auto blob_key = getBlobKey(directory_info, file_name, directory_info.files.at(file_name));

    if (directory_info.has_explicit_file_list || fs_tree->getBlobLinkCount(blob_key) > 1)
    {
        /// The file is dropped from the explicit file list; the blob stays until the last link is gone, and is removed after the commit.
        previous_directory_info = directory_info;

        fs_tree->removeFile(path);
        blob_to_remove = removeLinkAndGetBlobToRemove(*fs_tree, *layout, blob_key);
        fs_tree->markDirectoryExplicit(directory);

        prefix_path_written = true;
        writeDirectoryMetadata(*object_storage, *layout, directory, getDirectoryInfoOrThrow(*fs_tree, directory));
        return;
    }

    remote_source_path = layout->constructBlobObjectKey(blob_key);
    remote_tmp_path = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));

    copy_started = true;
    object_storage->copyObject(StoredObject(remote_source_path), StoredObject(remote_tmp_path), getReadSettings(), getWriteSettings());

    remove_started = true;
    object_storage->removeObjectIfExists(StoredObject(remote_source_path));

    fs_tree->removeFile(path);
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::undo()
{
    if (prefix_path_written)
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation"), "Reversing the metadata rewrite for the directory of '{}'", path);
        writeDirectoryMetadata(*object_storage, *layout, normalizePath(path).parent_path(), previous_directory_info.value());
        return;
    }

    if (!copy_started)
        return;

    if (remove_started)
        object_storage->copyObject(StoredObject(remote_tmp_path), StoredObject(remote_source_path), getReadSettings(), getWriteSettings());

    object_storage->removeObjectIfExists(StoredObject(remote_tmp_path));
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::finalize()
{
    if (blob_to_remove)
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation"), "Removing the blob '{}' of the unlinked file '{}'", blob_to_remove->remote_path, path);
        object_storage->removeObjectIfExists(*blob_to_remove);
        removed_objects.push_back(*blob_to_remove);
    }

    if (copy_started)
    {
        removed_objects.push_back(StoredObject(remote_source_path));
        object_storage->removeObjectIfExists(StoredObject(remote_tmp_path));
    }
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

    if (!fs_tree->existsDirectory(normalizePath(path_to).parent_path()))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Target directory '{}' does not exist", normalizePath(path_to).parent_path().string());

    if (fs_tree->existsFile(path_to))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

    const auto normalized_path_from = normalizePath(path_from);
    const auto directory_info_from = getDirectoryInfoOrThrow(*fs_tree, normalized_path_from.parent_path());
    const auto & file_info_from = directory_info_from.files.at(normalized_path_from.filename());
    /// The source may belong to a directory in the explicit form, written while hard links were enabled.
    remote_path_from = layout->constructBlobObjectKey(getBlobKey(directory_info_from, normalized_path_from.filename(), file_info_from));

    const auto normalized_path_to = normalizePath(path_to);
    const auto directory_to = normalized_path_to.parent_path();
    const auto directory_info_to = getDirectoryInfoOrThrow(*fs_tree, directory_to);
    /// The copy is a new blob at the default location, so it needs no explicit file list of its own.
    remote_path_to = layout->constructFileObjectKey(directory_info_to.remote_path, normalized_path_to.filename());

    copy_attempted = true;
    object_storage->copyObject(StoredObject(remote_path_from), StoredObject(remote_path_to), getReadSettings(), getWriteSettings());
    fs_tree->recordFile(path_to, FileRemoteInfo{.bytes_size = file_info_from.bytes_size, .last_modified = file_info_from.last_modified, .blob_key = {}});

    /// A directory in the explicit form does not list its blobs, so the copy has to be added to `prefix.path`.
    /// This happens only on a disk that had hard links enabled before.
    if (!directory_info_to.has_explicit_file_list)
        return;

    previous_directory_info = directory_info_to;
    prefix_path_written = true;
    writeDirectoryMetadata(*object_storage, *layout, directory_to, getDirectoryInfoOrThrow(*fs_tree, directory_to));
}

void MetadataStorageFromPlainObjectStorageCopyFileOperation::undo()
{
    if (prefix_path_written)
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageCopyFileOperation"), "Reversing the metadata rewrite for the directory of '{}'", path_to);
        writeDirectoryMetadata(*object_storage, *layout, normalizePath(path_to).parent_path(), previous_directory_info.value());
    }

    if (!copy_attempted)
        return;

    LOG_WARNING(
        getLogger("MetadataStorageFromPlainObjectStorageCopyFileOperation"),
        "Removing file '{}' that was copied from '{}",
        path_to,
        path_from);

    object_storage->removeObjectIfExists(StoredObject(remote_path_to));
}

MetadataStorageFromPlainObjectStorageHardLinkOperation::MetadataStorageFromPlainObjectStorageHardLinkOperation(
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

void MetadataStorageFromPlainObjectStorageHardLinkOperation::execute()
{
    LOG_TEST(getLogger("MetadataStorageFromPlainObjectStorageHardLinkOperation"), "Creating hard link '{}' to '{}'", path_to, path_from);

    if (!fs_tree->existsFile(path_from))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the source path '{}' does not exist", path_from);
    else if (!fs_tree->existsDirectory(path_to.parent_path()))
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path_to.parent_path());
    else if (!fs_tree->getDirectoryRemoteInfo(path_to.parent_path()))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", path_to.parent_path());
    else if (fs_tree->existsFile(path_to))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

    const auto normalized_path_from = normalizePath(path_from);
    const auto directory_info_from = getDirectoryInfoOrThrow(*fs_tree, normalized_path_from.parent_path());
    const auto & file_info_from = directory_info_from.files.at(normalized_path_from.filename());
    const auto blob_key = getBlobKey(directory_info_from, normalized_path_from.filename(), file_info_from);

    const auto directory_to = normalizePath(path_to).parent_path();
    previous_directory_info = getDirectoryInfoOrThrow(*fs_tree, directory_to);

    fs_tree->recordFile(path_to, FileRemoteInfo{.bytes_size = file_info_from.bytes_size, .last_modified = file_info_from.last_modified, .blob_key = blob_key});
    fs_tree->addBlobLink(blob_key);
    fs_tree->markDirectoryExplicit(directory_to);

    prefix_path_written = true;
    writeDirectoryMetadata(*object_storage, *layout, directory_to, getDirectoryInfoOrThrow(*fs_tree, directory_to));
}

void MetadataStorageFromPlainObjectStorageHardLinkOperation::undo()
{
    if (!prefix_path_written)
        return;

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageHardLinkOperation"), "Reversing the hard link '{}' to '{}'", path_to, path_from);
    writeDirectoryMetadata(*object_storage, *layout, normalizePath(path_to).parent_path(), previous_directory_info.value());
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
    const auto directory_from = normalized_path_from.parent_path();
    const auto directory_to = normalized_path_to.parent_path();
    const auto directory_info_from = getDirectoryInfoOrThrow(*fs_tree, directory_from);
    const auto directory_info_to = getDirectoryInfoOrThrow(*fs_tree, directory_to);

    file_from_remote_info = directory_info_from.files.at(normalized_path_from.filename());
    const auto blob_key_from = getBlobKey(directory_info_from, normalized_path_from.filename(), file_from_remote_info.value());

    std::optional<std::string> blob_key_of_existing_target;
    if (const auto it = directory_info_to.files.find(normalized_path_to.filename()); it != directory_info_to.files.end())
        blob_key_of_existing_target = getBlobKey(directory_info_to, normalized_path_to.filename(), it->second);

    if (isMetadataOnlyMove(*fs_tree, directory_info_from, directory_info_to, blob_key_from, blob_key_of_existing_target))
    {
        metadata_only_move = true;
        previous_directory_info_from = directory_info_from;
        previous_directory_info_to = directory_info_to;

        if (blob_key_of_existing_target)
        {
            if (!replaceable)
                throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

            fs_tree->removeFile(path_to);
            replaced_blob = removeLinkAndGetBlobToRemove(*fs_tree, *layout, *blob_key_of_existing_target);
        }

        /// The blob stays in place, only the file lists change.
        fs_tree->removeFile(path_from);
        fs_tree->recordFile(path_to, FileRemoteInfo{.bytes_size = file_from_remote_info->bytes_size, .last_modified = file_from_remote_info->last_modified, .blob_key = blob_key_from});
        fs_tree->markDirectoryExplicit(directory_to);
        fs_tree->markDirectoryExplicit(directory_from);

        prefix_path_written_to = true;
        writeDirectoryMetadata(*object_storage, *layout, directory_to, getDirectoryInfoOrThrow(*fs_tree, directory_to));

        if (directory_from != directory_to)
        {
            prefix_path_written_from = true;
            writeDirectoryMetadata(*object_storage, *layout, directory_from, getDirectoryInfoOrThrow(*fs_tree, directory_from));
        }

        return;
    }

    remote_path_from = layout->constructFileObjectKey(directory_info_from.remote_path, normalized_path_from.filename());
    remote_path_to = layout->constructFileObjectKey(directory_info_to.remote_path, normalized_path_to.filename());
    tmp_remote_path_from = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));
    tmp_remote_path_to = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));
    const auto read_settings = getReadSettingsForMetadata();
    const auto write_settings = getWriteSettingsForMetadata();

    if (fs_tree->existsFile(path_to))
    {
        if (!replaceable)
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

        fiu_do_on(FailPoints::plain_object_storage_copy_temp_target_file_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });

        object_storage->copyObject(
            /*object_from=*/StoredObject(remote_path_to),
            /*object_to=*/StoredObject(tmp_remote_path_to),
            read_settings,
            write_settings);
        moved_existing_target_file = true;

        fs_tree->removeFile(path_to);
        fs_tree->recordFile(path_to, file_from_remote_info.value());

        object_storage->removeObjectIfExists(StoredObject(remote_path_to));
    }
    else
    {
        fs_tree->recordFile(path_to, file_from_remote_info.value());
    }

    {
        fiu_do_on(FailPoints::plain_object_storage_copy_temp_source_file_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });

        object_storage->copyObject(
            /*object_from=*/StoredObject(remote_path_from),
            /*object_to=*/StoredObject(tmp_remote_path_from),
            read_settings,
            write_settings);
        moved_existing_source_file = true;
    }

    {
        fiu_do_on(FailPoints::plain_object_storage_copy_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });
        object_storage->copyObject(
            /*object_from=*/StoredObject(remote_path_from), /*object_to=*/StoredObject(remote_path_to), read_settings, write_settings);
        object_storage->removeObjectIfExists(StoredObject(remote_path_from));
        moved_file = true;
    }

    fs_tree->removeFile(path_from);
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::undo()
{
    if (metadata_only_move)
    {
        LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveFileOperation"), "Reversing the metadata rewrite for the move from '{}' to '{}'", path_from, path_to);

        if (prefix_path_written_to)
            writeDirectoryMetadata(*object_storage, *layout, normalizePath(path_to).parent_path(), previous_directory_info_to.value());

        if (prefix_path_written_from)
            writeDirectoryMetadata(*object_storage, *layout, normalizePath(path_from).parent_path(), previous_directory_info_from.value());

        return;
    }

    const auto read_settings = getReadSettings();
    const auto write_settings = getWriteSettings();

    if (moved_file)
    {
        LOG_WARNING(
            getLogger("MetadataStorageFromPlainObjectStorageMoveFileOperation"),
            "Removing file '{}' that was moved (replaceable = {}) from '{}",
            path_to,
            replaceable,
            path_from);

        object_storage->removeObjectIfExists(StoredObject(remote_path_to));
    }

    if (moved_existing_source_file)
    {
        object_storage->copyObject(
            /*object_from=*/StoredObject(tmp_remote_path_from),
            /*object_to=*/StoredObject(remote_path_from),
            read_settings,
            write_settings);

        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_from));
    }

    if (moved_existing_target_file)
    {
        object_storage->copyObject(
            /*object_from=*/StoredObject(tmp_remote_path_to),
            /*object_to=*/StoredObject(remote_path_to),
            read_settings,
            write_settings);

        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_to));
    }
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::finalize()
{
    if (metadata_only_move)
    {
        if (replaced_blob)
        {
            LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveFileOperation"), "Removing the blob '{}' of the replaced file '{}'", replaced_blob->remote_path, path_to);
            object_storage->removeObjectIfExists(*replaced_blob);
            removed_objects.push_back(*replaced_blob);
        }

        return;
    }

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

        for (const auto & [subdir, remote_info] : fs_tree->getSubtreeRemoteInfo(tmp_path))
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

            /// We also need to remove all files inside each of the subdirectories, but only the blobs that are not shared with files outside.
            for (const auto & [filename, file_info] : remote_info->files)
            {
                auto file_path = subdir_path / filename;
                const auto blob_key = getBlobKey(remote_info.value(), filename, file_info);

                if (fs_tree->getBlobLinkCount(blob_key) == 1)
                {
                    LOG_TRACE(log, "Removing file '{}'", file_path);
                    objects_to_remove.emplace_back(layout->constructBlobObjectKey(blob_key), file_path);
                }
                else
                {
                    LOG_TRACE(log, "Keeping the blob of the file '{}', it is shared with other files", file_path);
                    fs_tree->removeBlobLink(blob_key);
                }
            }
        }

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

    object_storage->removeObjectsIfExist(objects_to_remove);
    removed_objects.append_range(objects_to_remove);
}

}
