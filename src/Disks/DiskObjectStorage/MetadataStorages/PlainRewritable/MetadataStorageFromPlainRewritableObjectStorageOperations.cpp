#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/InMemoryDirectoryTree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritablePrefixPath.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetadataHelpers.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ReadSettings.h>
#include <IO/WriteSettings.h>

#include <filesystem>
#include <unordered_map>
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

MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
    bool recursive_,
    std::filesystem::path path_,
    std::string directory_remote_path_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : recursive(recursive_)
    , path(std::move(path_))
    , directory_remote_path(std::move(directory_remote_path_))
    , object_storage(std::move(object_storage_))
    , fs_tree(std::move(fs_tree_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
{
    chassert(path.empty() || path.string().ends_with('/'));
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::execute()
{
    const auto [exists_directory, info] = fs_tree->existsDirectory(path);
    if (info)
        return;

    if (fs_tree->existsFile(path))
        throw Exception(ErrorCodes::CANNOT_CREATE_DIRECTORY, "File '{}' already exists", path.parent_path());

    if (!recursive)
        if (!fs_tree->existsDirectory(path.parent_path().parent_path()).first)
            throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path.parent_path().parent_path());

    auto metadata_object_key = layout->constructDirectoryObjectKey(directory_remote_path);

    if (exists_directory)
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

    auto buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ 128,
        /*settings*/ getWriteSettings());

    writeString(
        serializePlainRewritablePrefixPath(
            PlainRewritablePrefixPath{.logical_path = path.string(), .explicit_files = false, .files = {}}),
        *buf);
    fiu_do_on(FailPoints::plain_object_storage_write_fail_on_directory_create, {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when creating '{}' directory", path);
    });
    buf->finalize();

    ProfileEvents::increment(metrics->directory_created);
    auto metadata = object_storage->getObjectMetadata(metadata_object.remote_path, /*with_tags=*/ false);
    fs_tree->recordDirectoryPath(path, DirectoryRemoteInfo{directory_remote_path, metadata.etag, metadata.last_modified.epochTime(), {}});
    created_directory = true;
}

void MetadataStorageFromPlainObjectStorageCreateDirectoryOperation::undo()
{
    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation"), "Reversing directory creation for path '{}'", path);

    if (created_directory)
        fs_tree->unlinkTree(path);

    auto metadata_object_key = layout->constructDirectoryObjectKey(directory_remote_path);
    object_storage->removeObjectIfExists(StoredObject(metadata_object_key, path));
}

MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , object_storage(std::move(object_storage_))
    , fs_tree(std::move(fs_tree_))
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
        const auto parsed = parsePlainRewritablePrefixPath(data);
        if (parsed.logical_path != expected_logical_path.value())
            throw Exception(
                ErrorCodes::INCORRECT_DATA,
                "Incorrect data for object key {}, expected logical path {}, got {}",
                metadata_object_key,
                expected_logical_path.value(),
                parsed.logical_path);
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
    const std::filesystem::path & to, const DirectoryRemoteInfo & remote_info, WriteBuffer & buffer)
{
    LOG_TRACE(
        getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"),
        "Rewriting directory metadata to '{}' (explicit_files={})",
        to,
        remote_info.explicit_files);

    PlainRewritablePrefixPath prefix_path;
    prefix_path.logical_path = to.string();
    prefix_path.explicit_files = remote_info.explicit_files;
    if (remote_info.explicit_files)
    {
        prefix_path.files.reserve(remote_info.files.size());
        for (const auto & [file_name, file_info] : remote_info.files)
        {
            const auto relative_key = file_info.object_key.empty()
                ? layout->makeRelativeFileObjectKey(remote_info.remote_path, file_name)
                : file_info.object_key;
            prefix_path.files.emplace_back(file_name, relative_key);
        }
    }

    writeString(serializePlainRewritablePrefixPath(prefix_path), buffer);

    fiu_do_on(FailPoints::plain_object_storage_write_fail_on_directory_move,
    {
        throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving directory to '{}'", to);
    });

    buffer.finalize();

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Updated directory metadata to '{}'", to);
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::execute()
{
#ifdef DEBUG_OR_SANITIZER_BUILD
    constexpr bool validate_content = true;
#else
    constexpr bool validate_content = false;
#endif

    if (!fs_tree->existsDirectory(path_from).first)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path_from);
    else if (fs_tree->existsDirectory(path_to).first)
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

        auto write_buf = createWriteBuf(
            remote_info.value(), /*expected_logical_path*/ validate_content ? std::make_optional(sub_path_from.string()) : std::nullopt);

        changed_paths.insert(sub_path_from);
        rewriteSingleDirectory(sub_path_to, remote_info.value(), *write_buf);
    }

    fs_tree->moveDirectory(path_from, path_to);
    moved_in_memory = true;
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::undo()
{
    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation"), "Reversing directory move from '{}' to '{}'", path_from, path_to);

    if (moved_in_memory)
        fs_tree->moveDirectory(path_to, path_from);

    for (const auto & [subdir, remote_info] : from_tree_info)
    {
        auto sub_path_to = path_to / subdir / "";
        auto sub_path_from = path_from / subdir / "";

        if (!changed_paths.contains(sub_path_from))
            continue;

        auto write_buf = createWriteBuf(remote_info.value(), /*expected_logical_path*/ std::nullopt);
        rewriteSingleDirectory(sub_path_from, remote_info.value(), *write_buf);
    }
}

MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
    std::filesystem::path path_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : path(std::move(path_))
    , object_storage(std::move(object_storage_))
    , fs_tree(std::move(fs_tree_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
{
    chassert(path.empty() || path.string().ends_with('/'));
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::execute()
{
    auto [exists, remote_info] = fs_tree->existsDirectory(path);
    if (!exists)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path);
    else if (auto children = fs_tree->listDirectory(path); !children.empty())
        throw Exception(ErrorCodes::CANNOT_RMDIR, "Directory '{}' is not empty. Children: [{}]", path, fmt::join(children, ", "));
    else if (normalizePath(path).empty())
        return;

    chassert(remote_info.has_value());
    info = std::move(remote_info.value());

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation"), "Removing directory '{}'", path);

    remove_attempted = true;
    auto metadata_object_key = layout->constructDirectoryObjectKey(info.remote_path);
    auto metadata_object = StoredObject(/*remote_path*/ metadata_object_key, /*local_path*/ path, path.string().length());
    object_storage->removeObjectIfExists(metadata_object);

    fs_tree->unlinkTree(path);
    ProfileEvents::increment(metrics->directory_removed);
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::undo()
{
    if (!remove_attempted)
        return;

    LOG_TRACE(getLogger("MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation"), "Reversing directory removal for '{}'", path);

    if (!fs_tree->existsDirectory(path).first)
        fs_tree->recordDirectoryPath(path, info);

    auto metadata_object_key = layout->constructDirectoryObjectKey(info.remote_path);
    auto metadata_object = StoredObject(metadata_object_key, path);

    auto buf = object_storage->writeObject(
        metadata_object,
        WriteMode::Rewrite,
        /*object_attributes*/ std::nullopt,
        /*buf_size*/ 128,
        /*settings*/ DB::getWriteSettings());
    writeString(
        serializePlainRewritablePrefixPath(
            PlainRewritablePrefixPath{.logical_path = path.string(), .explicit_files = false, .files = {}}),
        *buf);
    buf->finalize();
}
MetadataStorageFromPlainObjectStorageWriteFileOperation::MetadataStorageFromPlainObjectStorageWriteFileOperation(
    std::string path_,
    StoredObject object_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts_)
    : path(std::move(path_))
    , object(std::move(object_))
    , object_storage(std::move(object_storage_))
    , fs_tree(std::move(fs_tree_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , blob_refcounts(std::move(blob_refcounts_))
{
    chassert(metrics);
    chassert(blob_refcounts);
}

void MetadataStorageFromPlainObjectStorageWriteFileOperation::execute()
{
    LOG_TEST(getLogger("MetadataStorageFromPlainObjectStorageWriteFileOperation"), "Creating metadata for a file '{}', size: {}", path, object.bytes_size);

    if (fs_tree->existsFile(path))
        return;

    const auto normalized_path = normalizePath(path);
    auto directory_info = fs_tree->getDirectoryRemoteInfo(normalized_path.parent_path());
    if (!directory_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_path.parent_path().string());

    /// Prefer the real blob key when the caller passed an absolute object path (including random
    /// names in explicit directories). Unit tests sometimes pass a placeholder remote_path.
    if (!object.remote_path.empty() && object_storage->exists(StoredObject(object.remote_path)))
        relative_object_key = layout->makeRelativeObjectKey(object.remote_path);
    else
        relative_object_key = layout->makeRelativeFileObjectKey(directory_info->remote_path, normalized_path.filename());

    FileRemoteInfo file_info{
        .bytes_size = object.bytes_size,
        .last_modified = std::time(nullptr),
        .object_key = directory_info->explicit_files ? relative_object_key : std::string{},
    };

    fs_tree->recordFile(path, file_info);
    written = true;
    blob_refcounts->increment(relative_object_key);

    if (directory_info->explicit_files)
    {
        rewritePlainRewritableExplicitPrefixPath(*object_storage, *fs_tree, *layout, normalized_path.parent_path());
        rewrote_prefix_path = true;
    }
}

void MetadataStorageFromPlainObjectStorageWriteFileOperation::undo()
{
    if (!written)
        return;

    fs_tree->removeFile(path);
    blob_refcounts->decrement(relative_object_key);

    if (rewrote_prefix_path)
        rewritePlainRewritableExplicitPrefixPath(*object_storage, *fs_tree, *layout, normalizePath(path).parent_path());
}

MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation(
    std::filesystem::path path_,
    bool if_exists_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts_,
    StoredObjects & removed_objects_)
    : path(std::move(path_))
    , if_exists(if_exists_)
    , object_storage(object_storage_)
    , fs_tree(fs_tree_)
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , blob_refcounts(std::move(blob_refcounts_))
    , removed_objects(removed_objects_)
{
    chassert(metrics);
    chassert(blob_refcounts);
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

    file_remote_info = fs_tree->getFileRemoteInfo(path);

    const auto normalized_path = normalizePath(path);
    const auto parent_path = normalized_path.parent_path().string();
    auto directory_info = fs_tree->getDirectoryRemoteInfo(parent_path);
    if (!directory_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", parent_path);

    relative_object_key = resolvePlainRewritableRelativeObjectKey(*layout, *directory_info, normalized_path.filename(), *file_remote_info);
    remote_source_path = layout->constructObjectKey(relative_object_key);

    const auto refcount_before = blob_refcounts->get(relative_object_key);
    const bool has_external_links = refcount_before > 1;

    if (directory_info->explicit_files || has_external_links)
    {
        used_explicit_unlink = true;
        if (!directory_info->explicit_files)
            ensurePlainRewritableDirectoryExplicit(*object_storage, *fs_tree, *layout, parent_path);

        fs_tree->removeFile(path);
        rewritePlainRewritableExplicitPrefixPath(*object_storage, *fs_tree, *layout, parent_path);
        metadata_updated = true;

        const auto remaining = blob_refcounts->decrement(relative_object_key);
        blob_remove_scheduled = remaining == 0;
        return;
    }

    /// Implicit directory, sole link: keep the previous copy-to-tmp unlink protocol for undo safety.
    remote_tmp_path = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));

    copy_started = true;
    object_storage->copyObject(StoredObject(remote_source_path), StoredObject(remote_tmp_path), getReadSettings(), getWriteSettings());

    remove_started = true;
    object_storage->removeObjectIfExists(StoredObject(remote_source_path));

    remove_finished = true;
    fs_tree->removeFile(path);
    blob_refcounts->decrement(relative_object_key);
    blob_remove_scheduled = true;
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::undo()
{
    if (used_explicit_unlink)
    {
        if (!metadata_updated)
            return;

        chassert(file_remote_info.has_value());
        const auto parent_path = normalizePath(path).parent_path().string();
        fs_tree->recordFile(path, *file_remote_info);
        rewritePlainRewritableExplicitPrefixPath(*object_storage, *fs_tree, *layout, parent_path);
        blob_refcounts->increment(relative_object_key);
        blob_remove_scheduled = false;
        return;
    }

    if (!copy_started)
        return;

    chassert(file_remote_info.has_value());

    if (remove_started)
        object_storage->copyObject(StoredObject(remote_tmp_path), StoredObject(remote_source_path), getReadSettings(), getWriteSettings());

    object_storage->removeObjectIfExists(StoredObject(remote_tmp_path));

    if (remove_finished)
    {
        fs_tree->recordFile(path, std::move(file_remote_info.value()));
        blob_refcounts->increment(relative_object_key);
    }
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::finalize()
{
    if (blob_remove_scheduled)
        removed_objects.push_back(StoredObject(remote_source_path));

    if (copy_started)
        object_storage->removeObjectIfExists(StoredObject(remote_tmp_path));
}

MetadataStorageFromPlainObjectStorageHardLinkOperation::MetadataStorageFromPlainObjectStorageHardLinkOperation(
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , object_storage(std::move(object_storage_))
    , fs_tree(std::move(fs_tree_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , blob_refcounts(std::move(blob_refcounts_))
{
    chassert(metrics);
    chassert(blob_refcounts);
}

void MetadataStorageFromPlainObjectStorageHardLinkOperation::execute()
{
    LOG_TEST(getLogger("MetadataStorageFromPlainObjectStorageHardLinkOperation"), "Hard-linking '{}' to '{}'", path_from, path_to);

    if (!fs_tree->existsFile(path_from))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the source path '{}' does not exist", path_from);
    else if (auto [exists, remote_info] = fs_tree->existsDirectory(path_to.parent_path()); !exists)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path_to.parent_path());
    else if (!remote_info.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", path_to.parent_path());
    else if (fs_tree->existsFile(path_to))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

    const auto normalized_from = normalizePath(path_from);
    const auto normalized_to = normalizePath(path_to);
    const auto source_dir = fs_tree->getDirectoryRemoteInfo(normalized_from.parent_path());
    if (!source_dir)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' does not exist", normalized_from.parent_path().string());

    const auto source_file = fs_tree->getFileRemoteInfo(path_from).value();
    relative_object_key = resolvePlainRewritableRelativeObjectKey(*layout, *source_dir, normalized_from.filename(), source_file);

    const auto dest_parent = normalized_to.parent_path().string();
    ensurePlainRewritableDirectoryExplicit(*object_storage, *fs_tree, *layout, dest_parent);

    FileRemoteInfo dest_file = source_file;
    dest_file.object_key = relative_object_key;
    fs_tree->recordFile(path_to, dest_file);
    rewritePlainRewritableExplicitPrefixPath(*object_storage, *fs_tree, *layout, dest_parent);
    blob_refcounts->increment(relative_object_key);
    link_created = true;
}

void MetadataStorageFromPlainObjectStorageHardLinkOperation::undo()
{
    if (!link_created)
        return;

    LOG_WARNING(
        getLogger("MetadataStorageFromPlainObjectStorageHardLinkOperation"),
        "Removing hard link '{}' that was created from '{}'",
        path_to,
        path_from);

    const auto dest_parent = normalizePath(path_to).parent_path().string();
    if (fs_tree->existsFile(path_to))
        fs_tree->removeFile(path_to);
    rewritePlainRewritableExplicitPrefixPath(*object_storage, *fs_tree, *layout, dest_parent);
    blob_refcounts->decrement(relative_object_key);
}

MetadataStorageFromPlainObjectStorageMoveFileOperation::MetadataStorageFromPlainObjectStorageMoveFileOperation(
    bool replaceable_,
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts_,
    StoredObjects & removed_objects_)
    : replaceable(replaceable_)
    , path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , object_storage(std::move(object_storage_))
    , fs_tree(std::move(fs_tree_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , blob_refcounts(std::move(blob_refcounts_))
    , removed_objects(removed_objects_)
{
    chassert(metrics);
    chassert(blob_refcounts);
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
    else if (auto [exists, remote_info] = fs_tree->existsDirectory(path_to.parent_path()); !exists)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path_to.parent_path());
    else if (!remote_info.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", path_to.parent_path());

    const auto normalized_path_from = normalizePath(path_from);
    const auto normalized_path_to = normalizePath(path_to);
    auto directory_from = fs_tree->getDirectoryRemoteInfo(normalized_path_from.parent_path());
    auto directory_to = fs_tree->getDirectoryRemoteInfo(normalized_path_to.parent_path());
    if (!directory_from || !directory_to)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Source or destination directory does not exist");

    file_from_remote_info = fs_tree->getFileRemoteInfo(path_from).value();
    relative_object_key_from = resolvePlainRewritableRelativeObjectKey(
        *layout, *directory_from, normalized_path_from.filename(), *file_from_remote_info);

    /// object_key is populated for implicit files too (PR1 load path); only force a metadata-only
    /// move when a directory is already explicit or the blob is shared.
    const bool use_metadata_move = directory_from->explicit_files || directory_to->explicit_files
        || blob_refcounts->get(relative_object_key_from) > 1;

    if (use_metadata_move)
    {
        metadata_only_move = true;

        if (fs_tree->existsFile(path_to))
        {
            if (!replaceable)
                throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

            file_to_remote_info = fs_tree->getFileRemoteInfo(path_to);
            const auto replaced_key = resolvePlainRewritableRelativeObjectKey(
                *layout, *directory_to, normalized_path_to.filename(), *file_to_remote_info);
            fs_tree->removeFile(path_to);
            if (blob_refcounts->decrement(replaced_key) == 0)
                removed_objects.push_back(StoredObject(layout->constructObjectKey(replaced_key)));
            moved_existing_target_file = true;
        }

        ensurePlainRewritableDirectoryExplicit(*object_storage, *fs_tree, *layout, normalized_path_to.parent_path());
        if (!directory_from->explicit_files && normalized_path_from.parent_path() != normalized_path_to.parent_path())
            ensurePlainRewritableDirectoryExplicit(*object_storage, *fs_tree, *layout, normalized_path_from.parent_path());

        FileRemoteInfo moved_file_info = *file_from_remote_info;
        moved_file_info.object_key = relative_object_key_from;
        fs_tree->recordFile(path_to, moved_file_info);
        fs_tree->removeFile(path_from);

        rewritePlainRewritableExplicitPrefixPath(*object_storage, *fs_tree, *layout, normalized_path_to.parent_path());
        if (normalized_path_from.parent_path() != normalized_path_to.parent_path())
            rewritePlainRewritableExplicitPrefixPath(*object_storage, *fs_tree, *layout, normalized_path_from.parent_path());

        created_target_file = !moved_existing_target_file;
        moved_file = true;
        return;
    }

    const auto directory_remote_path_from = directory_from->remote_path;
    const auto directory_remote_path_to = directory_to->remote_path;

    /// Physical move is only used for unique blobs in implicit directories, so keys follow the
    /// conventional directory/filename layout.
    remote_path_from = layout->constructObjectKey(relative_object_key_from);
    relative_object_key_to = layout->makeRelativeFileObjectKey(directory_remote_path_to, normalized_path_to.filename());
    remote_path_to = layout->constructObjectKey(relative_object_key_to);
    tmp_remote_path_from = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));
    tmp_remote_path_to = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));
    const auto read_settings = getReadSettingsForMetadata();
    const auto write_settings = getWriteSettingsForMetadata();

    FileRemoteInfo dest_file_info = *file_from_remote_info;
    /// Keep the same convention as WriteFile: empty object_key means "derive from directory+name".
    dest_file_info.object_key.clear();

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

        file_to_remote_info = fs_tree->getFileRemoteInfo(path_to);
        relative_object_key_replaced = resolvePlainRewritableRelativeObjectKey(
            *layout, *directory_to, normalized_path_to.filename(), *file_to_remote_info);
        fs_tree->removeFile(path_to);
        blob_refcounts->decrement(relative_object_key_replaced);
        decremented_replaced = true;
        fs_tree->recordFile(path_to, dest_file_info);

        object_storage->removeObjectIfExists(StoredObject(remote_path_to));
    }
    else
    {
        fs_tree->recordFile(path_to, dest_file_info);
        created_target_file = true;
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
    blob_refcounts->decrement(relative_object_key_from);
    blob_refcounts->increment(relative_object_key_to);
    updated_blob_refcounts = true;
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::undo()
{
    if (metadata_only_move)
    {
        if (!moved_file)
            return;

        const auto normalized_path_from = normalizePath(path_from);
        const auto normalized_path_to = normalizePath(path_to);

        if (fs_tree->existsFile(path_to))
            fs_tree->removeFile(path_to);

        fs_tree->recordFile(path_from, file_from_remote_info.value());

        if (moved_existing_target_file)
        {
            fs_tree->recordFile(path_to, file_to_remote_info.value());
            const auto directory_to = fs_tree->getDirectoryRemoteInfo(normalized_path_to.parent_path()).value();
            const auto replaced_key = resolvePlainRewritableRelativeObjectKey(
                *layout, directory_to, normalized_path_to.filename(), *file_to_remote_info);
            blob_refcounts->increment(replaced_key);
        }

        rewritePlainRewritableExplicitPrefixPath(*object_storage, *fs_tree, *layout, normalized_path_from.parent_path());
        if (normalized_path_from.parent_path() != normalized_path_to.parent_path() || moved_existing_target_file)
            rewritePlainRewritableExplicitPrefixPath(*object_storage, *fs_tree, *layout, normalized_path_to.parent_path());
        return;
    }

    if (updated_blob_refcounts)
    {
        blob_refcounts->decrement(relative_object_key_to);
        blob_refcounts->increment(relative_object_key_from);
        updated_blob_refcounts = false;
    }

    if (decremented_replaced)
    {
        blob_refcounts->increment(relative_object_key_replaced);
        decremented_replaced = false;
    }

    if (moved_file)
        fs_tree->recordFile(path_from, file_from_remote_info.value());

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

        fs_tree->removeFile(path_to);
        fs_tree->recordFile(path_to, file_to_remote_info.value());

        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_to));
    }

    if (created_target_file)
    {
        fs_tree->removeFile(path_to);
    }
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::finalize()
{
    if (metadata_only_move)
        return;

    removed_objects.push_back(StoredObject(remote_path_from));

    if (moved_existing_source_file)
        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_from));

    if (moved_existing_target_file)
        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_to));
}

MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation(
    std::filesystem::path path_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    std::shared_ptr<PlainRewritableBlobRefcounts> blob_refcounts_,
    StoredObjects & removed_objects_)
    : path(std::move(path_))
    , object_storage(std::move(object_storage_))
    , fs_tree(std::move(fs_tree_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , blob_refcounts(std::move(blob_refcounts_))
    , removed_objects(removed_objects_)
    , log(getLogger("MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation"))
{
    chassert(metrics);
    chassert(blob_refcounts);
    tmp_path = getRandomASCIIString(16);
    move_to_tmp_op = std::make_unique<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation>(path / "", tmp_path / "", object_storage, fs_tree, layout, metrics);
}

void MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::execute()
{
    /// Unfortunately we are able to create merge tree unlinked from database directory.
    /// In this case during the dropAllData method removeRecursive can be called pointing to the root folder.
    /// I don't know what to do in this case, so right now it is a no-op.
    if (normalizePath(path).empty())
        return;

    if (fs_tree->existsDirectory(path).first)
    {
        move_tried = true;
        move_to_tmp_op->execute();
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
    std::unordered_map<std::string, uint32_t> links_in_subtree;

    for (const auto & [subdir, remote_info] : fs_tree->getSubtreeRemoteInfo(tmp_path))
    {
        auto subdir_path = tmp_path / subdir;

        if (!remote_info.has_value())
        {
            LOG_TRACE(log, "Directory '{}' is virtual, will not cleanup remote", subdir_path);
            continue;
        }

        auto metadata_object_key = layout->constructDirectoryObjectKey(remote_info->remote_path);
        objects_to_remove.emplace_back(metadata_object_key, path);

        for (const auto & child : fs_tree->listDirectory(subdir_path))
        {
            auto file_path = subdir_path / child;
            if (!fs_tree->existsFile(file_path))
                continue;

            const auto file_info = fs_tree->getFileRemoteInfo(file_path).value();
            const auto relative_key = resolvePlainRewritableRelativeObjectKey(*layout, *remote_info, child, file_info);
            ++links_in_subtree[relative_key];
        }
    }

    for (const auto & [relative_key, links] : links_in_subtree)
    {
        const auto total = blob_refcounts->get(relative_key);
        /// Delete the blob only when every remaining link lives inside the removed subtree.
        if (total <= links)
            objects_to_remove.emplace_back(layout->constructObjectKey(relative_key));

        for (uint32_t i = 0; i < links; ++i)
            blob_refcounts->decrement(relative_key);
    }

    fs_tree->unlinkTree(tmp_path);
    object_storage->removeObjectsIfExist(objects_to_remove);
    removed_objects.append_range(objects_to_remove);
}

}
