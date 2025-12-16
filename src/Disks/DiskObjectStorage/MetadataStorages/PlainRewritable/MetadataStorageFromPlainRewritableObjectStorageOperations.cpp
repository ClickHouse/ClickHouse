#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/InMemoryDirectoryTree.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
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

    writeString(path.string(), *buf);
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
        read_settings.remote_fs_method = RemoteFSReadMethod::threadpool;
        read_settings.remote_fs_prefetch = false;
        read_settings.remote_fs_buffer_size = 1024;

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

        auto write_buf = createWriteBuf(remote_info.value(), /*expected_content*/validate_content ? std::make_optional(sub_path_from) : std::nullopt);

        changed_paths.insert(sub_path_from);
        rewriteSingleDirectory(sub_path_from, sub_path_to, *write_buf);
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

        auto write_buf = createWriteBuf(remote_info.value(), /*expected_content*/std::nullopt);
        rewriteSingleDirectory(sub_path_to, sub_path_from, *write_buf);
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
    writeString(path.string(), *buf);
    buf->finalize();
}

MetadataStorageFromPlainObjectStorageWriteFileOperation::MetadataStorageFromPlainObjectStorageWriteFileOperation(
    std::string path_,
    StoredObject object_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : path(std::move(path_))
    , object(std::move(object_))
    , object_storage(std::move(object_storage_))
    , fs_tree(std::move(fs_tree_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
{
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageWriteFileOperation::execute()
{
    LOG_TEST(getLogger("MetadataStorageFromPlainObjectStorageWriteFileOperation"), "Creating metadata for a file '{}', size: {}", path, object.bytes_size);

    if (!fs_tree->existsFile(path))
    {
        fs_tree->recordFile(path, {object.bytes_size, std::time(nullptr)});
        written = true;
    }
}

void MetadataStorageFromPlainObjectStorageWriteFileOperation::undo()
{
    if (!written)
        return;

    fs_tree->removeFile(path);
}

MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation(
    std::filesystem::path path_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : path(std::move(path_))
    , object_storage(object_storage_)
    , fs_tree(fs_tree_)
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
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
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File '{}' does not exist", path);

    file_remote_info = fs_tree->getFileRemoteInfo(path);

    fs_tree->removeFile(path);
    unlinked = true;
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::undo()
{
    if (!unlinked)
        return;

    chassert(file_remote_info.has_value());
    fs_tree->recordFile(path, std::move(file_remote_info.value()));
}

MetadataStorageFromPlainObjectStorageCopyFileOperation::MetadataStorageFromPlainObjectStorageCopyFileOperation(
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
    chassert(metrics);
}

void MetadataStorageFromPlainObjectStorageCopyFileOperation::execute()
{
    LOG_TEST(getLogger("MetadataStorageFromPlainObjectStorageCopyFileOperation"), "Copying file from '{}' to '{}'", path_from, path_to);

    if (!fs_tree->existsFile(path_from))
        throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata object for the source path '{}' does not exist", path_from);
    else if (auto [exists, remote_info] = fs_tree->existsDirectory(path_to.parent_path()); !exists)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path_to.parent_path());
    else if (!remote_info.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", path_to.parent_path());
    else if (fs_tree->existsFile(path_to))
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

    const auto normalized_path_from = normalizePath(path_from);
    const auto directory_remote_path_from = fs_tree->getDirectoryRemoteInfo(normalized_path_from.parent_path())->remote_path;
    remote_path_from = layout->constructFileObjectKey(directory_remote_path_from, normalized_path_from.filename());

    const auto normalized_path_to = normalizePath(path_to);
    const auto directory_remote_path_to = fs_tree->getDirectoryRemoteInfo(normalized_path_to.parent_path())->remote_path;
    remote_path_to = layout->constructFileObjectKey(directory_remote_path_to, normalized_path_to.filename());

    copy_attempted = true;
    object_storage->copyObject(StoredObject(remote_path_from), StoredObject(remote_path_to), getReadSettings(), getWriteSettings());
    fs_tree->recordFile(path_to, fs_tree->getFileRemoteInfo(path_from).value());
}

void MetadataStorageFromPlainObjectStorageCopyFileOperation::undo()
{
    if (!copy_attempted)
        return;

    LOG_WARNING(
        getLogger("MetadataStorageFromPlainObjectStorageCopyFileOperation"),
        "Removing file '{}' that was copied from '{}",
        path_to,
        path_from);

    object_storage->removeObjectIfExists(StoredObject(remote_path_to));

    if (fs_tree->existsFile(path_to))
        fs_tree->removeFile(path_to);
}

MetadataStorageFromPlainObjectStorageMoveFileOperation::MetadataStorageFromPlainObjectStorageMoveFileOperation(
    bool replaceable_,
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<InMemoryDirectoryTree> fs_tree_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : replaceable(replaceable_)
    , path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , object_storage(std::move(object_storage_))
    , fs_tree(std::move(fs_tree_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
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
    else if (auto [exists, remote_info] = fs_tree->existsDirectory(path_to.parent_path()); !exists)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Directory '{}' does not exist", path_to.parent_path());
    else if (!remote_info.has_value())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Directory '{}' is virtual", path_to.parent_path());

    const auto normalized_path_from = normalizePath(path_from);
    const auto normalized_path_to = normalizePath(path_to);
    const auto directory_remote_path_from = fs_tree->getDirectoryRemoteInfo(normalized_path_from.parent_path())->remote_path;
    const auto directory_remote_path_to = fs_tree->getDirectoryRemoteInfo(normalized_path_to.parent_path())->remote_path;

    remote_path_from = layout->constructFileObjectKey(directory_remote_path_from, normalized_path_from.filename());
    remote_path_to = layout->constructFileObjectKey(directory_remote_path_to, normalized_path_to.filename());
    tmp_remote_path_from = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));
    tmp_remote_path_to = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));
    file_from_remote_info = fs_tree->getFileRemoteInfo(path_from).value();
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

        file_to_remote_info = fs_tree->getFileRemoteInfo(path_to);
        fs_tree->removeFile(path_to);
        fs_tree->recordFile(path_to, file_from_remote_info.value());

        object_storage->removeObjectIfExists(StoredObject(remote_path_to));
    }
    else
    {
        fs_tree->recordFile(path_to, file_from_remote_info.value());
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
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::undo()
{
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
    std::shared_ptr<PlainRewritableMetrics> metrics_)
    : path(std::move(path_))
    , object_storage(std::move(object_storage_))
    , fs_tree(std::move(fs_tree_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , log(getLogger("MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation"))
{
    chassert(metrics);
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
    for (const auto & [subdir, remote_info] : fs_tree->getSubtreeRemoteInfo(tmp_path))
    {
        auto subdir_path = tmp_path / subdir;

        if (!remote_info.has_value())
        {
            LOG_TRACE(log, "Directory '{}' is virtual, will not cleanup remote", subdir_path);
            continue;
        }

        LOG_TRACE(log, "Removing directory '{}'", subdir_path);

        /// Info should exist since it's lifetime is bounded to execution of this operation, because tmp path is unique.
        auto metadata_object_key = layout->constructDirectoryObjectKey(remote_info->remote_path);
        objects_to_remove.emplace_back(metadata_object_key, path);

        /// We also need to remove all files inside each of the subdirectories.
        for (const auto & child : fs_tree->listDirectory(subdir_path))
        {
            auto file_path = subdir_path / child;
            if (!fs_tree->existsFile(file_path))
                continue;

            LOG_TRACE(log, "Removing file '{}'", file_path);

            auto file_object_key = layout->constructFileObjectKey(remote_info->remote_path, child);
            objects_to_remove.emplace_back(file_object_key, file_path);
        }
    }

    fs_tree->unlinkTree(tmp_path);
    object_storage->removeObjectsIfExist(objects_to_remove);
}

}
