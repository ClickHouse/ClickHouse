#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/MetadataStorageFromPlainRewritableObjectStorageOperations.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
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
    extern const char plain_object_storage_fail_on_directory_move_undo[];
    extern const char plain_object_storage_fail_on_file_move_undo[];
    extern const char plain_object_storage_fail_after_copy_on_file_move[];
    extern const char plain_object_storage_pause_on_directory_move[];
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
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    UndoRetriesPtr undo_retries_)
    : recursive(recursive_)
    , path(std::move(path_))
    , directory_remote_path(std::move(directory_remote_path_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , undo_retries(std::move(undo_retries_))
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

    undo_prepared = true;
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
    auto log = getLogger("MetadataStorageFromPlainObjectStorageCreateDirectoryOperation");
    LOG_TRACE(log, "Reversing directory creation for path '{}'", path);

    if (!undo_prepared)
        return;

    undo_retries->runStage(log, fmt::format("remove the metadata of the directory '{}'", path), [&]
    {
        auto metadata_object_key = layout->constructDirectoryObjectKey(directory_remote_path);
        object_storage->removeObjectIfExists(StoredObject(metadata_object_key, path));
    });
}

MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    UndoRetriesPtr undo_retries_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , undo_retries(std::move(undo_retries_))
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

        rewriteSingleDirectory(sub_path_from, sub_path_to, *write_buf);

        /// Lets a test hold the move once one marker carries its new path, so that it can decide what the rest of the
        /// move and the reversal of this marker run into.
        FailPointInjection::pauseFailPoint(FailPoints::plain_object_storage_pause_on_directory_move);
    }

    fs_tree->moveDirectory(path_from, path_to);
}

void MetadataStorageFromPlainObjectStorageMoveDirectoryOperation::undo()
{
    auto log = getLogger("MetadataStorageFromPlainObjectStorageMoveDirectoryOperation");
    LOG_TRACE(log, "Reversing directory move from '{}' to '{}'", path_from, path_to);

    /// Every marker of the subtree is rewritten, not only the ones `execute` reported as written. The old logical path
    /// of each is known here, so rewriting a marker that `execute` never reached costs one write and changes nothing,
    /// while depending on what `execute` saw succeed would leave a marker behind when a write reported a failure after
    /// it had landed.
    for (const auto & [subdir, remote_info] : from_tree_info)
    {
        auto sub_path_to = path_to / subdir / "";
        auto sub_path_from = path_from / subdir / "";

        if (!remote_info.has_value())
            continue;

        /// One stage per directory, so a marker that is back under its old path is never rewritten again.
        undo_retries->runStage(log, fmt::format("restore the metadata of the directory '{}'", sub_path_from), [&]
        {
            /// Injected here rather than in `rewriteSingleDirectory`, which the forward pass calls first: a fault there
            /// can never leave a move half reversed.
            fiu_do_on(FailPoints::plain_object_storage_fail_on_directory_move_undo,
            {
                throw Exception(
                    ErrorCodes::FAULT_INJECTED, "Injecting fault when reversing the move from '{}' to '{}'", sub_path_to, sub_path_from);
            });

            auto write_buf = createWriteBuf(remote_info.value(), /*expected_content*/std::nullopt);
            rewriteSingleDirectory(sub_path_to, sub_path_from, *write_buf);
        });
    }
}

MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
    std::filesystem::path path_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    UndoRetriesPtr undo_retries_)
    : path(std::move(path_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , undo_retries(std::move(undo_retries_))
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

    undo_prepared = true;
    auto metadata_object_key = layout->constructDirectoryObjectKey(info.remote_path);
    auto metadata_object = StoredObject(/*remote_path*/ metadata_object_key, /*local_path*/ path, path.string().length());
    object_storage->removeObjectIfExists(metadata_object);

    fs_tree->removeDirectory(path);
    ProfileEvents::increment(metrics->directory_removed);
}

void MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation::undo()
{
    if (!undo_prepared)
        return;

    auto log = getLogger("MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation");
    LOG_TRACE(log, "Reversing directory removal for '{}'", path);

    undo_retries->runStage(log, fmt::format("restore the metadata of the directory '{}'", path), [&]
    {
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
    });
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
    UndoRetriesPtr undo_retries_,
    StoredObjects & removed_objects_)
    : path(std::move(path_))
    , if_exists(if_exists_)
    , fs_tree(std::move(fs_tree_))
    , object_storage(object_storage_)
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , undo_retries(std::move(undo_retries_))
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
    remote_tmp_path = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));

    /// Both keys are known and nothing has been written yet. Past this line the reversal runs, and it converges on the
    /// blob being back under its own key rather than on what this method saw succeed.
    blob_removal_prepared = true;

    object_storage->copyObject(StoredObject(remote_source_path), StoredObject(remote_tmp_path), getReadSettings(), getWriteSettings());
    object_storage->removeObjectIfExists(StoredObject(remote_source_path));

    fs_tree->removeFile(path);
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::undo()
{
    if (!blob_removal_prepared)
        return;

    auto log = getLogger("MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation");

    /// The restore states where the blob has to end up and asks object storage whether it is already there, so it
    /// holds whether the removal never ran, ran, or ran and lost its answer. The temporary copy is dropped in a later
    /// stage, so a failure never leaves the reversal without the copy it still needs.
    undo_retries->runStage(log, fmt::format("restore the blob of the file '{}'", path), [&]
    {
        if (object_storage->exists(StoredObject(remote_source_path)))
            return;

        if (!object_storage->exists(StoredObject(remote_tmp_path)))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot restore the blob of the file '{}': it is absent both under its own key '{}' and under the "
                "temporary key '{}' the removal copied it to",
                path,
                remote_source_path,
                remote_tmp_path);

        object_storage->copyObject(StoredObject(remote_tmp_path), StoredObject(remote_source_path), getReadSettings(), getWriteSettings());
    });

    undo_retries->runStage(log, fmt::format("remove the temporary copy of the blob of the file '{}'", path), [&]
    {
        object_storage->removeObjectIfExists(StoredObject(remote_tmp_path));
    });
}

void MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation::finalize()
{
    removed_objects.push_back(StoredObject(remote_source_path));

    if (blob_removal_prepared)
        object_storage->removeObjectIfExists(StoredObject(remote_tmp_path));
}

MetadataStorageFromPlainObjectStorageCopyFileOperation::MetadataStorageFromPlainObjectStorageCopyFileOperation(
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    UndoRetriesPtr undo_retries_)
    : path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , undo_retries(std::move(undo_retries_))
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

    undo_prepared = true;
    object_storage->copyObject(StoredObject(remote_path_from), StoredObject(remote_path_to), getReadSettings(), getWriteSettings());
    fs_tree->recordFile(path_to, fs_tree->getFileRemoteInfo(path_from).value());
}

void MetadataStorageFromPlainObjectStorageCopyFileOperation::undo()
{
    if (!undo_prepared)
        return;

    auto log = getLogger("MetadataStorageFromPlainObjectStorageCopyFileOperation");
    LOG_WARNING(log, "Removing file '{}' that was copied from '{}", path_to, path_from);

    /// The target held no file of this filesystem before the copy, so the key has to be empty again whether or not the
    /// copy reported success.
    undo_retries->runStage(log, fmt::format("remove the copy of the file '{}'", path_to), [&]
    {
        object_storage->removeObjectIfExists(StoredObject(remote_path_to));
    });
}

MetadataStorageFromPlainObjectStorageMoveFileOperation::MetadataStorageFromPlainObjectStorageMoveFileOperation(
    bool replaceable_,
    std::filesystem::path path_from_,
    std::filesystem::path path_to_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    UndoRetriesPtr undo_retries_,
    StoredObjects & removed_objects_)
    : replaceable(replaceable_)
    , path_from(std::move(path_from_))
    , path_to(std::move(path_to_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , undo_retries(std::move(undo_retries_))
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
    tmp_remote_path_from = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));
    tmp_remote_path_to = layout->constructFileObjectKey(PlainRewritableLayout::ROOT_DIRECTORY_TOKEN, getRandomASCIIString(16));
    file_from_remote_info = fs_tree->getFileRemoteInfo(path_from).value();
    const auto read_settings = getReadSettingsForMetadata();
    const auto write_settings = getWriteSettingsForMetadata();

    had_existing_target = fs_tree->existsFile(path_to);
    if (had_existing_target && !replaceable)
        throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "Target file '{}' already exists", path_to);

    /// Everything the reversal needs is known now, and nothing has been written yet. Past this line the reversal runs,
    /// and it converges on the state recorded here rather than on what this method managed to do - an object storage
    /// call that writes and then reports a failure must not be able to hide a step from it.
    blob_move_prepared = true;

    if (had_existing_target)
    {
        fiu_do_on(FailPoints::plain_object_storage_copy_temp_target_file_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });

        object_storage->copyObject(
            /*object_from=*/StoredObject(remote_path_to),
            /*object_to=*/StoredObject(tmp_remote_path_to),
            read_settings,
            write_settings);

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
    }

    {
        fiu_do_on(FailPoints::plain_object_storage_copy_fail_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault when moving from '{}' to '{}'", path_from, path_to);
        });
        object_storage->copyObject(
            /*object_from=*/StoredObject(remote_path_from), /*object_to=*/StoredObject(remote_path_to), read_settings, write_settings);

        /// Fires once the blob is published but before this method knows it, which is the shape of a client that
        /// writes and then reports a failure.
        fiu_do_on(FailPoints::plain_object_storage_fail_after_copy_on_file_move, {
            throw Exception(ErrorCodes::FAULT_INJECTED, "Injecting fault after moving from '{}' to '{}'", path_from, path_to);
        });

        object_storage->removeObjectIfExists(StoredObject(remote_path_from));
    }

    fs_tree->removeFile(path_from);
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::undo()
{
    if (!blob_move_prepared)
        return;

    const auto read_settings = getReadSettings();
    const auto write_settings = getWriteSettings();

    auto log = getLogger("MetadataStorageFromPlainObjectStorageMoveFileOperation");
    LOG_WARNING(log, "Reversing the move (replaceable = {}) of '{}' to '{}'", replaceable, path_from, path_to);

    /// Each stage states where one key has to end up and asks object storage whether it is already there. That answer
    /// holds whether the matching step of `execute` never ran, ran, or ran and lost its answer, so no stage depends on
    /// this operation having seen its own writes succeed.
    undo_retries->runStage(log, fmt::format("restore the blob of the source file '{}'", path_from), [&]
    {
        if (object_storage->exists(StoredObject(remote_path_from)))
            return;

        if (!object_storage->exists(StoredObject(tmp_remote_path_from)))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot restore the blob of the file '{}': it is absent both under its own key '{}' and under the "
                "temporary key '{}' the move copied it to",
                path_from,
                remote_path_from,
                tmp_remote_path_from);

        object_storage->copyObject(
            /*object_from=*/StoredObject(tmp_remote_path_from),
            /*object_to=*/StoredObject(remote_path_from),
            read_settings,
            write_settings);
    });

    undo_retries->runStage(log, fmt::format("restore the blob of the target file '{}'", path_to), [&]
    {
        fiu_do_on(FailPoints::plain_object_storage_fail_on_file_move_undo,
        {
            throw Exception(
                ErrorCodes::FAULT_INJECTED, "Injecting fault when reversing the move from '{}' to '{}'", path_from, path_to);
        });

        if (!had_existing_target)
        {
            /// The move published the source blob under a key that held no file of this filesystem, so the key has to
            /// be empty again. Nothing of value can be there, whether or not the publishing copy reported success.
            object_storage->removeObjectIfExists(StoredObject(remote_path_to));
            return;
        }

        if (object_storage->exists(StoredObject(tmp_remote_path_to)))
        {
            object_storage->copyObject(
                /*object_from=*/StoredObject(tmp_remote_path_to),
                /*object_to=*/StoredObject(remote_path_to),
                read_settings,
                write_settings);
            return;
        }

        /// The copy that puts the target aside runs before anything overwrites or removes the target, so without that
        /// copy the target is still the blob this transaction found.
        if (!object_storage->exists(StoredObject(remote_path_to)))
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Cannot restore the blob of the file '{}': it is absent both under its own key '{}' and under the "
                "temporary key '{}' the move copied it to",
                path_to,
                remote_path_to,
                tmp_remote_path_to);
    });

    /// The temporary copies go last, so a stage that fails never leaves the reversal without a copy it still needs.
    undo_retries->runStage(log, fmt::format("remove the temporary copy of the blob of the source file '{}'", path_from), [&]
    {
        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_from));
    });

    undo_retries->runStage(log, fmt::format("remove the temporary copy of the blob of the target file '{}'", path_to), [&]
    {
        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_to));
    });
}

void MetadataStorageFromPlainObjectStorageMoveFileOperation::finalize()
{
    removed_objects.push_back(StoredObject(remote_path_from));

    if (blob_move_prepared)
    {
        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_from));
        object_storage->removeObjectIfExists(StoredObject(tmp_remote_path_to));
    }
}

MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation::MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation(
    std::filesystem::path path_,
    std::shared_ptr<FsSnapshot> fs_tree_,
    std::shared_ptr<IObjectStorage> object_storage_,
    std::shared_ptr<PlainRewritableLayout> layout_,
    std::shared_ptr<PlainRewritableMetrics> metrics_,
    UndoRetriesPtr undo_retries_,
    StoredObjects & removed_objects_)
    : path(std::move(path_))
    , fs_tree(std::move(fs_tree_))
    , object_storage(std::move(object_storage_))
    , layout(std::move(layout_))
    , metrics(std::move(metrics_))
    , undo_retries(std::move(undo_retries_))
    , removed_objects(removed_objects_)
    , log(getLogger("MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation"))
{
    chassert(metrics);
    tmp_path = getRandomASCIIString(16);
    move_to_tmp_op = std::make_unique<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation>(path / "", tmp_path / "", fs_tree, object_storage, layout, metrics, undo_retries);
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
