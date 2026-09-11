#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataOperation.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Plain/MetadataStorageFromPlainObjectStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableMetrics.h>
#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Transactions/Preconditions.h>

#include <filesystem>
#include <memory>
#include <optional>

namespace DB
{

/// A move of a file of a `plain_rewritable` disk is a copy of its blob followed by a delete of the
/// blob it copied. Both requests address the blob by the path of the file, so on an object storage
/// where a blob can be overwritten in place they may not be talking about the same content: another
/// writer can replace the blob between them, and a delete by path alone would then take away a
/// generation of the file that was never copied anywhere.
///
/// One `HEAD` names the generation before the copy. `copyObject` is then pinned to it (and needs no
/// `HEAD` of its own any more), and so is the delete: both transfer and remove exactly the
/// generation named here, or fail with `FILE_CHANGED_DURING_READ` and leave the file in place.
///
/// This is done for Azure only, the object storage whose delete honours the generation
/// (`AzureObjectStorage::removeObjectImpl` sends it as `If-Match`); for the others the object is
/// returned as it was and not a single extra request is made. An Azure endpoint that reports no
/// generation for the blob cannot be pinned to one at all, and the move is refused with
/// `AZURE_BLOB_STORAGE_ERROR` rather than made blind; a blob that the `HEAD` does not find at all
/// is refused with `FILE_DOESNT_EXIST` for the same reason, because a blob recreated after that
/// `HEAD` is a generation this operation has never named.
StoredObject pinToTheGenerationThatIsThereNow(IObjectStorage & object_storage, const std::filesystem::path & remote_path);

/// A rollback of one of the operations below may only put a blob back where the blob it saved aside
/// came from, so it may only write over a key that nobody has taken over in the meantime. Once the
/// execute side has deleted the source, the key is free, and another writer that recreates it holds
/// a generation that this transaction has never seen: copying the saved blob over it would be
/// exactly the loss that the generation pinning of the execute side exists to prevent. So the
/// rollback asks what is at the key now and refuses to run when something is, and it then leaves
/// the blob it saved aside in the bucket, so that the generation this transaction took away is
/// still there to be recovered by hand.
///
/// Only Azure is asked, because only Azure carries the generation of an object through these
/// operations; the other object storages delete and restore by path on the execute side too, and
/// this is not the place to change that.
bool aRollbackMayWriteOver(IObjectStorage & object_storage, const std::filesystem::path & remote_path);

/// Names the generation of a blob that was just written, so that a rollback that takes it back out
/// is pinned to it (`removeObjectIfExists` sends it as `If-Match`) and cannot take away a
/// generation that somebody else has written since. The `HEAD` runs right after the write, so the
/// generation it reports is the one that was written unless another writer got in between the two
/// requests; a copy that reported the generation it created would close that window, and the
/// `IObjectStorage` copy does not report one.
///
/// Nothing is returned when the blob is on Azure and the generation of it cannot be named at all -
/// the `HEAD` does not find the blob, or the endpoint answers without an `ETag`. A delete by path
/// alone is exactly the cross-generation loss the pinning exists to prevent, so the caller has to
/// fail closed rather than fall back to one. For every other object storage the object is returned
/// as it was and not a single extra request is made.
std::optional<StoredObject> nameTheGenerationThatWasJustWritten(IObjectStorage & object_storage, const std::filesystem::path & remote_path);

class MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation final : public IMetadataOperation
{
private:
    const std::shared_ptr<Preconditions> preconditions;
    const std::shared_ptr<FsSnapshot> fs_tree;

public:
    MetadataStorageFromPlainObjectStorageValidatePreconditionsOperation(
        std::shared_ptr<Preconditions> preconditions_,
        std::shared_ptr<FsSnapshot> fs_tree_);

    void execute() override;
};

class MetadataStorageFromPlainObjectStorageCreateDirectoryOperation final : public IMetadataOperation
{
private:
    const bool recursive;
    const std::filesystem::path path;
    const std::string directory_remote_path;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;

    bool write_attempted = false;

public:
    MetadataStorageFromPlainObjectStorageCreateDirectoryOperation(
        bool recursive_,
        std::filesystem::path path_,
        std::string directory_remote_path_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageMoveDirectoryOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path_from;
    const std::filesystem::path path_to;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;

    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> from_tree_info;
    std::unordered_set<std::string> changed_paths;

    std::unique_ptr<WriteBufferFromFileBase> createWriteBuf(const DirectoryRemoteInfo & remote_info, std::optional<std::string> expected_content);
    void rewriteSingleDirectory(const std::filesystem::path & from, const std::filesystem::path & to, WriteBuffer & buffer);

public:
    MetadataStorageFromPlainObjectStorageMoveDirectoryOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;

    DirectoryRemoteInfo info;
    bool remove_attempted = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveDirectoryOperation(
        std::filesystem::path path_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_);

    void execute() override;
    void undo() override;
};

class MetadataStorageFromPlainObjectStorageWriteFileOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const StoredObject object;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;

public:
    MetadataStorageFromPlainObjectStorageWriteFileOperation(
        std::string path_,
        StoredObject object_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_);

    void execute() override;
};

class MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const bool if_exists;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    StoredObjects & removed_objects;

    std::filesystem::path remote_source_path;
    std::filesystem::path remote_tmp_path;
    bool copy_started = false;
    bool remove_started = false;
    /// The delete of the source found a generation it had not copied aside and left it in place, so
    /// `undo` must not restore the copy over it.
    bool source_was_left_in_place = false;

public:
    MetadataStorageFromPlainObjectStorageUnlinkMetadataFileOperation(
        std::filesystem::path path_,
        bool if_exists_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        StoredObjects & removed_objects_);

    void execute() override;
    void undo() override;
    void finalize() override;
};

/// Throws an exception if path_to_ already exists.
class MetadataStorageFromPlainObjectStorageCopyFileOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path_from;
    const std::filesystem::path path_to;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;

    std::filesystem::path remote_path_from;
    std::filesystem::path remote_path_to;
    bool copy_attempted = false;

public:
    MetadataStorageFromPlainObjectStorageCopyFileOperation(
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_);

    void execute() override;
    void undo() override;
};

/**
 * @brief MetadataStorageFromPlainObjectStorageMoveFileOperation move file from {path_from, remote_path_from} to {path_to, remote_path_to}.
 *  If `replacable` is enabled, the target file will be replaced if exists. If disabled, the target file must not exist.
 *  Both source and target files must not be directories.
 */
class MetadataStorageFromPlainObjectStorageMoveFileOperation final : public IMetadataOperation
{
private:
    bool replaceable{false};
    const std::filesystem::path path_from;
    const std::filesystem::path path_to;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    StoredObjects & removed_objects;

    std::filesystem::path remote_path_from;
    std::filesystem::path remote_path_to;
    std::filesystem::path tmp_remote_path_from;
    std::filesystem::path tmp_remote_path_to;
    std::optional<FileRemoteInfo> file_from_remote_info;
    /// The source blob, pinned to the generation of it that this move carries.
    StoredObject source;
    bool moved_existing_source_file{false};
    bool moved_existing_target_file{false};
    /// A delete found a generation of the blob that this move had not copied aside and left it in
    /// place, so `undo` must not restore the copy of the generation before it.
    bool source_was_left_in_place{false};
    bool target_was_left_in_place{false};
    /// The copy to the destination succeeded. It is set before the delete of the source, which can
    /// fail on its own, so that `undo` takes the blob it wrote back out even then: the object of a
    /// move that was never committed is not harmless garbage, because
    /// `MetadataStorageFromPlainRewritableObjectStorage::load` rebuilds the files of a directory
    /// from the blobs that are in the bucket, so leaving it there resurrects `path_to` on restart.
    bool copied_to_destination{false};
    /// The generation of the destination blob as it was right after the copy wrote it, so that the
    /// delete in `undo` is pinned to it and cannot take away a generation written by somebody else.
    StoredObject destination;
    /// Whether `destination` names a generation. The execute side refuses to go on without one, so
    /// `undo` only ever sees it unset for a move that was refused for exactly that reason, and it
    /// then leaves the blob the copy wrote alone instead of deleting the key blindly.
    bool destination_generation_is_named{false};

public:
    MetadataStorageFromPlainObjectStorageMoveFileOperation(
        bool replaceable_,
        std::filesystem::path path_from_,
        std::filesystem::path path_to_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        StoredObjects & removed_objects_);
    /**
     * @brief Move a file from remote_path_from to remote_path_to
     *  1. Copy remote_path_to (if exists) to tmp_remote_path_from, which is used to restore the target file in case of failure.
     *  2. Copy remote_path_from to tmp_remote_path_to, which is used to restore the source file in case of failure.
     *  3. Copy remote_path_from to remote_path_to.
     *  4. Remove remote_path_to.
     *  5. Update fs_tree
     */
    void execute() override;
    /**
     * @brief Undo the `execute` logic:
     *  1. If remote_path_from is copied to remote_path_to, remove remote_path_to. The delete is
     *     pinned to the generation that the copy wrote, and a generation that somebody else has
     *     written since is left in place.
     *  2. Restore remote_path_from from tmp_remote_path_from if it is copied, unless a blob that
     *     this move never carried is at remote_path_from by then.
     *  3. Restore remote_path_to from tmp_remote_path_to if it is copied, under the same condition.
     *  5. Update fs_tree
     *
     * A restore that is refused leaves the blob it would have restored in the bucket, at the
     * temporary key named in the log, rather than destroying either generation.
     */
    void undo() override;
    /**
     * @brief Finalize `execute` logic
     *  1. Remove tmp_remote_path_from if exists
     *  2. Remove tmp_remote_path_to if exists
     */
    void finalize() override;
};

class MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation final : public IMetadataOperation
{
private:
    const std::filesystem::path path;
    const std::shared_ptr<FsSnapshot> fs_tree;
    const std::shared_ptr<IObjectStorage> object_storage;
    const std::shared_ptr<PlainRewritableLayout> layout;
    const std::shared_ptr<PlainRewritableMetrics> metrics;
    StoredObjects & removed_objects;

    const LoggerPtr log;

    std::filesystem::path tmp_path;
    std::unique_ptr<MetadataStorageFromPlainObjectStorageMoveDirectoryOperation> move_to_tmp_op;
    std::unordered_map<std::string, std::optional<DirectoryRemoteInfo>> subtree_remote_info;
    bool move_tried = false;

public:
    MetadataStorageFromPlainObjectStorageRemoveRecursiveOperation(
        std::filesystem::path path_,
        std::shared_ptr<FsSnapshot> fs_tree_,
        std::shared_ptr<IObjectStorage> object_storage_,
        std::shared_ptr<PlainRewritableLayout> layout_,
        std::shared_ptr<PlainRewritableMetrics> metrics_,
        StoredObjects & removed_objects_);

    void execute() override;
    void undo() override;
    void finalize() override;
};

}
