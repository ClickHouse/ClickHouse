#pragma once

#include <Disks/ObjectStorages/IMetadataOperation.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>

#include <numeric>

namespace DB
{
class MetadataStorageFromDisk;
class IDisk;

/**
 * Implementations for transactional operations with metadata used by MetadataStorageFromDisk.
 */

struct SetLastModifiedOperation final : public IMetadataOperation
{
    SetLastModifiedOperation(const std::string & path_, Poco::Timestamp new_timestamp_, IDisk & disk_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    Poco::Timestamp new_timestamp;
    Poco::Timestamp old_timestamp;
    IDisk & disk;
};

struct ChmodOperation final : public IMetadataOperation
{
    ChmodOperation(const std::string & path_, mode_t mode_, IDisk & disk_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    mode_t mode;
    mode_t old_mode;
    IDisk & disk;
};


struct UnlinkFileOperation final : public IMetadataOperation
{
    UnlinkFileOperation(const std::string & path_, IDisk & disk_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    IDisk & disk;
    std::string prev_data;
};


struct CreateDirectoryOperation final : public IMetadataOperation
{
    CreateDirectoryOperation(const std::string & path_, IDisk & disk_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    IDisk & disk;
};


struct CreateDirectoryRecursiveOperation final : public IMetadataOperation
{
    CreateDirectoryRecursiveOperation(const std::string & path_, IDisk & disk_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    std::vector<std::string> paths_created;
    IDisk & disk;
};


struct RemoveDirectoryOperation final : public IMetadataOperation
{
    RemoveDirectoryOperation(const std::string & path_, IDisk & disk_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    IDisk & disk;
};

struct RemoveRecursiveOperation final : public IMetadataOperation
{
    RemoveRecursiveOperation(const std::string & path_, IDisk & disk_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

    void finalize() override;

private:
    std::string path;
    IDisk & disk;
    std::string temp_path;
};

struct WriteFileOperation final : public IMetadataOperation
{
    WriteFileOperation(const std::string & path_, IDisk & disk_, const std::string & data_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    IDisk & disk;
    std::string data;
    bool existed = false;
    std::string prev_data;
};

struct CreateHardlinkOperation final : public IMetadataOperation
{
    CreateHardlinkOperation(
        const std::string & path_from_,
        const std::string & path_to_,
        IDisk & disk_,
        const MetadataStorageFromDisk & metadata_storage_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
    std::unique_ptr<WriteFileOperation> write_operation;
    const MetadataStorageFromDisk & metadata_storage;
};


struct MoveFileOperation final : public IMetadataOperation
{
    MoveFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
};


struct MoveDirectoryOperation final : public IMetadataOperation
{
    MoveDirectoryOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
};


struct ReplaceFileOperation final : public IMetadataOperation
{
    ReplaceFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_);

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

    void finalize() override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
    std::string temp_path_to;
};


struct WriteInlineDataOperation final : public IMetadataOperation
{
    WriteInlineDataOperation(
        const std::string & path_,
        const std::string & inline_data_,
        IDisk & disk_,
        const MetadataStorageFromDisk & metadata_storage_)
        : path(path_)
        , inline_data(inline_data_)
        , disk(disk_)
        , metadata_storage(metadata_storage_)
    {}

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    String inline_data;
    IDisk & disk;
    const MetadataStorageFromDisk & metadata_storage;

    std::unique_ptr<WriteFileOperation> write_operation;
};


struct RewriteFileOperation final : public IMetadataOperation
{
    RewriteFileOperation(
        const std::string & path_,
        ObjectStorageKey object_key_,
        uint64_t size_in_bytes_,
        IDisk & disk_,
        const MetadataStorageFromDisk & metadata_storage_)
        : path(path_)
        , object_key(std::move(object_key_))
        , size_in_bytes(size_in_bytes_)
        , disk(disk_)
        , metadata_storage(metadata_storage_)
    {}

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    ObjectStorageKey object_key;
    uint64_t size_in_bytes;
    IDisk & disk;
    const MetadataStorageFromDisk & metadata_storage;

    std::unique_ptr<WriteFileOperation> write_operation;
};


struct AddBlobOperation final : public IMetadataOperation
{
    AddBlobOperation(
        const std::string & path_,
        ObjectStorageKey object_key_,
        uint64_t size_in_bytes_,
        IDisk & disk_,
        const MetadataStorageFromDisk & metadata_storage_)
        : path(path_)
        , object_key(std::move(object_key_))
        , size_in_bytes(size_in_bytes_)
        , disk(disk_)
        , metadata_storage(metadata_storage_)
    {}

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    ObjectStorageKey object_key;
    uint64_t size_in_bytes;
    IDisk & disk;
    const MetadataStorageFromDisk & metadata_storage;

    std::unique_ptr<WriteFileOperation> write_operation;
};

struct UnlinkMetadataFileOperation final : public IMetadataOperation
{
    const UnlinkMetadataFileOperationOutcomePtr outcome = std::make_shared<UnlinkMetadataFileOperationOutcome>();

    UnlinkMetadataFileOperation(
        const std::string & path_,
        IDisk & disk_,
        const MetadataStorageFromDisk & metadata_storage_)
        : path(path_)
        , disk(disk_)
        , metadata_storage(metadata_storage_)
    {
    }

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    IDisk & disk;
    const MetadataStorageFromDisk & metadata_storage;

    std::unique_ptr<WriteFileOperation> write_operation;
    std::unique_ptr<UnlinkFileOperation> unlink_operation;
};

struct SetReadonlyFileOperation final : public IMetadataOperation
{
    SetReadonlyFileOperation(
        const std::string & path_,
        IDisk & disk_,
        const MetadataStorageFromDisk & metadata_storage_)
        : path(path_)
        , disk(disk_)
        , metadata_storage(metadata_storage_)
    {
    }

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & metadata_lock) override;

private:
    std::string path;
    IDisk & disk;
    const MetadataStorageFromDisk & metadata_storage;

    std::unique_ptr<WriteFileOperation> write_operation;
};

struct TruncateMetadataFileOperation final : public IMetadataOperation
{
    const TruncateFileOperationOutcomePtr outcome = std::make_shared<TruncateFileOperationOutcome>();

    TruncateMetadataFileOperation(
        const std::string & path_,
        size_t target_size_,
        const MetadataStorageFromDisk & metadata_storage_,
        IDisk & disk_)
        : path(path_)
        , target_size(target_size_)
        , metadata_storage(metadata_storage_)
        , disk(disk_)
    {
    }

    void execute(std::unique_lock<SharedMutex> & metadata_lock) override;

    void undo(std::unique_lock<SharedMutex> & lock) override;

private:
    std::string path;
    size_t target_size;

    const MetadataStorageFromDisk & metadata_storage;
    IDisk & disk;

    std::unique_ptr<WriteFileOperation> write_operation;
};

}
