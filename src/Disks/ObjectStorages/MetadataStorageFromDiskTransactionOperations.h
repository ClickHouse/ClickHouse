#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>

namespace DB
{
class IDisk;

/**
 * Implementations for transactional operations with metadata used by MetadataStorageFromDisk.
 */

struct IMetadataOperation
{
    virtual void execute() = 0;
    virtual void undo() = 0;
    virtual void finalize() {}
    virtual ~IMetadataOperation() = default;
};

using MetadataOperationPtr = std::unique_ptr<IMetadataOperation>;


struct SetLastModifiedOperation final : public IMetadataOperation
{
    SetLastModifiedOperation(const std::string & path_, Poco::Timestamp new_timestamp_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    Poco::Timestamp new_timestamp;
    Poco::Timestamp old_timestamp;
    IDisk & disk;
};


struct UnlinkFileOperation final : public IMetadataOperation
{
    UnlinkFileOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    IDisk & disk;
    std::string prev_data;
};


struct CreateDirectoryOperation final : public IMetadataOperation
{
    CreateDirectoryOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    IDisk & disk;
};


struct CreateDirectoryRecursiveOperation final : public IMetadataOperation
{
    CreateDirectoryRecursiveOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    std::vector<std::string> paths_created;
    IDisk & disk;
};


struct RemoveDirectoryOperation final : public IMetadataOperation
{
    RemoveDirectoryOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    IDisk & disk;
};

struct RemoveRecursiveOperation final : public IMetadataOperation
{
    RemoveRecursiveOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

    void finalize() override;

private:
    std::string path;
    IDisk & disk;
    std::string temp_path;
};


struct CreateHardlinkOperation final : public IMetadataOperation
{
    CreateHardlinkOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
};


struct MoveFileOperation final : public IMetadataOperation
{
    MoveFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
};


struct MoveDirectoryOperation final : public IMetadataOperation
{
    MoveDirectoryOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
};


struct ReplaceFileOperation final : public IMetadataOperation
{
    ReplaceFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_);

    void execute() override;

    void undo() override;

    void finalize() override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
    std::string temp_path_to;
};


struct WriteFileOperation final : public IMetadataOperation
{
    WriteFileOperation(const std::string & path_, IDisk & disk_, const std::string & data_);

    void execute() override;

    void undo() override;
private:
    std::string path;
    IDisk & disk;
    std::string data;
    bool existed = false;
    std::string prev_data;
};

}
