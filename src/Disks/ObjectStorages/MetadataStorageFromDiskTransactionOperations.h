#pragma once

#include <Disks/ObjectStorages/IMetadataStorage.h>

namespace DB
{
class IDisk;

class SetLastModifiedOperation final : public IMetadataOperation
{
public:
    SetLastModifiedOperation(const std::string & path_, Poco::Timestamp new_timestamp_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    Poco::Timestamp new_timestamp;
    Poco::Timestamp old_timestamp;
    IDisk & disk;
};


class UnlinkFileOperation final : public IMetadataOperation
{
public:
    UnlinkFileOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    IDisk & disk;
    std::string prev_data;
};


class CreateDirectoryOperation final : public IMetadataOperation
{
public:
    CreateDirectoryOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    IDisk & disk;
};


class CreateDirectoryRecursiveOperation final : public IMetadataOperation
{
public:
    CreateDirectoryRecursiveOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    std::vector<std::string> paths_created;
    IDisk & disk;
};


class RemoveDirectoryOperation final : public IMetadataOperation
{
public:
    RemoveDirectoryOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    IDisk & disk;
};

class RemoveRecursiveOperation final : public IMetadataOperation
{
public:
    RemoveRecursiveOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

    void finalize() override;

private:
    std::string path;
    IDisk & disk;
    std::string temp_path;
};


class CreateHardlinkOperation final : public IMetadataOperation
{
public:
    CreateHardlinkOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
};


class MoveFileOperation final : public IMetadataOperation
{
public:
    MoveFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
};


class MoveDirectoryOperation final : public IMetadataOperation
{
public:
    MoveDirectoryOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
};


class ReplaceFileOperation final : public IMetadataOperation
{
public:
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


class WriteFileOperation final : public IMetadataOperation
{
public:
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

class SetReadOnlyOperation final : public IMetadataOperation
{
public:
    SetReadOnlyOperation(const std::string & path_, IDisk & disk_);

    void execute() override;

    void undo() override;

private:
    std::string path;
    IDisk & disk;
};

}
