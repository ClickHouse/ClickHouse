#include <Disks/ObjectStorages/LocalDiskMetadataStorage.h>
#include <ranges>
#include <filesystem>

namespace DB
{

namespace
{

class SetLastModifiedOperation final : public IMetadataOperation
{
    std::string path;
    Poco::Timestamp new_timestamp;
    Poco::Timestamp old_timestamp;
    IDisk & disk;
public:
    SetLastModifiedOperation(const std::string & path_, Poco::Timestamp new_timestamp_, IDisk & disk_)
        : path(path_)
        , new_timestamp(new_timestamp_)
        , disk(disk_)
    {}

    void execute() override
    {
        old_timestamp = disk.getLastModified(path);
        disk.setLastModified(path, new_timestamp);
    }

    void undo() override
    {
        disk.setLastModified(path, old_timestamp);
    }
};

class UnlinkFileOperation final : public IMetadataOperation
{
    std::string path;
    IDisk & disk;
public:
    UnlinkFileOperation(const std::string & path_, IDisk & disk_)
        : path(path_)
        , disk(disk_)
    {
    }

    void execute() override
    {
        disk.removeFile(path);
    }


    void undo() override
    {
        /// TODO Do something with it
    }
};

class CreateDirectoryOperation final : public IMetadataOperation
{
private:
    std::string path;
    IDisk & disk;
public:

    CreateDirectoryOperation(const std::string & path_, IDisk & disk_)
        : path(path_)
        , disk(disk_)
    {
    }

    void execute() override
    {
        disk.createDirectory(path);
    }

    void undo() override
    {
        disk.removeDirectory(path);
    }
};

class CreateDirectoryRecursiveOperation : public IMetadataOperation
{
private:
    std::string path;
    std::vector<std::string> paths_created;
    IDisk & disk;
public:

    CreateDirectoryRecursiveOperation(const std::string & path_, IDisk & disk_)
        : path(path_)
        , disk(disk_)
    {
    }

    void execute() override
    {
        namespace fs = std::filesystem;
        fs::path p(path);
        while (!disk.exists(p))
        {
            disk.createDirectory(p);
            paths_created.push_back(p);
            if (!p.has_parent_path())
                break;
            p = p.parent_path();
        }
    }

    void undo() override
    {
        for (const auto & path_created : paths_created)
            disk.removeDirectory(path_created);
    }
};

class RemoveDirectoryOperation : public IMetadataOperation
{
private:
    std::string path;
    IDisk & disk;
public:
    RemoveDirectoryOperation(const std::string & path_, IDisk & disk_)
        : path(path_)
        , disk(disk_)
    {}

    void execute() override
    {
        disk.removeDirectory(path);
    }

    void undo() override
    {
        disk.createDirectory(path);
    }
};

class CreateHardlinkOperation : public IMetadataOperation
{
private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
public:
    CreateHardlinkOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_)
        : path_from(path_from_)
        , path_to(path_to_)
        , disk(disk_)
    {}

    void execute() override
    {
        disk.createHardLink(path_from, path_to);
    }

    void undo() override
    {
        disk.removeFile(path_to);
    }
};

class MoveFileOperation : public IMetadataOperation
{
private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
public:
    MoveFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_)
        : path_from(path_from_)
        , path_to(path_to_)
        , disk(disk_)
    {}

    void execute() override
    {
        disk.moveFile(path_from, path_to);
    }

    void undo() override
    {
        disk.moveFile(path_to, path_from);
    }
};

class ReplaceFileOperation : public IMetadataOperation
{
private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
public:
    ReplaceFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_)
        : path_from(path_from_)
        , path_to(path_to_)
        , disk(disk_)
    {}

    void execute() override
    {
        disk.replaceFile(path_from, path_to);
    }

    void undo() override
    {
        /// TODO something with this
    }
};

class WriteFileOperation: public IMetadataOperation
{
private:
    std::string path;
    std::string temp_path;
    IDisk & disk;
public:
    WriteFileOperation(const std::string & path_, const std::string & temp_path_, IDisk & disk_)
        : path(path_)
        , temp_path(temp_path_)
        , disk(disk_)
    {}

    void execute() override
    {
        disk.moveFile(temp_path, path);
    }

    void undo() override
    {
        disk.removeFileIfExists(path);
        disk.removeFileIfExists(temp_path);
    }
};

}

std::unique_ptr<WriteBufferFromFileBase> LocalDiskMetadataStorage::writeFile( /// NOLINT
     const std::string & path,
     MetadataTransactionPtr transaction,
     size_t buf_size,
     const WriteSettings & settings)
{
    std::string temp_path = path + "_tmp";
    transaction->addOperation(std::make_unique<WriteFileOperation>(path, temp_path, *local_disk));
    return local_disk->writeFile(temp_path, buf_size, WriteMode::Rewrite, settings);
}


void LocalDiskMetadataStorage::setLastModified(const std::string & path, const Poco::Timestamp & timestamp, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *local_disk));
}

void LocalDiskMetadataStorage::unlinkFile(const std::string & path, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<UnlinkFileOperation>(path, *local_disk));
}

void LocalDiskMetadataStorage::createDirectory(const std::string & path, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<CreateDirectoryOperation>(path, *local_disk));
}

void LocalDiskMetadataStorage::createDicrectoryRecursive(const std::string & path, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<CreateDirectoryRecursiveOperation>(path, *local_disk));
}

void LocalDiskMetadataStorage::removeDirectory(const std::string & path, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<RemoveDirectoryOperation>(path, *local_disk));
}

void LocalDiskMetadataStorage::createHardlink(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<CreateHardlinkOperation>(path_from, path_to, *local_disk));
}

void LocalDiskMetadataStorage::moveFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *local_disk));
}

void LocalDiskMetadataStorage::replaceFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *local_disk));
}


}
