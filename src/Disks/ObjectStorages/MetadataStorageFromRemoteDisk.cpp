#include <Disks/ObjectStorages/MetadataStorageFromRemoteDisk.h>
#include <Disks/ObjectStorages/IMetadataStorage.h>
#include <Disks/TemporaryFileOnDisk.h>
#include <Common/getRandomASCIIString.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Poco/TemporaryFile.h>
#include <ranges>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int FS_METADATA_ERROR;
}

namespace
{

std::string getTempFileName(const std::string & dir)
{
    return fs::path(dir) / getRandomASCIIString();
}

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
    std::string prev_data;
public:
    UnlinkFileOperation(const std::string & path_, IDisk & disk_)
        : path(path_)
        , disk(disk_)
    {
    }

    void execute() override
    {
        auto buf = disk.readFile(path);
        readStringUntilEOF(prev_data, *buf);
        disk.removeFile(path);
    }

    void undo() override
    {
        auto buf = disk.writeFile(path);
        writeString(prev_data, *buf);
        buf->finalize();
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

class CreateDirectoryRecursiveOperation final : public IMetadataOperation
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
            paths_created.push_back(p);
            if (!p.has_parent_path())
                break;
            p = p.parent_path();
        }
        for (const auto & path_to_create : paths_created | std::views::reverse)
            disk.createDirectory(path_to_create);
    }

    void undo() override
    {
        for (const auto & path_created : paths_created)
            disk.removeDirectory(path_created);
    }
};

class RemoveDirectoryOperation final : public IMetadataOperation
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

class RemoveRecursiveOperation final : public IMetadataOperation
{
    std::string path;
    IDisk & disk;
    std::string temp_path;
public:
    RemoveRecursiveOperation(const std::string & path_, IDisk & disk_)
        : path(path_)
        , disk(disk_)
        , temp_path(getTempFileName(fs::path(path).parent_path()))
    {
    }

    void execute() override
    {
        if (disk.isFile(path))
            disk.moveFile(path, temp_path);
        else if (disk.isDirectory(path))
            disk.moveDirectory(path, temp_path);
    }

    void undo() override
    {
        if (disk.isFile(temp_path))
            disk.moveFile(temp_path, path);
        else if (disk.isDirectory(temp_path))
            disk.moveDirectory(temp_path, path);
    }

    void finalize() override
    {
        if (disk.exists(temp_path))
            disk.removeRecursive(temp_path);

        if (disk.exists(path))
            disk.removeRecursive(path);
    }
};


class CreateHardlinkOperation final : public IMetadataOperation
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

class MoveFileOperation final : public IMetadataOperation
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

class MoveDirectoryOperation final : public IMetadataOperation
{
private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
public:
    MoveDirectoryOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_)
        : path_from(path_from_)
        , path_to(path_to_)
        , disk(disk_)
    {}

    void execute() override
    {
        disk.moveDirectory(path_from, path_to);
    }

    void undo() override
    {
        disk.moveDirectory(path_to, path_from);
    }
};


class ReplaceFileOperation final : public IMetadataOperation
{
private:
    std::string path_from;
    std::string path_to;
    IDisk & disk;
    std::string temp_path_to;
public:
    ReplaceFileOperation(const std::string & path_from_, const std::string & path_to_, IDisk & disk_)
        : path_from(path_from_)
        , path_to(path_to_)
        , disk(disk_)
        , temp_path_to(getTempFileName(fs::path(path_to).parent_path()))
    {
    }

    void execute() override
    {
        if (disk.exists(path_to))
            disk.moveFile(path_to, temp_path_to);

        disk.replaceFile(path_from, path_to);
    }

    void undo() override
    {
        disk.moveFile(path_to, path_from);
        disk.moveFile(temp_path_to, path_to);
    }

    void finalize() override
    {
        disk.removeFileIfExists(temp_path_to);
    }
};

class WriteFileOperation final : public IMetadataOperation
{
private:
    std::string path;
    IDisk & disk;
    std::string data;
    bool existed = false;
    std::string prev_data;
public:
    WriteFileOperation(const std::string & path_, IDisk & disk_, const std::string & data_)
        : path(path_)
        , disk(disk_)
        , data(data_)
    {}

    void execute() override
    {
        if (disk.exists(path))
        {
            existed = true;
            auto buf = disk.readFile(path);
            readStringUntilEOF(prev_data, *buf);
        }
        auto buf = disk.writeFile(path);
        writeString(data, *buf);
        buf->finalize();
    }

    void undo() override
    {
        if (!existed)
            disk.removeFileIfExists(path);
        else
        {
            auto buf = disk.writeFile(path);
            writeString(prev_data, *buf);
        }
    }
};

}

const std::string & MetadataStorageFromRemoteDisk::getPath() const
{
    return disk->getPath();
}

bool MetadataStorageFromRemoteDisk::exists(const std::string & path) const
{
    return disk->exists(path);
}

bool MetadataStorageFromRemoteDisk::isFile(const std::string & path) const
{
    return disk->isFile(path);
}


bool MetadataStorageFromRemoteDisk::isDirectory(const std::string & path) const
{
    return disk->isDirectory(path);
}

Poco::Timestamp MetadataStorageFromRemoteDisk::getLastModified(const std::string & path) const
{
    return disk->getLastModified(path);
}

time_t MetadataStorageFromRemoteDisk::getLastChanged(const std::string & path) const
{
    return disk->getLastChanged(path);
}

uint64_t MetadataStorageFromRemoteDisk::getFileSize(const String & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getTotalSizeBytes();
}

std::vector<std::string> MetadataStorageFromRemoteDisk::listDirectory(const std::string & path) const
{
    std::vector<std::string> result_files;
    disk->listFiles(path, result_files);
    return result_files;
}

DirectoryIteratorPtr MetadataStorageFromRemoteDisk::iterateDirectory(const std::string & path) const
{
    return disk->iterateDirectory(path);
}


std::string MetadataStorageFromRemoteDisk::readFileToString(const std::string & path) const
{
    auto buf = disk->readFile(path);
    std::string result;
    readStringUntilEOF(result, *buf);
    return result;
}

DiskObjectStorageMetadataPtr MetadataStorageFromRemoteDisk::readMetadataUnlocked(const std::string & path, std::shared_lock<std::shared_mutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(disk->getPath(), root_path_for_remote_metadata, path);
    auto str = readFileToString(path);
    metadata->deserializeFromString(str);
    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromRemoteDisk::readMetadata(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return readMetadataUnlocked(path, lock);
}

std::unordered_map<String, String> MetadataStorageFromRemoteDisk::getSerializedMetadata(const std::vector<String> & file_paths) const
{
    std::shared_lock lock(metadata_mutex);
    std::unordered_map<String, String> metadatas;

    for (const auto & path : file_paths)
    {
        auto metadata = readMetadataUnlocked(path, lock);
        metadata->resetRefCount();
        WriteBufferFromOwnString buf;
        metadata->serialize(buf, false);
        metadatas[path] = buf.str();
    }

    return metadatas;
}

void MetadataStorageFromRemoteDiskTransaction::createHardLink(const std::string & path_from, const std::string & path_to)
{
    auto metadata = metadata_storage_for_remote.readMetadata(path_from);

    metadata->incrementRefCount();

    writeStringToFile(path_from, metadata->serializeToString());

    addOperation(std::make_unique<CreateHardlinkOperation>(path_from, path_to, *metadata_storage.getDisk()));
}

MetadataTransactionPtr MetadataStorageFromRemoteDisk::createTransaction() const
{
    return std::make_shared<MetadataStorageFromRemoteDiskTransaction>(*this);
}

std::vector<std::string> MetadataStorageFromRemoteDisk::getRemotePaths(const std::string & path) const
{
    auto metadata = readMetadata(path);

    std::vector<std::string> remote_paths;
    auto blobs = metadata->getBlobs();
    auto root_path = metadata->getBlobsCommonPrefix();
    remote_paths.reserve(blobs.size());
    for (const auto & [remote_path, _] : blobs)
        remote_paths.push_back(fs::path(root_path) / remote_path);

    return remote_paths;
}

uint32_t MetadataStorageFromRemoteDisk::getHardlinkCount(const std::string & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getRefCount();
}

BlobsPathToSize MetadataStorageFromRemoteDisk::getBlobs(const std::string & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getBlobs();
}

void MetadataStorageFromRemoteDiskTransaction::writeStringToFile( /// NOLINT
     const std::string & path,
     const std::string & data)
{
    addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_remote.getDisk(), data));
}

void MetadataStorageFromRemoteDiskTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::unlinkFile(const std::string & path)
{
    addOperation(std::make_unique<UnlinkFileOperation>(path, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::removeRecursive(const std::string & path)
{
    addOperation(std::make_unique<RemoveRecursiveOperation>(path, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::createDirectory(const std::string & path)
{
    addOperation(std::make_unique<CreateDirectoryOperation>(path, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::createDicrectoryRecursive(const std::string & path)
{
    addOperation(std::make_unique<CreateDirectoryRecursiveOperation>(path, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::removeDirectory(const std::string & path)
{
    addOperation(std::make_unique<RemoveDirectoryOperation>(path, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::moveFile(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::moveDirectory(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<MoveDirectoryOperation>(path_from, path_to, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::replaceFile(const std::string & path_from, const std::string & path_to)
{
    addOperation(std::make_unique<ReplaceFileOperation>(path_from, path_to, *metadata_storage_for_remote.getDisk()));
}

void MetadataStorageFromRemoteDiskTransaction::setReadOnly(const std::string & path)
{
    auto metadata = metadata_storage_for_remote.readMetadata(path);
    metadata->setReadOnly();

    auto data = metadata->serializeToString();
    if (!data.empty())
        addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_remote.getDisk(), data));
}

void MetadataStorageFromRemoteDiskTransaction::createEmptyMetadataFile(const std::string & path)
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(
        metadata_storage_for_remote.getDisk()->getPath(), metadata_storage_for_remote.getMetadataPath(), path);

    auto data = metadata->serializeToString();
    if (!data.empty())
        addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_remote.getDisk(), data));
}

void MetadataStorageFromRemoteDiskTransaction::createMetadataFile(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes)
{
    DiskObjectStorageMetadataPtr metadata = std::make_unique<DiskObjectStorageMetadata>(
        metadata_storage_for_remote.getDisk()->getPath(), metadata_storage_for_remote.getMetadataPath(), path);

    metadata->addObject(blob_name, size_in_bytes);

    auto data = metadata->serializeToString();
    if (!data.empty())
        addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_remote.getDisk(), data));
}

void MetadataStorageFromRemoteDiskTransaction::addBlobToMetadata(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes)
{
    DiskObjectStorageMetadataPtr metadata;
    if (metadata_storage_for_remote.exists(path))
    {
        metadata = metadata_storage_for_remote.readMetadata(path);
        metadata->addObject(blob_name, size_in_bytes);

        auto data = metadata->serializeToString();
        if (!data.empty())
            addOperation(std::make_unique<WriteFileOperation>(path, *metadata_storage_for_remote.getDisk(), data));
    }
    else
    {
        createMetadataFile(path, blob_name, size_in_bytes);
    }
}

void MetadataStorageFromRemoteDiskTransaction::unlinkMetadata(const std::string & path)
{
    auto metadata = metadata_storage_for_remote.readMetadata(path);
    uint32_t ref_count = metadata->getRefCount();
    if (ref_count != 0)
    {
        metadata->decrementRefCount();
        writeStringToFile(path, metadata->serializeToString());
    }
    unlinkFile(path);
}

}
