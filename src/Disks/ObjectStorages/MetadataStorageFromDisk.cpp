#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <ranges>
#include <filesystem>
#include <Disks/TemporaryFileOnDisk.h>
#include <Poco/TemporaryFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/getRandomASCIIString.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int FS_METADATA_ERROR;
    extern const int LOGICAL_ERROR;
}


std::string toString(MetadataFromDiskTransactionState state)
{
    switch (state)
    {
        case MetadataFromDiskTransactionState::PREPARING:
            return "PREPARING";
        case MetadataFromDiskTransactionState::FAILED:
            return "FAILED";
        case MetadataFromDiskTransactionState::COMMITTED:
            return "COMMITTED";
        case MetadataFromDiskTransactionState::ROLLED_BACK:
            return "ROLLED_BACK";
        case MetadataFromDiskTransactionState::PARTIALLY_ROLLED_BACK:
            return "PARTIALLY_ROLLED_BACK";
    }
    __builtin_unreachable();
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
        if (disk.isDirectory(path))
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

void MetadataStorageFromDisk::writeMetadataToFile( /// NOLINT
     const std::string & path,
     MetadataTransactionPtr transaction,
     const std::string & data)
{
    transaction->addOperation(std::make_unique<WriteFileOperation>(path, *disk, data));
}


void MetadataStorageFromDiskTransaction::addOperation(MetadataOperationPtr && operation)
{
    if (state != MetadataFromDiskTransactionState::PREPARING)
        throw Exception(ErrorCodes::FS_METADATA_ERROR, "Cannot add operations to transaction in {} state, it should be in {} state",
                        toString(state), toString(MetadataFromDiskTransactionState::PREPARING));

    operations.emplace_back(std::move(operation));
}

void MetadataStorageFromDiskTransaction::commit()
{
    if (state != MetadataFromDiskTransactionState::PREPARING)
        throw Exception(ErrorCodes::FS_METADATA_ERROR, "Cannot commit transaction in {} state, it should be in {} state",
                        toString(state), toString(MetadataFromDiskTransactionState::PREPARING));

    {
        std::unique_lock lock(commit_mutex);
        for (size_t i = 0; i < operations.size(); ++i)
        {
            try
            {
                operations[i]->execute();
            }
            catch (Exception & ex)
            {
                ex.addMessage(fmt::format("While committing operation #{}", i));
                failed_operation_index = i;
                state = MetadataFromDiskTransactionState::FAILED;
                throw;
            }
        }
    }

    /// Do it in "best effort" mode
    for (size_t i = 0; i < operations.size(); ++i)
    {
        try
        {
            operations[i]->finalize();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__, fmt::format("Failed to finalize operation #{}", i));
        }
    }

    state = MetadataFromDiskTransactionState::COMMITTED;
}

void MetadataStorageFromDiskTransaction::rollback()
{
    /// Otherwise everything is alright
    if (state == MetadataFromDiskTransactionState::FAILED)
    {
        if (!failed_operation_index.has_value())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Transaction in failed state, but has not failed operations. It's a bug");

        for (int64_t i = failed_operation_index.value(); i >= 0; --i)
        {
            try
            {
                std::unique_lock lock(commit_mutex);
                operations[i]->undo();
            }
            catch (Exception & ex)
            {
                state = MetadataFromDiskTransactionState::PARTIALLY_ROLLED_BACK;
                ex.addMessage(fmt::format("While rolling back operation #{}", i));
                throw;
            }
        }
    }
    else
    {
        /// Nothing to do, transaction committed or not even started to commit
    }

    state = MetadataFromDiskTransactionState::ROLLED_BACK;
}

MetadataStorageFromDiskTransaction::~MetadataStorageFromDiskTransaction()
{
    if (state == MetadataFromDiskTransactionState::FAILED)
    {
        try
        {
            rollback();
        }
        catch (...)
        {
            tryLogCurrentException(__PRETTY_FUNCTION__);
        }
    }
}

const std::string & MetadataStorageFromDisk::getPath() const
{
    return disk->getPath();
}

bool MetadataStorageFromDisk::exists(const std::string & path) const
{
    return disk->exists(path);
}

bool MetadataStorageFromDisk::isFile(const std::string & path) const
{
    return disk->isFile(path);
}


bool MetadataStorageFromDisk::isDirectory(const std::string & path) const
{
    return disk->isDirectory(path);
}

Poco::Timestamp MetadataStorageFromDisk::getLastModified(const std::string & path) const
{
    return disk->getLastModified(path);
}

uint64_t MetadataStorageFromDisk::getFileSize(const String & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getTotalSizeBytes();
}

std::vector<std::string> MetadataStorageFromDisk::listDirectory(const std::string & path) const
{
    std::vector<std::string> result_files;
    disk->listFiles(path, result_files);
    return result_files;
}

DirectoryIteratorPtr MetadataStorageFromDisk::iterateDirectory(const std::string & path)
{
    return disk->iterateDirectory(path);
}


std::string MetadataStorageFromDisk::readMetadataFileToString(const std::string & path) const
{
    auto buf = disk->readFile(path);
    std::string result;
    readStringUntilEOF(result, *buf);
    return result;
}

void MetadataStorageFromDisk::createEmptyMetadataFile(const std::string & path, MetadataTransactionPtr transaction)
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(disk->getPath(), root_path_for_remote_metadata, path);
    writeMetadataToFile(path, transaction, metadata->serializeToString());
}

void MetadataStorageFromDisk::setLastModified(const std::string & path, const Poco::Timestamp & timestamp, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<SetLastModifiedOperation>(path, timestamp, *disk));
}

void MetadataStorageFromDisk::unlinkFile(const std::string & path, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<UnlinkFileOperation>(path, *disk));
}

void MetadataStorageFromDisk::removeRecursive(const std::string & path, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<RemoveRecursiveOperation>(path, *disk));
}

void MetadataStorageFromDisk::createDirectory(const std::string & path, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<CreateDirectoryOperation>(path, *disk));
}

void MetadataStorageFromDisk::createDicrectoryRecursive(const std::string & path, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<CreateDirectoryRecursiveOperation>(path, *disk));
}

void MetadataStorageFromDisk::removeDirectory(const std::string & path, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<RemoveDirectoryOperation>(path, *disk));
}

void MetadataStorageFromDisk::createHardLink(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction)
{
    auto metadata = readMetadata(path_from);

    metadata->incrementRefCount();

    writeMetadataToFile(path_from, transaction, metadata->serializeToString());

    transaction->addOperation(std::make_unique<CreateHardlinkOperation>(path_from, path_to, *disk));
}

void MetadataStorageFromDisk::moveFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<MoveFileOperation>(path_from, path_to, *disk));
}

void MetadataStorageFromDisk::moveDirectory(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<MoveDirectoryOperation>(path_from, path_to, *disk));
}

void MetadataStorageFromDisk::replaceFile(const std::string & path_from, const std::string & path_to, MetadataTransactionPtr transaction)
{
    transaction->addOperation(std::make_unique<ReplaceFileOperation>(path_from, path_to, *disk));
}

void MetadataStorageFromDisk::setReadOnly(const std::string & path, MetadataTransactionPtr transaction)
{
    auto metadata = readMetadata(path);
    metadata->setReadOnly();
    writeMetadataToFile(path, transaction, metadata->serializeToString());
}

void MetadataStorageFromDisk::createMetadataFile(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes, MetadataTransactionPtr transaction)
{
    DiskObjectStorageMetadataPtr metadata = std::make_unique<DiskObjectStorageMetadata>(disk->getPath(), root_path_for_remote_metadata, path);
    metadata->addObject(blob_name, size_in_bytes);
    writeMetadataToFile(path, transaction, metadata->serializeToString());
}

void MetadataStorageFromDisk::addBlobToMetadata(const std::string & path, const std::string & blob_name, uint64_t size_in_bytes, MetadataTransactionPtr transaction)
{
    DiskObjectStorageMetadataPtr metadata;
    if (exists(path))
    {
        metadata = readMetadata(path);
    }
    else
    {
        metadata = std::make_unique<DiskObjectStorageMetadata>(disk->getPath(), root_path_for_remote_metadata, path);
    }

    metadata->addObject(blob_name, size_in_bytes);
    writeMetadataToFile(path, transaction, metadata->serializeToString());
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadataUnlocked(const std::string & path, std::shared_lock<std::shared_mutex> &) const
{
    auto metadata = std::make_unique<DiskObjectStorageMetadata>(disk->getPath(), root_path_for_remote_metadata, path);
    auto str = readMetadataFileToString(path);
    metadata->deserializeFromString(str);
    return metadata;
}

DiskObjectStorageMetadataPtr MetadataStorageFromDisk::readMetadata(const std::string & path) const
{
    std::shared_lock lock(metadata_mutex);
    return readMetadataUnlocked(path, lock);
}

std::unordered_map<String, String> MetadataStorageFromDisk::getSerializedMetadata(const std::vector<String> & file_paths) const
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

std::vector<std::string> MetadataStorageFromDisk::getRemotePaths(const std::string & path) const
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

uint32_t MetadataStorageFromDisk::getHardlinkCount(const std::string & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getRefCount();
}


BlobsPathToSize MetadataStorageFromDisk::getBlobs(const std::string & path) const
{
    auto metadata = readMetadata(path);
    return metadata->getBlobs();
}


uint32_t MetadataStorageFromDisk::unlinkAndGetHardlinkCount(const std::string & path, MetadataTransactionPtr transaction)
{
    auto metadata = readMetadata(path);
    uint32_t ref_count = metadata->getRefCount();
    if (ref_count != 0)
    {
        metadata->decrementRefCount();
        writeMetadataToFile(path, transaction, metadata->serializeToString());
    }
    unlinkFile(path, transaction);
    return ref_count;
}

}
