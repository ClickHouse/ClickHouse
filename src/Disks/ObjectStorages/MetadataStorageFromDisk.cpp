#include <Disks/ObjectStorages/MetadataStorageFromDisk.h>
#include <ranges>
#include <filesystem>
#include <Disks/TemporaryFileOnDisk.h>
#include <Poco/TemporaryFile.h>

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

std::string getTempFileName()
{
    std::string temp_filepath;
    std::string dummy_prefix = "a/";
    temp_filepath = Poco::TemporaryFile::tempName(dummy_prefix);
    dummy_prefix += "tmp";
    assert(temp_filepath.starts_with(dummy_prefix));
    temp_filepath.replace(0, dummy_prefix.length(), "tmp");
    return temp_filepath;
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
    std::string temp_filepath;
public:
    UnlinkFileOperation(const std::string & path_, IDisk & disk_)
        : path(path_)
        , disk(disk_)
        , temp_filepath(getTempFileName())
    {
    }

    void execute() override
    {
        disk.moveFile(path, temp_filepath);
    }

    void undo() override
    {
        disk.moveFile(temp_filepath, path);
    }

    void finalize() override
    {
        disk.removeFileIfExists(temp_filepath);
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
        , temp_path(getTempFileName())
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
        if (disk.isDirectory(temp_path))
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
        , temp_path_to(getTempFileName())
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

std::unique_ptr<WriteBufferFromFileBase> MetadataStorageFromDisk::writeFile( /// NOLINT
     const std::string & path,
     MetadataTransactionPtr transaction,
     size_t buf_size,
     const WriteSettings & settings)
{
    std::string temp_path = getTempFileName();
    transaction->addOperation(std::make_unique<WriteFileOperation>(path, temp_path, *disk));
    return disk->writeFile(temp_path, buf_size, WriteMode::Rewrite, settings);
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

std::unique_ptr<ReadBufferFromFileBase> MetadataStorageFromDisk::readFile(  /// NOLINT
     const std::string & path,
     const ReadSettings & settings,
     std::optional<size_t> read_hint,
     std::optional<size_t> file_size) const
{
    return disk->readFile(path, settings, read_hint, file_size);
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

}
