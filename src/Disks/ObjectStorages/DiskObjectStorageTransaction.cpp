#include <Disks/ObjectStorages/DiskObjectStorageTransaction.h>

namespace DB
{

struct PureMetadataOperation : public DiskObjectStorageOperation
{
    std::function<void(MetadataTransactionPtr tx)> on_execute;

    PureMetadataOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        std::function<void(MetadataTransactionPtr tx)> && on_execute_)
        : on_execute(std::move(on_execute_))
    {}

    void execute(MetadataTransactionPtr transaction) override
    {
        on_exexcute(transaction);
    }

    void undo() override
    {
    }

    void finalize() override
    {
    }
};

struct RemoveObjectOperation : public DiskObjectStorageOperation
{
    std::string path;
    bool delete_metadata_only;
    bool remove_from_cache{false};
    std::vector<std::string> paths_to_remove;
    bool if_exists;

    RemoveObjectOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_,
        bool delete_metadata_only_,
        bool if_exists_)
        : DiskObjectStorageOperation(object_storage_, metadata_storage_)
        , path(path_)
        , delete_metadata_only(delete_metadata_only_)
        , if_exists(if_exists_)
    {}

    void execute(MetadataTransactionPtr tx) override
    {
        LOG_TRACE(log, "Remove file by path: {}", backQuote(metadata_storage->getPath() + path));

        if (!metadata_storage->exists(path))
        {
            if (if_exists)
                return;

            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata path '{}' doesn't exist", path);
        }

        if (!metadata_storage->isFile(path))
            throw Exception(ErrorCodes::BAD_FILE_TYPE, "Path '{}' is not a regular file", path);

        try
        {
            uint32_t hardlink_count = metadata_storage->getHardlinkCount(path);
            auto remote_objects = metadata_storage->getRemotePaths(path);

            tx->unlinkMetadata(path);

            if (hardlink_count == 0)
            {
                paths_to_remove = remote_objects;
                remove_from_cache = true;
            }
        }
        catch (const Exception & e)
        {
            /// If it's impossible to read meta - just remove it from FS.
            if (e.code() == ErrorCodes::UNKNOWN_FORMAT
                || e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF
                || e.code() == ErrorCodes::CANNOT_READ_ALL_DATA
                || e.code() == ErrorCodes::CANNOT_OPEN_FILE)
            {
                LOG_INFO(log, "Failed to read metadata file {} before removal because it's incomplete or empty. "
                         "It's Ok and can happen after operation interruption (like metadata fetch), so removing as is", path);

                tx->unlinkFile(path);
            }
            else
                throw;
        }
    }

    void undo() override
    {

    }

    void finalize() override
    {
        if (!delete_metadata_only && !paths_to_remove.empty())
            object_storage.removeObjects(paths_to_remove);

        if (remove_from_cache)
        {
            for (const auto & path_to_remove : paths_to_remove)
                object_storage->removeFromCache(path_to_remove);
        }

    }
};

struct RemoveRecursiveOperation : public DiskObjectStorageOperation
{
    std::unordered_map<std::string, std::vector<std::string>> paths_to_remove;
    bool keep_all_batch_data;
    NameSet file_names_remove_metadata_only;
    std::vector<std::string> path_to_remove_from_cache;

    RemoveRecursiveOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_,
        bool keep_all_batch_data_,
        const NameSet & file_names_remove_metadata_only_)
        : DiskObjectStorageOperation(object_storage_, metadata_storage_)
        , path(path_)
        , keep_all_batch_data(keep_all_batch_data_)
        , file_names_remove_metadata_only(file_names_remove_metadata_only_)
    {}

    void removeMetadataRecursive(MetadataTransactionPtr tx, const std::string & path_to_remove)
    {
        checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

        if (metadata_storage->isFile(path_to_remove))
        {
            try
            {
                uint32_t hardlink_count = metadata_storage->getHardlinkCount(path_to_remove);
                auto remote_objects = metadata_storage->getRemotePaths(path_to_remove);

                tx->unlinkMetadata(path_to_remove);

                if (hardlink_count == 0)
                {
                    paths_to_remove[path_to_remove] = remote_objects;
                    path_to_remove_from_cache.insert(path_to_remove_from_cache.end(), remote_objects.begin(), remote_objects.end());
                }

            }
            catch (const Exception & e)
            {
                /// If it's impossible to read meta - just remove it from FS.
                if (e.code() == ErrorCodes::UNKNOWN_FORMAT
                    || e.code() == ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF
                    || e.code() == ErrorCodes::CANNOT_READ_ALL_DATA
                    || e.code() == ErrorCodes::CANNOT_OPEN_FILE)
                {
                    LOG_INFO(log, "Failed to read metadata file {} before removal because it's incomplete or empty. "
                             "It's Ok and can happen after operation interruption (like metadata fetch), so removing as is", path_to_remove);

                    tx->unlinkFile(path_to_remove);
                }
                else
                    throw;
            }
        }
        else
        {
            for (auto it = iterateDirectory(path_to_remove); it->isValid(); it->next())
                removeMetadataRecursive(tx, it->path());

            tx->removeDirectory(path_to_remove);
        }
    }

    void execute(MetadataTransactionPtr tx) override
    {
        removeMetadataRecursive(tx, path);
    }

    void undo() override
    {

    }

    void finalize() override
    {
        if (!keep_all_batch_data)
        {
            std::vector<std::string> remove_from_remote;
            for (auto && [local_path, remote_paths] : paths_to_remove)
            {
                if (!file_names_remove_metadata_only.contains(fs::path(local_path).filename()))
                {
                    remove_from_remote.insert(remove_from_remote.end(), remote_paths.begin(), remote_paths.end());
                }
            }
            removeFromRemoteFS(remove_from_remote);
        }

        for (const auto & path_to_remove : path_to_remove_from_cache)
            object_storage->removeFromCache(path_to_remove);
    }
};


struct ReplaceFileOperation : public DiskObjectStorageOperation
{
    std::string path_from;
    std::string path_to;
    std::vector<std::string> blobs_to_remove;

    ReplaceFileOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_from_,
        const std::string & path_to_)
        : DiskObjectStorageOperation(object_storage_, metadata_storage_)
        , path_from(path_from_)
        , path_to(path_to_)
    {}

    void execute(MetadataTransactionPtr tx) override
    {
        if (exists(to_path))
        {
            blobs_to_remove = metadata_storage->getRemotePaths(to_path);
            tx->replaceFile(path_from, path_to);
        }
        else
            tx->moveFile(path_from, path_to);
    }

    void undo() override
    {

    }

    void finalize() override
    {
        if (!blobs_to_remove.emtpy())
            object_storage->removeObjects(blobs_to_remove);
    }
};

struct WriteFileOperation : public DiskObjectStorageOperation
{
    std::string path;
    std::string blob_path;

    WriteFileOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_,
        const std::string & blob_path_)
        : DiskObjectStorageOperation(object_storage_, metadata_storage_)
        , path(path_)
        , blob_path(blob_path_)
    {}

    void execute(MetadataTransactionPtr tx) override
    {
    }

    void undo() override
    {
        if (object_storage.exists(blob_path))
            object_storage.removeObject(blob_path);
    }

    void finalize() override
    {
    }
};


struct CopyFileOperation : public DiskObjectStorageOperation
{
    std::string from_path;
    std::string to_path;

    std::vector<std::string>

    CopyFileOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & from_path_,
        const std::string & to_path_)
        : DiskObjectStorageOperation(object_storage_, metadata_storage_)
        , from_path(from_path_)
        , to_path(to_path_)
    {}

    void execute(MetadataTransactionPtr tx) override
    {
        auto
        object_storage.copyObject()
    }

    void undo() override
    {
        if (object_storage.exists(blob_path))
            object_storage.removeObject(blob_path);
    }

    void finalize() override
    {
    }


};

void DiskObjectStorageTransaction::createDirectory(const std::string & path)
{
    operations_to_execute.emplace_back(
        PureMetadataOperation(*disk.object_storage, *disk.metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->createDirectory(path);
        }));
}

void DiskObjectStorageTransaction::createDirectories(const std::string & path)
{
    operations_to_execute.emplace_back(
        PureMetadataOperation(*disk.object_storage, *disk.metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->createDicrectoryRecursive(path);
        }));
}


void DiskObjectStorageTransaction::moveDirectory(const std::string & from_path, const std::string & to_path)
{
    operations_to_execute.emplace_back(
        PureMetadataOperation(*disk.object_storage, *disk.metadata_storage, [from_path, to_path](MetadataTransactionPtr tx)
        {
            tx->moveDirectory(from_path, to_path);
        }));
}


void DiskObjectStorageTransaction::replaceFile(const std::string & from_path, const std::string & to_path)
{
    operations_to_execute.emplace_back(ReplaceFileOperation(*disk.object_storage, *disk.metadata_storage, from_path, to_path));
}

void DiskObjectStorageTransaction::clearDirectory(const std::string & path)
{
    for (auto it = disk.iterateDirectory(path); it->isValid(); it->next())
    {
        if (disk.isFile(it->path()))
            removeFile(it->path());
    }
}

void DiskObjectStorageTransaction::removeFile(const std::string & path)
{
    removeSharedFile(path, false);
}

void DiskObjectStorageTransaction::removeSharedFile(const std::string & path, bool keep_shared_data)
{
    operations_to_execute.emplace_back(RemoveObjectOperation(*disk.object_storage, *disk.metadata_storage, path, keeper_shared_data, false))
}

void DiskObjectStorageTransaction::removeSharedRecursive(const std::string & path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only)
{
    operations_to_execute.emplace_back(RemoveRecursiveOperation(*disk.object_storage, *disk.metadata_storage, path, keep_all_shared_data, file_names_remove_metadata_only))
}

void DiskObjectStorageTransaction::removeSharedFileIfExists(const std::string & path, bool keep_shared_data)
{
    operations_to_execute.emplace_back(RemoveObjectOperation(*disk.object_storage, *disk.metadata_storage, path, keeper_shared_data, false))
}

void DiskObjectStorageTransaction::removeDirectory(const std::string & path)
{
    operations_to_execute.emplace_back(
        PureMetadataOperation(*disk.object_storage, *disk.metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->removeDirectory(path);
        }));
}


void DiskObjectStorageTransaction::removeRecursive(const std::string & path)
{
    removeSharedRecursive(path, false, {});
}

void DiskObjectStorageTransaction::removeFileIfExists(const std::string & path)
{
    removeSharedFileIfExists(path, false);
}


void DiskObjectStorageTransaction::removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only)
{
    for (const auto & file : files)
    {
        bool keep_file = keep_all_batch_data || file_names_remove_metadata_only.contains(fs::path(file.path).filename());
        if (file.if_exists)
            removeSharedFileIfExists(file.path, keep_file);
        else
            removeSharedFile(file.path, keep_file);
    }
}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorageTransaction::writeFile( /// NOLINT
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    auto blob_name = getRandomASCIIString();

    auto create_metadata_callback = [this, mode, path, blob_name] (size_t count)
    {
        if (mode == WriteMode::Rewrite)
            metadata_transaction->createMetadataFile(path, blob_name, count);
        else
            metadata_transaction->addBlobToMetadata(path, blob_name, count);
    };

    auto blob_path = fs::path(remote_fs_root_path) / blob_name;

    operations_to_execute.emplace_back(WriteFileOperation(*disk.object_storage, *disk.metadata_storage, path, blob_path));

    /// We always use mode Rewrite because we simulate append using metadata and different files
    return object_storage->writeObject(
        blob_path, WriteMode::Rewrite, object_attributes,
        std::move(create_metadata_callback),
        buf_size, settings);
}


void DiskObjectStorageTransaction::createHardLink(const std::string & src_path, const std::string & dst_path)
{
    operations_to_execute.emplace_back(
        PureMetadataOperation(*disk.object_storage, *disk.metadata_storage, [src_path, dst_path](MetadataTransactionPtr tx)
        {
            tx->createHardLink(src_path, dst_path);
        }));
}

void DiskObjectStorageTransaction::setReadOnly(const std::string & path)
{
    operations_to_execute.emplace_back(
        PureMetadataOperation(*disk.object_storage, *disk.metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->setReadOnly(path);
        }));
}


void DiskObjectStorageTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    operations_to_execute.emplace_back(
        PureMetadataOperation(*disk.object_storage, *disk.metadata_storage, [path, timestamp](MetadataTransactionPtr tx)
        {
            tx->setLastModified(path, timestamp);
        }));
}

/// Recursively copy data containing at `from_path` to `to_path` located at `to_disk`.
void DiskObjectStorageTransaction::copy(const std::string & from_path, const std::string & to_path)
{

}

/// Recursively copy files from from_dir to to_dir. Create to_dir if not exists.
void DiskObjectStorageTransaction::copyDirectoryContent(const std::string & from_dir, const std::string & to_dir)
{
}

/// Copy file `from_file_path` to `to_file_path` located at `to_disk`.
void DiskObjectStorageTransaction::copyFile(const std::string & from_file_path, const std::string & to_file_path)
{

}



void DiskObjectStorageTransaction::commit()
{
    for (const auto & operation : operations_to_execute)
        operation->execute(metadata_transaction);

    try
    {
        metadata_transaction->commit();
    }
    catch (...)
    {
        for (const auto & operation : operations_to_execute | std::views::reverse)
            operation->undo();

        throw;
    }

    for (const auto & operation : operations_to_execute)
        operation->finalize();
}

}
