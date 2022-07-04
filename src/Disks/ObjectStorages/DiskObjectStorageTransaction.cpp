#include <Disks/ObjectStorages/DiskObjectStorageTransaction.h>
#include <Disks/ObjectStorages/DiskObjectStorage.h>
#include <Common/checkStackSize.h>
#include <Common/getRandomASCIIString.h>
#include <ranges>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_FORMAT;
    extern const int ATTEMPT_TO_READ_AFTER_EOF;
    extern const int CANNOT_READ_ALL_DATA;
    extern const int CANNOT_OPEN_FILE;
    extern const int FILE_DOESNT_EXIST;
    extern const int BAD_FILE_TYPE;
    extern const int FILE_ALREADY_EXISTS;
}

DiskObjectStorageTransaction::DiskObjectStorageTransaction(
    IObjectStorage & object_storage_,
    IMetadataStorage & metadata_storage_,
    const std::string & remote_fs_root_path_,
    DiskObjectStorageRemoteMetadataRestoreHelper * metadata_helper_)
    : object_storage(object_storage_)
    , metadata_storage(metadata_storage_)
    , metadata_transaction(metadata_storage.createTransaction())
    , remote_fs_root_path(remote_fs_root_path_)
    , metadata_helper(metadata_helper_)
{}

namespace
{
/// Operation which affects only metadata. Simplest way to
/// implement via callback.
struct PureMetadataObjectStorageOperation final : public IDiskObjectStorageOperation
{
    std::function<void(MetadataTransactionPtr tx)> on_execute;

    PureMetadataObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        std::function<void(MetadataTransactionPtr tx)> && on_execute_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
        , on_execute(std::move(on_execute_))
    {}

    void execute(MetadataTransactionPtr transaction) override
    {
        on_execute(transaction);
    }

    void undo() override
    {
    }

    void finalize() override
    {
    }
};

struct RemoveObjectStorageOperation final : public IDiskObjectStorageOperation
{
    std::string path;
    bool delete_metadata_only;
    bool remove_from_cache{false};
    PathsWithSize paths_to_remove;
    bool if_exists;

    RemoveObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_,
        bool delete_metadata_only_,
        bool if_exists_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
        , path(path_)
        , delete_metadata_only(delete_metadata_only_)
        , if_exists(if_exists_)
    {}

    void execute(MetadataTransactionPtr tx) override
    {
        if (!metadata_storage.exists(path))
        {
            if (if_exists)
                return;

            throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "Metadata path '{}' doesn't exist", path);
        }

        if (!metadata_storage.isFile(path))
            throw Exception(ErrorCodes::BAD_FILE_TYPE, "Path '{}' is not a regular file", path);

        try
        {
            uint32_t hardlink_count = metadata_storage.getHardlinkCount(path);
            auto objects = metadata_storage.getObjectStoragePaths(path);

            tx->unlinkMetadata(path);

            if (hardlink_count == 0)
            {
                paths_to_remove = objects;
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
                object_storage.removeFromCache(path_to_remove.path);
        }

    }
};

struct RemoveRecursiveObjectStorageOperation final : public IDiskObjectStorageOperation
{
    std::string path;
    std::unordered_map<std::string, PathsWithSize> paths_to_remove;
    bool keep_all_batch_data;
    NameSet file_names_remove_metadata_only;
    PathsWithSize path_to_remove_from_cache;

    RemoveRecursiveObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_,
        bool keep_all_batch_data_,
        const NameSet & file_names_remove_metadata_only_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
        , path(path_)
        , keep_all_batch_data(keep_all_batch_data_)
        , file_names_remove_metadata_only(file_names_remove_metadata_only_)
    {}

    void removeMetadataRecursive(MetadataTransactionPtr tx, const std::string & path_to_remove)
    {
        checkStackSize(); /// This is needed to prevent stack overflow in case of cyclic symlinks.

        if (metadata_storage.isFile(path_to_remove))
        {
            try
            {
                uint32_t hardlink_count = metadata_storage.getHardlinkCount(path_to_remove);
                auto objects_paths = metadata_storage.getObjectStoragePaths(path_to_remove);

                tx->unlinkMetadata(path_to_remove);

                if (hardlink_count == 0)
                {
                    paths_to_remove[path_to_remove] = objects_paths;
                    path_to_remove_from_cache.insert(path_to_remove_from_cache.end(), objects_paths.begin(), objects_paths.end());
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
                    tx->unlinkFile(path_to_remove);
                }
                else
                    throw;
            }
        }
        else
        {
            for (auto it = metadata_storage.iterateDirectory(path_to_remove); it->isValid(); it->next())
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
            PathsWithSize remove_from_remote;
            for (auto && [local_path, remote_paths] : paths_to_remove)
            {
                if (!file_names_remove_metadata_only.contains(fs::path(local_path).filename()))
                {
                    remove_from_remote.insert(remove_from_remote.end(), remote_paths.begin(), remote_paths.end());
                }
            }
            object_storage.removeObjects(remove_from_remote);
        }

        for (const auto & [path_to_remove, _] : path_to_remove_from_cache)
            object_storage.removeFromCache(path_to_remove);
    }
};


struct ReplaceFileObjectStorageOperation final : public IDiskObjectStorageOperation
{
    std::string path_from;
    std::string path_to;
    PathsWithSize blobs_to_remove;

    ReplaceFileObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_from_,
        const std::string & path_to_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
        , path_from(path_from_)
        , path_to(path_to_)
    {}

    void execute(MetadataTransactionPtr tx) override
    {
        if (metadata_storage.exists(path_to))
        {
            blobs_to_remove = metadata_storage.getObjectStoragePaths(path_to);
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
        if (!blobs_to_remove.empty())
            object_storage.removeObjects(blobs_to_remove);
    }
};

struct WriteFileObjectStorageOperation final : public IDiskObjectStorageOperation
{
    std::string path;
    std::string blob_path;
    size_t size;
    std::function<void(MetadataTransactionPtr)> on_execute;

    WriteFileObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & path_,
        const std::string & blob_path_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
        , path(path_)
        , blob_path(blob_path_)
    {}

    void setOnExecute(std::function<void(MetadataTransactionPtr)> && on_execute_)
    {
        on_execute = on_execute_;
    }

    void execute(MetadataTransactionPtr tx) override
    {
        if (on_execute)
            on_execute(tx);
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


struct CopyFileObjectStorageOperation final : public IDiskObjectStorageOperation
{
    std::string from_path;
    std::string to_path;
    std::string remote_fs_root_path;

    std::vector<std::string> created_blobs;

    CopyFileObjectStorageOperation(
        IObjectStorage & object_storage_,
        IMetadataStorage & metadata_storage_,
        const std::string & from_path_,
        const std::string & to_path_,
        const std::string & remote_fs_root_path_)
        : IDiskObjectStorageOperation(object_storage_, metadata_storage_)
        , from_path(from_path_)
        , to_path(to_path_)
        , remote_fs_root_path(remote_fs_root_path_)
    {}

    void execute(MetadataTransactionPtr tx) override
    {
        tx->createEmptyMetadataFile(to_path);
        auto source_blobs = metadata_storage.getObjectStoragePaths(from_path); /// Full paths

        for (const auto & [blob_from, size] : source_blobs)
        {
            auto blob_name = getRandomASCIIString();

            auto blob_to = fs::path(remote_fs_root_path) / blob_name;

            object_storage.copyObject(blob_from, blob_to);

            tx->addBlobToMetadata(to_path, blob_name, size);

            created_blobs.push_back(blob_to);
        }
    }

    void undo() override
    {
        for (const auto & blob_path : created_blobs)
            object_storage.removeObject(blob_path);
    }

    void finalize() override
    {
    }
};

}

void DiskObjectStorageTransaction::createDirectory(const std::string & path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->createDirectory(path);
        }));
}

void DiskObjectStorageTransaction::createDirectories(const std::string & path)
{
    LOG_DEBUG(&Poco::Logger::get("DEBUG"), "CREATE DIRECTORIES TRANSACTION FOR PATH {}", path);
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->createDicrectoryRecursive(path);
        }));
}


void DiskObjectStorageTransaction::moveDirectory(const std::string & from_path, const std::string & to_path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [from_path, to_path](MetadataTransactionPtr tx)
        {
            tx->moveDirectory(from_path, to_path);
        }));
}

void DiskObjectStorageTransaction::moveFile(const String & from_path, const String & to_path)
{
     operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [from_path, to_path, this](MetadataTransactionPtr tx)
        {
            if (metadata_storage.exists(to_path))
                throw Exception("File already exists: " + to_path, ErrorCodes::FILE_ALREADY_EXISTS);

            if (!metadata_storage.exists(from_path))
                throw Exception(ErrorCodes::FILE_DOESNT_EXIST, "File {} doesn't exist, cannot move", to_path);

            tx->moveFile(from_path, to_path);
        }));
}

void DiskObjectStorageTransaction::replaceFile(const std::string & from_path, const std::string & to_path)
{
    operations_to_execute.emplace_back(std::make_unique<ReplaceFileObjectStorageOperation>(object_storage, metadata_storage, from_path, to_path));
}

void DiskObjectStorageTransaction::clearDirectory(const std::string & path)
{
    for (auto it = metadata_storage.iterateDirectory(path); it->isValid(); it->next())
    {
        if (metadata_storage.isFile(it->path()))
            removeFile(it->path());
    }
}

void DiskObjectStorageTransaction::removeFile(const std::string & path)
{
    removeSharedFile(path, false);
}

void DiskObjectStorageTransaction::removeSharedFile(const std::string & path, bool keep_shared_data)
{
    operations_to_execute.emplace_back(std::make_unique<RemoveObjectStorageOperation>(object_storage, metadata_storage, path, keep_shared_data, false));
}

void DiskObjectStorageTransaction::removeSharedRecursive(const std::string & path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only)
{
    operations_to_execute.emplace_back(std::make_unique<RemoveRecursiveObjectStorageOperation>(object_storage, metadata_storage, path, keep_all_shared_data, file_names_remove_metadata_only));
}

void DiskObjectStorageTransaction::removeSharedFileIfExists(const std::string & path, bool keep_shared_data)
{
    operations_to_execute.emplace_back(std::make_unique<RemoveObjectStorageOperation>(object_storage, metadata_storage, path, keep_shared_data, true));
}

void DiskObjectStorageTransaction::removeDirectory(const std::string & path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path](MetadataTransactionPtr tx)
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

namespace
{

String revisionToString(UInt64 revision)
{
    return std::bitset<64>(revision).to_string();
}

}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorageTransaction::writeFile( /// NOLINT
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings,
    bool autocommit)
{
    auto blob_name = getRandomASCIIString();

    std::optional<ObjectAttributes> object_attributes;
    if (metadata_helper)
    {
        auto revision = metadata_helper->revision_counter + 1;
        metadata_helper->revision_counter++;
        object_attributes = {
            {"path", path}
        };
        blob_name = "r" + revisionToString(revision) + "-file-" + blob_name;
    }

    auto blob_path = fs::path(remote_fs_root_path) / blob_name;

    auto write_operation = std::make_unique<WriteFileObjectStorageOperation>(object_storage, metadata_storage, path, blob_path);
    std::function<void(size_t count)> create_metadata_callback;

    if  (autocommit)
    {
        create_metadata_callback = [tx = shared_from_this(), mode, path, blob_name] (size_t count)
        {
            if (mode == WriteMode::Rewrite)
                tx->metadata_transaction->createMetadataFile(path, blob_name, count);
            else
                tx->metadata_transaction->addBlobToMetadata(path, blob_name, count);

            tx->metadata_transaction->commit();
        };
    }
    else
    {
        create_metadata_callback = [write_op = write_operation.get(), mode, path, blob_name] (size_t count)
        {
            /// This callback called in WriteBuffer finalize method -- only there we actually know
            /// how many bytes were written. We don't control when this finalize method will be called
            /// so here we just modify operation itself, but don't execute anything (and don't modify metadata transaction).
            /// Otherwise it's possible to get reorder of operations, like:
            /// tx->createDirectory(xxx) -- will add metadata operation in execute
            /// buf1 = tx->writeFile(xxx/yyy.bin)
            /// buf2 = tx->writeFile(xxx/zzz.bin)
            /// ...
            /// buf1->finalize() // shouldn't do anything with metadata operations, just memoize what to do
            /// tx->commit()
            write_op->setOnExecute([mode, path, blob_name, count](MetadataTransactionPtr tx)
            {
                if (mode == WriteMode::Rewrite)
                    tx->createMetadataFile(path, blob_name, count);
                else
                    tx->addBlobToMetadata(path, blob_name, count);
            });
        };

    }

    operations_to_execute.emplace_back(std::move(write_operation));

    /// We always use mode Rewrite because we simulate append using metadata and different files
    return object_storage.writeObject(
        blob_path, WriteMode::Rewrite, object_attributes,
        std::move(create_metadata_callback),
        buf_size, settings);
}


void DiskObjectStorageTransaction::createHardLink(const std::string & src_path, const std::string & dst_path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [src_path, dst_path](MetadataTransactionPtr tx)
        {
            tx->createHardLink(src_path, dst_path);
        }));
}

void DiskObjectStorageTransaction::setReadOnly(const std::string & path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->setReadOnly(path);
        }));
}

void DiskObjectStorageTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path, timestamp](MetadataTransactionPtr tx)
        {
            tx->setLastModified(path, timestamp);
        }));
}

void DiskObjectStorageTransaction::createFile(const std::string & path)
{
    operations_to_execute.emplace_back(
        std::make_unique<PureMetadataObjectStorageOperation>(object_storage, metadata_storage, [path](MetadataTransactionPtr tx)
        {
            tx->createEmptyMetadataFile(path);
        }));
}

void DiskObjectStorageTransaction::copyFile(const std::string & from_file_path, const std::string & to_file_path)
{
    operations_to_execute.emplace_back(std::make_unique<CopyFileObjectStorageOperation>(object_storage, metadata_storage, from_file_path, to_file_path, remote_fs_root_path));
}

void DiskObjectStorageTransaction::commit()
{
    for (size_t i = 0; i < operations_to_execute.size(); ++i)
    {
        try
        {
            operations_to_execute[i]->execute(metadata_transaction);
        }
        catch (Exception & ex)
        {
            ex.addMessage(fmt::format("While executing operation #{}", i));

            for (int64_t j = i; j >= 0; --j)
            {
                try
                {
                    operations_to_execute[j]->undo();
                }
                catch (Exception & rollback_ex)
                {
                    rollback_ex.addMessage(fmt::format("While undoing operation #{}", i));
                    throw;
                }
            }
            throw;
        }
    }

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
