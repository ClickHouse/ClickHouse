#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/MetadataStorages/Local/MetadataStorageFromDisk.h>
#include <Disks/DiskObjectStorage/DiskObjectStorageTransaction.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <IO/ForkWriteBuffer.h>
#include <IO/WriteBuffer.h>
#if ENABLE_DISTRIBUTED_CACHE
#include <DistributedCache/Utils.h>
#endif
#include <Core/Settings.h>
#include <Core/SettingsEnums.h>
#include <Disks/IO/WriteBufferWithFinalizeCallback.h>
#include <Disks/WriteMode.h>
#include <Disks/IDisk.h>

#include <Common/Logger.h>
#include <Common/checkStackSize.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <base/defines.h>

#include <cstddef>
#include <memory>
#include <ranges>
#include <vector>

namespace DB
{

namespace FailPoints
{
    extern const char smt_insert_fake_hardware_error[];
    extern const char disk_object_storage_fail_commit_metadata_transaction[];
    extern const char disk_object_storage_fail_precommit_metadata_transaction[];
}

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
    extern const int FAULT_INJECTED;
    extern const int LOGICAL_ERROR;
}

DiskObjectStorageTransaction::DiskObjectStorageTransaction(
    ClusterConfigurationPtr cluster_,
    MetadataStoragePtr metadata_storage_,
    ObjectStorageRouterPtr object_storages_)
    : cluster(std::move(cluster_))
    , metadata_storage(std::move(metadata_storage_))
    , object_storages(std::move(object_storages_))
    , metadata_transaction(metadata_storage->createTransaction())
{
}

MultipleDisksObjectStorageTransaction::MultipleDisksObjectStorageTransaction(
    ClusterConfigurationPtr source_cluster_,
    MetadataStoragePtr source_metadata_storage_,
    ObjectStorageRouterPtr source_object_storages_,
    ClusterConfigurationPtr destination_cluster_,
    MetadataStoragePtr destination_metadata_storage_,
    ObjectStorageRouterPtr destination_object_storages_)
    : DiskObjectStorageTransaction(destination_cluster_, destination_metadata_storage_, destination_object_storages_)
    , source_cluster(std::move(source_cluster_))
    , source_metadata_storage(std::move(source_metadata_storage_))
    , source_object_storages(std::move(source_object_storages_))
{
}

void DiskObjectStorageTransaction::createDirectory(const std::string & path)
{
    operations_to_execute.push_back([path](MetadataTransactionPtr tx)
    {
        tx->createDirectory(path);
    });
}

void DiskObjectStorageTransaction::createDirectories(const std::string & path)
{
    operations_to_execute.push_back([path](MetadataTransactionPtr tx)
    {
        tx->createDirectoryRecursive(path);
    });
}

void DiskObjectStorageTransaction::moveDirectory(const std::string & from_path, const std::string & to_path)
{
    operations_to_execute.push_back([from_path, to_path](MetadataTransactionPtr tx)
    {
        tx->moveDirectory(from_path, to_path);
    });
}

void DiskObjectStorageTransaction::moveFile(const String & from_path, const String & to_path)
{
    operations_to_execute.push_back([from_path, to_path](MetadataTransactionPtr tx)
    {
        tx->moveFile(from_path, to_path);
    });
}

void DiskObjectStorageTransaction::truncateFile(const String & path, size_t size)
{
    operations_to_execute.push_back([path, size](MetadataTransactionPtr tx)
    {
        tx->truncateFile(path, size);
    });
}

void DiskObjectStorageTransaction::replaceFile(const std::string & from_path, const std::string & to_path)
{
    operations_to_execute.push_back([from_path, to_path](MetadataTransactionPtr tx)
    {
        tx->replaceFile(from_path, to_path);
    });
}

void DiskObjectStorageTransaction::removeFile(const std::string & path)
{
    operations_to_execute.push_back([path](MetadataTransactionPtr tx)
    {
        tx->unlinkFile(path, /*if_exists=*/false, /*should_remove_objects=*/true);
    });
}

void DiskObjectStorageTransaction::removeSharedFile(const std::string & path, bool keep_shared_data)
{
    operations_to_execute.push_back([path, keep_shared_data](MetadataTransactionPtr tx)
    {
        tx->unlinkFile(path, /*if_exists=*/false, /*should_remove_objects=*/!keep_shared_data);
    });
}

void DiskObjectStorageTransaction::removeSharedRecursive(
    const std::string & path, bool keep_all_shared_data, const NameSet & file_names_remove_metadata_only)
{
    if (!keep_all_shared_data && file_names_remove_metadata_only.empty())
    {
        operations_to_execute.push_back([path](MetadataTransactionPtr tx)
        {
            tx->removeRecursive(path, /*should_remove_objects=*/nullptr);
        });
    }
    else
    {
        operations_to_execute.push_back([path, keep_all_shared_data, file_names_remove_metadata_only](MetadataTransactionPtr tx)
        {
            tx->removeRecursive(path, /*should_remove_objects=*/[keep_all_shared_data, file_names_remove_metadata_only](const std::string & relative_path)
            {
                return !keep_all_shared_data && !file_names_remove_metadata_only.contains(relative_path);
            });
        });
    }
}

void DiskObjectStorageTransaction::removeSharedFileIfExists(const std::string & path, bool keep_shared_data)
{
    operations_to_execute.push_back([path, keep_shared_data](MetadataTransactionPtr tx)
    {
        tx->unlinkFile(path, /*if_exists=*/true, /*should_remove_objects=*/!keep_shared_data);
    });
}

void DiskObjectStorageTransaction::removeDirectory(const std::string & path)
{
    operations_to_execute.push_back([path](MetadataTransactionPtr tx)
    {
        tx->removeDirectory(path);
    });
}

void DiskObjectStorageTransaction::removeRecursive(const std::string & path)
{
    operations_to_execute.push_back([path](MetadataTransactionPtr tx)
    {
        tx->removeRecursive(path, /*should_remove_objects=*/nullptr);
    });
}

void DiskObjectStorageTransaction::removeFileIfExists(const std::string & path)
{
    operations_to_execute.push_back([path](MetadataTransactionPtr tx)
    {
        tx->unlinkFile(path, /*if_exists=*/true, /*should_remove_objects*/true);
    });
}

void DiskObjectStorageTransaction::removeSharedFiles(const RemoveBatchRequest & files, bool keep_all_batch_data, const NameSet & file_names_remove_metadata_only)
{
    for (const auto & [path, if_exists] : files)
    {
        const bool should_remove_objects = !keep_all_batch_data && !file_names_remove_metadata_only.contains(fs::path(path).filename());
        operations_to_execute.push_back([path, if_exists, should_remove_objects](MetadataTransactionPtr tx)
        {
            tx->unlinkFile(path, if_exists, should_remove_objects);
        });
    }
}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorageTransaction::writeFileWithAutoCommit(
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    return writeFileImpl(/*autocommit=*/true, path, buf_size, mode, settings);
}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorageTransaction::writeFile(
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    return writeFileImpl(/*autocommit=*/false, path, buf_size, mode, settings);
}

std::unique_ptr<WriteBufferFromFileBase> DiskObjectStorageTransaction::writeFileImpl(
    bool autocommit,
    const std::string & path,
    size_t buf_size,
    WriteMode mode,
    const WriteSettings & settings)
{
    LOG_TEST(getLogger("DiskObjectStorageTransaction"), "write file {} mode {} autocommit {}", path, mode, autocommit);

    /// NOTE: We check it here and not after writing blob because in case of plain/plain-rewritable metadata storages
    ///       undo of disk tx will actually remove existing data.
    if (mode == WriteMode::Append && !metadata_storage->supportWritingWithAppend())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk does not support WriteMode::Append");

    StoredObject object(metadata_transaction->generateObjectKeyForPath(path).serialize(), path);
    std::vector<WriteBufferPtr> writers;
    for (const auto & location : cluster->getEnabledLocations())
    {
        size_t use_buffer_size = buf_size;
        std::unique_ptr<WriteBufferFromFileBase> writer;

        if (location == cluster->getLocalLocation())
        {
            ObjectStoragePtr object_storage = object_storages->takePointingTo(location);

            #if ENABLE_DISTRIBUTED_CACHE
                bool use_distributed_cache = DistributedCache::canUseDistributedCacheForWrite(settings, *object_storage);

                if (use_distributed_cache && settings.distributed_cache_settings.write_through_cache_buffer_size)
                    use_buffer_size = settings.distributed_cache_settings.write_through_cache_buffer_size;
            #endif

            writer = object_storage->writeObject(
                object,
                /// We always use mode Rewrite because we simulate append using metadata and different files
                WriteMode::Rewrite,
                /*attributes=*/std::nullopt,
                use_buffer_size,
                settings);

            #if ENABLE_DISTRIBUTED_CACHE
                if (use_distributed_cache)
                    writer = DistributedCache::writeWithDistributedCache(path, object, settings, *object_storage, std::move(writer));
            #endif
        }
        else
        {
            writer = object_storages->takePointingTo(location)->writeObject(
                object,
                /// We always use mode Rewrite because we simulate append using metadata and different files
                WriteMode::Rewrite,
                /*attributes=*/std::nullopt,
                use_buffer_size,
                settings);
        }

        writers.push_back(std::move(writer));
        written_blobs[location].push_back(object);
    }

    auto buffer_to_enabled_locations = std::make_unique<ForkWriteBuffer>(std::move(writers));

    /// Does metadata_storage support empty files without actual blobs in the object_storage?
    const bool create_blob_if_empty = !metadata_storage->supportsEmptyFilesWithoutBlobs();

    /// This callback called in WriteBuffer finalize method -- only there we actually know
    /// how many bytes were written. We don't control when this finalize method will be called
    /// so here we just modify operation itself, but don't execute anything (and don't modify metadata transaction).
    /// Otherwise it's possible to get reorder of operations, like:
    /// tx->createDirectory(xxx) -- will add metadata operation in execute
    /// buf1 = tx->writeFile(xxx/yyy.bin)
    /// buf2 = tx->writeFile(xxx/zzz.bin)
    /// ...
    /// buf1->finalize() // shouldn't do anything with metadata operations, just memorize what to do
    /// tx->commit()
    const auto create_metadata_callback = [disk_tx = shared_from_this(), mode, object, autocommit, create_blob_if_empty](size_t count) mutable
    {
        object.bytes_size = count;

        auto missing_locations = disk_tx->cluster->findComplement(disk_tx->cluster->getEnabledLocations());
        disk_tx->operations_to_execute.push_back([object, mode, create_blob_if_empty, blob_replication = std::move(missing_locations)](MetadataTransactionPtr tx)
        {
            if (mode == WriteMode::Rewrite)
            {
                if (object.bytes_size > 0 || create_blob_if_empty)
                {
                    LOG_TEST(getLogger("DiskObjectStorageTransaction"), "Writing blob for path {}, key {}, size {}", object.local_path, object.remote_path, object.bytes_size);
                    tx->recordBlobsReplication(object, blob_replication);
                    tx->createMetadataFile(object.local_path, {object});
                }
                else
                {
                    LOG_TRACE(getLogger("DiskObjectStorageTransaction"), "Skipping writing empty blob for path {}, key {}", object.local_path, object.remote_path);
                    tx->createMetadataFile(object.local_path, {});
                }
            }
            else
            {
                if (object.bytes_size > 0 || create_blob_if_empty)
                    tx->recordBlobsReplication(object, blob_replication);

                /// Even if not create_blob_if_empty and size is 0, we still need to add metadata just to make sure that a file gets created if this is the 1st append
                tx->addBlobToMetadata(object.local_path, object);
            }
        });

        if (autocommit)
            disk_tx->commit();
    };

    return std::make_unique<WriteBufferWithFinalizeCallback>(std::move(buffer_to_enabled_locations), std::move(create_metadata_callback), object.remote_path, create_blob_if_empty);
}

/// This function is a simplified and adapted version of DiskObjectStorageTransaction::writeFile().
/// TODO: This function is incorrect for multiple object storages, we need to remove it and use everywhere regular writeFile
void DiskObjectStorageTransaction::writeFileUsingBlobWritingFunction(
    const String & path, WriteMode mode, WriteBlobFunction && write_blob_function)
{
    if (cluster->getConfiguration().size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "writeFileUsingBlobWritingFunction not supported for replicated setup");

    StoredObject object(metadata_transaction->generateObjectKeyForPath(path).serialize(), path);
    written_blobs[cluster->getLocalLocation()].push_back(object);

    /// See DiskObjectStorage::getBlobPath().
    Strings blob_path;
    blob_path.reserve(2);
    blob_path.emplace_back(object.remote_path);
    String objects_namespace = object_storages->takePointingTo(cluster->getLocalLocation())->getObjectsNamespace();
    if (!objects_namespace.empty())
        blob_path.emplace_back(objects_namespace);

    /// We always use mode Rewrite because we simulate append using metadata and different files
    object.bytes_size = std::move(write_blob_function)(blob_path, WriteMode::Rewrite, /*object_attributes=*/std::nullopt);

    operations_to_execute.push_back([object, mode](MetadataTransactionPtr tx)
    {
        if (mode == WriteMode::Rewrite)
        {
            LOG_TEST(getLogger("DiskObjectStorageTransaction"), "Writing blob for path {}, key {}, size {}", object.local_path, object.remote_path, object.bytes_size);
            tx->recordBlobsReplication(object, /*missing_locations=*/{});
            tx->createMetadataFile(object.local_path, {object});
        }
        else
        {
            tx->recordBlobsReplication(object, /*missing_locations=*/{});
            tx->addBlobToMetadata(object.local_path, object);
        }
    });
}

void DiskObjectStorageTransaction::createHardLink(const std::string & src_path, const std::string & dst_path)
{
    operations_to_execute.push_back([src_path, dst_path](MetadataTransactionPtr tx)
    {
        tx->createHardLink(src_path, dst_path);
    });
}

void DiskObjectStorageTransaction::setReadOnly(const std::string & path)
{
    operations_to_execute.push_back([path](MetadataTransactionPtr tx)
    {
        tx->setReadOnly(path);
    });
}

void DiskObjectStorageTransaction::setLastModified(const std::string & path, const Poco::Timestamp & timestamp)
{
    operations_to_execute.push_back([path, timestamp](MetadataTransactionPtr tx)
    {
        tx->setLastModified(path, timestamp);
    });
}

void DiskObjectStorageTransaction::chmod(const String & path, mode_t mode)
{
    operations_to_execute.push_back([path, mode](MetadataTransactionPtr tx)
    {
        tx->chmod(path, mode);
    });
}

void DiskObjectStorageTransaction::createFile(const std::string & path)
{
    if (metadata_storage->supportsEmptyFilesWithoutBlobs())
    {
        operations_to_execute.push_back([path](MetadataTransactionPtr tx)
        {
            tx->createMetadataFile(path, /*objects=*/{});
        });
    }
    else
    {
        writeFile(path, 0, WriteMode::Rewrite, getWriteSettings())->finalize();
    }
}

void DiskObjectStorageTransaction::copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    const auto blobs_to_copy = metadata_storage->getStorageObjects(from_file_path);
    const auto blobs_to_create = blobs_to_copy
                        | std::views::transform([&](const auto & from) { return StoredObject(metadata_transaction->generateObjectKeyForPath(to_file_path).serialize(), to_file_path, from.bytes_size); })
                        | std::ranges::to<StoredObjects>();

    const auto locations_for_writing = cluster->getEnabledLocations();
    const auto missing_locations = cluster->findComplement(locations_for_writing);
    const auto local_location = cluster->getLocalLocation();

    for (const auto & location : locations_for_writing)
    {
        for (const auto [src_blob, dst_blob] : std::views::zip(blobs_to_copy, blobs_to_create))
        {
            written_blobs[location].push_back(dst_blob);
            object_storages->takePointingTo(local_location)->copyObjectToAnotherObjectStorage(src_blob, dst_blob, read_settings, write_settings, *object_storages->takePointingTo(location));
        }
    }

    operations_to_execute.push_back([blobs_to_create, missing_locations, to_file_path](MetadataTransactionPtr tx)
    {
        for (const auto & blob : blobs_to_create)
            tx->recordBlobsReplication(blob, missing_locations);

        tx->createMetadataFile(to_file_path, blobs_to_create);
    });
}

void MultipleDisksObjectStorageTransaction::copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    const auto blobs_to_copy = source_metadata_storage->getStorageObjects(from_file_path);
    const auto blobs_to_create = blobs_to_copy
                        | std::views::transform([&](const auto & from) { return StoredObject(metadata_transaction->generateObjectKeyForPath(to_file_path).serialize(), to_file_path, from.bytes_size); })
                        | std::ranges::to<StoredObjects>();

    const auto locations_for_writing = cluster->getEnabledLocations();
    const auto missing_locations = cluster->findComplement(locations_for_writing);
    const auto source_local_location = source_cluster->getLocalLocation();

    for (const auto & location : locations_for_writing)
    {
        for (const auto [src_blob, dst_blob] : std::views::zip(blobs_to_copy, blobs_to_create))
        {
            written_blobs[location].push_back(dst_blob);
            source_object_storages->takePointingTo(source_local_location)->copyObjectToAnotherObjectStorage(src_blob, dst_blob, read_settings, write_settings, *object_storages->takePointingTo(location));
        }
    }

    operations_to_execute.push_back([blobs_to_create, missing_locations, to_file_path](MetadataTransactionPtr tx)
    {
        for (const auto & blob : blobs_to_create)
            tx->recordBlobsReplication(blob, missing_locations);

        tx->createMetadataFile(to_file_path, blobs_to_create);
    });
}

void DiskObjectStorageTransaction::commit()
{
    auto component_guard = Coordination::setCurrentComponent("DiskObjectStorageTransaction::commit");
    for (size_t i = 0; i < operations_to_execute.size(); ++i)
    {
        try
        {
            operations_to_execute[i](metadata_transaction);
        }
        catch (...)
        {
            tryLogCurrentException(
                getLogger("DiskObjectStorageTransaction"),
                fmt::format("An error occurred while executing transaction's operation #{}", i));

            undo();

            throw;
        }
    }

    try
    {
        fiu_do_on(FailPoints::disk_object_storage_fail_commit_metadata_transaction,
        {
            throw Exception(ErrorCodes::FAULT_INJECTED, "disk_object_storage_fail_commit_metadata_transaction");
        });

        metadata_transaction->commit(NoCommitOptions{});
    }
    catch (...)
    {
        undo();
        throw;
    }

    operations_to_execute.clear();
    written_blobs.clear();
    LOG_TEST(getLogger("DiskObjectStorageTransaction"), "Transaction committed successfully");
}

TransactionCommitOutcomeVariant DiskObjectStorageTransaction::tryCommit(const TransactionCommitOptionsVariant & options)
{
    for (size_t i = 0; i < operations_to_execute.size(); ++i)
    {
        try
        {
            operations_to_execute[i](metadata_transaction);

            fiu_do_on(FailPoints::disk_object_storage_fail_precommit_metadata_transaction,
            {
                throw Coordination::Exception(Coordination::Error::ZOPERATIONTIMEOUT, "disk_object_storage_fail_precommit_metadata_transaction");
            });
        }
        catch (Exception & ex)
        {
            /// Reset metadata transaction, it will be refilled in operations_to_execute[i]->execute on the next retry if needed
            metadata_transaction = metadata_storage->createTransaction();

            ex.addMessage(fmt::format("While executing operation #{}", i));

            if (needRollbackBlobs(options))
                undo();

            throw;
        }
    }

    // disk_object_storage_fail_commit_metadata_transaction injects a fake hardware error before the commit attempt
    TransactionCommitOutcomeVariant outcome;
    fiu_do_on(FailPoints::disk_object_storage_fail_commit_metadata_transaction,
    {
        MetaInKeeperCommitOutcome result;
        result.code = Coordination::Error::ZOPERATIONTIMEOUT;
        outcome = result;
        LOG_ERROR(getLogger("DiskObjectStorageTransaction"), "Failpoint smt_insert_fake_hardware_error triggered");
    });

    if (std::get_if<MetaInKeeperCommitOutcome>(&outcome) == nullptr)
        outcome = metadata_transaction->tryCommit(options);

    // smt_insert_fake_hardware_error injects a fake hardware error after the commit attempt
    fiu_do_on(FailPoints::smt_insert_fake_hardware_error,
    {
        auto * result = std::get_if<MetaInKeeperCommitOutcome>(&outcome);
        result->code = Coordination::Error::ZOPERATIONTIMEOUT;
    });

    if (!isSuccessfulOutcome(outcome))
    {
        /// Reset metadata transaction, it will be refilled in operations_to_execute[i]->execute on the next retry if needed
        metadata_transaction = metadata_storage->createTransaction();

        if (canRollbackBlobs(options, outcome))
        {
            undo();
        }
        else
        {
            LOG_DEBUG(getLogger("DiskObjectStorageTransaction"),
                "Commit failed, but rollback of blobs is not needed. "
                "Transaction will be retried without rolling back blobs.");
        }

        return outcome;
    }

    operations_to_execute.clear();
    written_blobs.clear();
    LOG_TEST(getLogger("DiskObjectStorageTransaction"), "Transaction committed successfully");

    return outcome;
}

void DiskObjectStorageTransaction::undo() noexcept
{
    try
    {
        for (const auto & [location, blobs] : written_blobs)
            object_storages->takePointingTo(location)->removeObjectsIfExist(blobs);
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("DiskObjectStorageTransaction"), "An error occurred during transaction cleanup");
    }

    operations_to_execute.clear();
    written_blobs.clear();
}

}
