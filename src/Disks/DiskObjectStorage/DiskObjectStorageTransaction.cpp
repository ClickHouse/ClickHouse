#include <Disks/DiskObjectStorage/Replication/ClusterConfiguration.h>
#include <Disks/DiskObjectStorage/MetadataStorages/IMetadataStorage.h>
#include <Disks/DiskObjectStorage/DiskObjectStorageTransaction.h>
#include <Disks/DiskObjectStorage/DiskObjectStorage.h>
#include <Disks/DiskObjectStorage/IOSchedulingSettings.h>
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
#include <Interpreters/Context.h>

#include <Common/ElapsedTimeProfileEventIncrement.h>
#include <Common/Logger.h>
#include <Common/checkStackSize.h>
#include <Common/logger_useful.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/ProfileEvents.h>
#include <Common/setThreadName.h>
#include <Common/threadPoolCallbackRunner.h>
#include <base/defines.h>

#include <chrono>
#include <cstddef>
#include <memory>
#include <ranges>
#include <vector>

namespace ProfileEvents
{
    extern const Event DiskObjectStorageWaitBlobRemovalMicroseconds;
}

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

void DiskObjectStorageTransaction::waitBlobRemoval(const StoredObjects & blobs) const
{
    try
    {
        /// `wait_blob_removal_timeout_ms` is a snapshot of either the per-disk
        /// `wait_for_blob_removal_timeout_ms` config option or, when absent, the
        /// server setting `disk_object_storage_blob_removal_wait_timeout_ms`.
        /// A value of `0` means "wait indefinitely" and restores the strict
        /// pre-fix semantics for operators who prefer it. Blobs not cleaned up
        /// here are removed by the next scheduled `BlobKillerThread` round.
        const auto deadline = wait_blob_removal_timeout_ms == 0
            ? std::chrono::steady_clock::time_point::max()
            : std::chrono::steady_clock::now() + std::chrono::milliseconds(wait_blob_removal_timeout_ms);

        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::DiskObjectStorageWaitBlobRemovalMicroseconds);
        for (size_t i = 0; i < 100 && metadata_storage->hasPendingRemovalBlobs(blobs); ++i)
        {
            /// The deadline is enforced *inside* `waitRound` via the killer's
            /// condition variable, so a single slow or stuck round cannot block
            /// the caller past the configured budget.
            if (!blob_killer->triggerAndWait(deadline))
            {
                LOG_WARNING(
                    getLogger("DiskObjectStorageTransaction"),
                    "Waiting for blob removal timed out after {} ms ({} iterations, configured limit {} ms). "
                    "Remaining blobs will be cleaned up asynchronously by the blob killer.",
                    watch.elapsed() / 1000,
                    i + 1,
                    wait_blob_removal_timeout_ms);
                break;
            }
        }

        if (watch.elapsed() > 100'000)
            LOG_TRACE(getLogger("DiskObjectStorageTransaction"), "Waiting for blob removal took {} ms", watch.elapsed() / 1000);
    }
    catch (...)
    {
        tryLogCurrentException(getLogger("DiskObjectStorageTransaction"));
    }
}

DiskObjectStorageTransaction::DiskObjectStorageTransaction(
    ClusterConfigurationPtr cluster_,
    MetadataStoragePtr metadata_storage_,
    ObjectStorageRouterPtr object_storages_,
    BlobKillerThreadPtr blob_killer_,
    std::shared_ptr<ThreadPool> copy_object_pool_,
    bool wait_blob_removal_,
    UInt64 wait_blob_removal_timeout_ms_,
    String read_resource_name_,
    String write_resource_name_)
    : cluster(std::move(cluster_))
    , metadata_storage(std::move(metadata_storage_))
    , object_storages(std::move(object_storages_))
    , blob_killer(std::move(blob_killer_))
    , copy_object_pool(std::move(copy_object_pool_))
    , wait_blob_removal(wait_blob_removal_)
    , wait_blob_removal_timeout_ms(wait_blob_removal_timeout_ms_)
    , read_resource_name(std::move(read_resource_name_))
    , write_resource_name(std::move(write_resource_name_))
    , metadata_transaction(metadata_storage->createTransaction())
{
}

MultipleDisksObjectStorageTransaction::MultipleDisksObjectStorageTransaction(
    ClusterConfigurationPtr source_cluster_,
    MetadataStoragePtr source_metadata_storage_,
    ObjectStorageRouterPtr source_object_storages_,
    ClusterConfigurationPtr destination_cluster_,
    MetadataStoragePtr destination_metadata_storage_,
    ObjectStorageRouterPtr destination_object_storages_,
    std::shared_ptr<ThreadPool> copy_object_pool_,
    std::string read_resource_name_,
    std::string write_resource_name_)
    : DiskObjectStorageTransaction(
          destination_cluster_,
          destination_metadata_storage_,
          destination_object_storages_,
          /*blob_killer=*/nullptr,
          std::move(copy_object_pool_),
          /*wait_blob_removal=*/false,
          /*wait_blob_removal_timeout_ms=*/0,
          std::move(read_resource_name_),
          std::move(write_resource_name_))
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

    WriteSettings enriched_settings = updateIOSchedulingSettings(settings, read_resource_name, write_resource_name);

    /// NOTE: We check it here and not after writing blob because in case of plain/plain-rewritable metadata storages
    ///       undo of disk tx will actually remove existing data.
    if (mode == WriteMode::Append && !metadata_storage->supportWritingWithAppend())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Disk does not support WriteMode::Append");

    StoredObject object(metadata_transaction->generateObjectKeyForPath(path).serialize(), path);
    ForkWriteBuffer::WriteBufferPtrs writers;
    auto enabled_locations = cluster->getEnabledLocations();
    for (const auto & location : enabled_locations)
    {
        size_t use_buffer_size = buf_size;
        std::unique_ptr<WriteBufferFromFileBase> writer;

        if (location == cluster->getLocalLocation())
        {
            ObjectStoragePtr object_storage = object_storages->takePointingTo(location);

            #if ENABLE_DISTRIBUTED_CACHE
                bool use_distributed_cache = DistributedCache::canUseDistributedCacheForWrite(enriched_settings, *object_storage);

                if (use_distributed_cache && enriched_settings.distributed_cache_settings.write_through_cache_buffer_size)
                    use_buffer_size = enriched_settings.distributed_cache_settings.write_through_cache_buffer_size;
            #endif

            writer = object_storage->writeObject(
                object,
                /// We always use mode Rewrite because we simulate append using metadata and different files
                WriteMode::Rewrite,
                /*attributes=*/std::nullopt,
                use_buffer_size,
                enriched_settings);

            #if ENABLE_DISTRIBUTED_CACHE
                if (use_distributed_cache)
                    writer = DistributedCache::writeWithDistributedCache(path, object, enriched_settings, *object_storage, std::move(writer));
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
                enriched_settings);
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
    const auto create_metadata_callback = [disk_tx = shared_from_this(), replicated_locations = std::move(enabled_locations), mode, object, autocommit, create_blob_if_empty](size_t count) mutable
    {
        object.bytes_size = count;

        /// Locations to which blobs were not originally copied should be marked as missing.
        auto missing_locations = disk_tx->cluster->findComplement(replicated_locations);
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

void DiskObjectStorageTransaction::copyFileImpl(
    const MetadataStoragePtr & src_metadata_storage,
    const ClusterConfigurationPtr & src_cluster,
    const ObjectStorageRouterPtr & src_object_storages,
    const std::string & from_file_path,
    const std::string & to_file_path,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings)
{
    /// Share the enriched settings via shared_ptr so each task lambda captures a cheap refcount bump
    /// rather than a full copy of ReadSettings / WriteSettings.
    const auto enriched_read_settings = std::make_shared<const ReadSettings>(
        updateIOSchedulingSettings(read_settings, read_resource_name, write_resource_name));
    const auto enriched_write_settings = std::make_shared<const WriteSettings>(
        updateIOSchedulingSettings(write_settings, read_resource_name, write_resource_name));

    const auto blobs_to_copy = src_metadata_storage->getStorageObjects(from_file_path);
    const auto blobs_to_create = blobs_to_copy
                        | std::views::transform([&](const auto & from) { return StoredObject(metadata_transaction->generateObjectKeyForPath(to_file_path).serialize(), to_file_path, from.bytes_size); })
                        | std::ranges::to<StoredObjects>();

    const auto locations_for_writing = cluster->getEnabledLocations();
    const auto missing_locations = cluster->findComplement(locations_for_writing);
    const auto src_local_location = src_cluster->getLocalLocation();

    /// Pre-populate `written_blobs` sequentially so the parallel section does not race on it.
    for (const auto & location : locations_for_writing)
        for (const auto & dst_blob : blobs_to_create)
            written_blobs[location].push_back(dst_blob);

    /// Dispatch `copyObjectToAnotherObjectStorage` calls in parallel onto the disk-level pool.
    /// We can't reuse `IObjectStorage::getThreadPoolWriter()` here because the copy implementation
    /// itself submits onto that pool via `writeObject`, which would risk pool self-deadlock.
    /// `ThreadPoolCallbackRunnerLocal` drains tasks in its destructor, so on exception unwinding
    /// the captured state stays alive until in-flight workers complete.
    ThreadPoolCallbackRunnerLocal<void> runner(*copy_object_pool, ThreadName::DISK_OBJECT_STORAGE_COPY);

    for (const auto & location : locations_for_writing)
    {
        for (const auto [src_blob, dst_blob] : std::views::zip(blobs_to_copy, blobs_to_create))
        {
            runner.enqueueAndKeepTrack(
                [this, src_object_storages, src_blob, dst_blob, location, src_local_location, enriched_read_settings, enriched_write_settings]
                {
                    src_object_storages->takePointingTo(src_local_location)->copyObjectToAnotherObjectStorage(
                        src_blob, dst_blob, *enriched_read_settings, *enriched_write_settings, *object_storages->takePointingTo(location));
                });
        }
    }

    runner.waitForAllToFinishAndRethrowFirstError();

    operations_to_execute.push_back([blobs_to_create, missing_locations, to_file_path](MetadataTransactionPtr tx)
    {
        for (const auto & blob : blobs_to_create)
            tx->recordBlobsReplication(blob, missing_locations);

        tx->createMetadataFile(to_file_path, blobs_to_create);
    });
}

void DiskObjectStorageTransaction::copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    copyFileImpl(metadata_storage, cluster, object_storages, from_file_path, to_file_path, read_settings, write_settings);
}

void MultipleDisksObjectStorageTransaction::copyFile(const std::string & from_file_path, const std::string & to_file_path, const ReadSettings & read_settings, const WriteSettings & write_settings)
{
    copyFileImpl(source_metadata_storage, source_cluster, source_object_storages, from_file_path, to_file_path, read_settings, write_settings);
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

    if (wait_blob_removal)
        waitBlobRemoval(metadata_transaction->getSubmittedForRemovalBlobs());

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

    if (wait_blob_removal)
        waitBlobRemoval(metadata_transaction->getSubmittedForRemovalBlobs());

    operations_to_execute.clear();
    written_blobs.clear();
    LOG_TEST(getLogger("DiskObjectStorageTransaction"), "Transaction committed successfully");

    return outcome;
}

void DiskObjectStorageTransaction::undo() noexcept
{
    for (const auto & [location, blobs] : written_blobs)
    {
        try
        {
            object_storages->takePointingTo(location)->removeObjectsIfExist(blobs);
        }
        catch (...)
        {
            tryLogCurrentException(getLogger("DiskObjectStorageTransaction"), fmt::format("An error occurred during transaction cleanup from location '{}'", location));
        }
    }

    operations_to_execute.clear();
    written_blobs.clear();
}

}
