#include "config.h"
#if USE_AVRO

#include <cstddef>
#include <memory>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Common/Exception.h>


#include <Core/NamesAndTypes.h>
#include <Core/Settings.h>
#include <Databases/DataLake/Common.h>
#include <Databases/DataLake/ICatalog.h>
#include <Disks/DiskObjectStorage/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>

#include <IO/CompressedReadBufferWrapper.h>
#include <IO/Progress.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <Storages/VirtualColumnUtils.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>

#include <Common/ProfileEvents.h>
#include <Common/SharedLockGuard.h>
#include <Common/UniqueLock.h>
#include <Common/logger_useful.h>

#include <Interpreters/IcebergMetadataLog.h>
#include <base/wide_integer_to_string.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>


namespace ProfileEvents
{
extern const Event IcebergIteratorInitializationMicroseconds;
extern const Event IcebergMetadataReadWaitTimeMicroseconds;
extern const Event IcebergMetadataReturnedObjectInfos;
extern const Event IcebergMinMaxNonPrunedDeleteFiles;
extern const Event IcebergMinMaxPrunedDeleteFiles;
extern const Event IcebergPartitionPrunedFiles;
extern const Event IcebergPartitionPrunedManifestFiles;
};


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
namespace Setting
{
extern const SettingsBool use_iceberg_partition_pruning;
extern const SettingsBool use_iceberg_manifest_list_partition_pruning;
};


using namespace Iceberg;

namespace
{

/// decode path agrees on whether pruning applies.
std::shared_ptr<const ActionsDAG> makeManifestFilterDag(const ActionsDAG * filter_dag, const ContextPtr & context)
{
    if (!filter_dag)
        return nullptr;
    if (!context)
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "Context is required with non-empty filter_dag to implement "
            "partition pruning for Iceberg table");
    }
    if (!context->getSettingsRef()[Setting::use_iceberg_partition_pruning].value)
        return nullptr;
    return std::make_shared<ActionsDAG>(filter_dag->clone());
}

std::span<const ProcessedManifestFileEntryPtr> defineDeletesSpan(
    ProcessedManifestFileEntryPtr data_object_,
    const std::vector<ProcessedManifestFileEntryPtr> & deletes_objects,
    bool is_equality_delete,
    LoggerPtr logger)
{
    if (deletes_objects.empty())
    {
        return {};
    }
    /// Objects in deletes_objects are sorted by common_partition_specification, partition_key_value and added_sequence_number.
    /// It is done to have an invariant that position deletes objects which corresponds
    /// to the data object form a subsegment in a deletes_objects vector.
    /// We need to take all position deletes objects which has the same partition schema and value and has added_sequence_number
    /// greater than or equal to the data object added_sequence_number (https://iceberg.apache.org/spec/#scan-planning)
    /// ManifestFileEntry has comparator by default which helps to do that.
    auto beg_it = is_equality_delete ?
        std::upper_bound(deletes_objects.begin(), deletes_objects.end(), data_object_)
        : std::lower_bound(deletes_objects.begin(), deletes_objects.end(), data_object_);
    auto end_it = std::upper_bound(
        deletes_objects.begin(),
        deletes_objects.end(),
        data_object_,
        [](const ProcessedManifestFileEntryPtr & lhs, const ProcessedManifestFileEntryPtr & rhs)
        {
            return std::tie(*lhs->common_partition_specification, lhs->normalized_partition_key_value)
                < std::tie(*rhs->common_partition_specification, rhs->normalized_partition_key_value);
        });
    if (beg_it - deletes_objects.begin() > end_it - deletes_objects.begin())
    {
        throw DB::Exception(
            DB::ErrorCodes::LOGICAL_ERROR,
            "{} deletes objects are not sorted by common_partition_specification and partition_key_value, "
            "beginning: {}, end: {}, position_deletes_objects size: {}",
            is_equality_delete ? "Equality" : "Position",
            beg_it - deletes_objects.begin(),
            end_it - deletes_objects.begin(),
            deletes_objects.size());
    }
    if (beg_it != end_it)
    {
        auto previous_it = std::prev(end_it);
        chassert(*beg_it);
        chassert(*previous_it);
        LOG_DEBUG(
            logger,
            "Preliminary check got {} {} delete elements for data file {}, taken data file object info: {}, first taken delete object info is "
            "{}, last taken "
            "delete object info is {}",
            std::distance(beg_it, end_it),
            is_equality_delete ? "equality" : "position",
            data_object_->parsed_entry->file_path_key,
            data_object_->dumpDeletesMatchingInfo(),
            (*beg_it)->dumpDeletesMatchingInfo(),
            (*previous_it)->dumpDeletesMatchingInfo());
    }
    else
    {
        LOG_DEBUG(
            logger,
            "No {} delete elements for data file {}, taken data file object info: {}",
            is_equality_delete ? "equality" : "position",
            data_object_->parsed_entry->file_path_key,
            data_object_->dumpDeletesMatchingInfo());
    }
    return {beg_it, end_it};
}

}

IcebergIterator::IcebergIterator(
    ObjectStoragePtr object_storage_,
    ContextPtr local_context_,
    const ActionsDAG * filter_dag_,
    IDataLakeMetadata::FileProgressCallback callback_,
    Iceberg::TableStateSnapshotPtr table_snapshot_,
    Iceberg::IcebergDataSnapshotPtr data_snapshot_,
    PersistentTableComponents persistent_components_)
    : logger(getLogger("IcebergIterator"))
    , object_storage(std::move(object_storage_))
    , local_context(local_context_)
    , table_state_snapshot(table_snapshot_)
    , data_snapshot(data_snapshot_)
    , persistent_components(persistent_components_)
    , manifest_filter_dag(makeManifestFilterDag(filter_dag_, local_context_))
    , callback(std::move(callback_))
{
    chassert(local_context);

    /// Every manifest entry is logged when the trace is requested, so the manifests cannot be skipped then.
    const bool per_entry_trace_requested
        = getIcebergMetadataLogLevel(local_context) >= DB::IcebergMetadataLogLevel::ManifestFileEntry;
    const bool manifest_list_pruning_enabled = manifest_filter_dag && data_snapshot && table_state_snapshot
        && data_snapshot->partition_specs && !per_entry_trace_requested
        && local_context->getSettingsRef()[Setting::use_iceberg_partition_pruning]
        && local_context->getSettingsRef()[Setting::use_iceberg_manifest_list_partition_pruning];

    /// The filter sets are shared with the reader of this table, which prepares them on its own
    /// thread, so they must be ready before any manifest is pruned.
    if (data_snapshot && manifest_filter_dag)
        VirtualColumnUtils::buildOrderedSetsForDAG(*manifest_filter_dag, local_context);

    /// The key conditions of the pruner are built over the filter DAG, so they need its ordered sets
    /// to be ready.
    std::unique_ptr<Iceberg::ManifestListPruner> manifest_list_pruner;
    if (manifest_list_pruning_enabled)
        manifest_list_pruner = std::make_unique<Iceberg::ManifestListPruner>(
            *persistent_components.schema_processor,
            table_state_snapshot->schema_id,
            data_snapshot->schema_id_on_snapshot_commit,
            data_snapshot->partition_specs,
            manifest_filter_dag.get(),
            local_context);

    ManifestFileCacheKeys delete_manifest_keys;
    ManifestFileCacheKeys data_manifest_keys;
    if (data_snapshot)
    {
        for (const auto & manifest_list_entry : data_snapshot->manifest_list_entries)
        {
            if (manifest_list_entry.content_type == Iceberg::ManifestFileContentType::DELETE)
            {
                delete_manifest_keys.push_back(manifest_list_entry);
                continue;
            }
            if (manifest_list_pruner
                && manifest_list_pruner->canBePruned(manifest_list_entry.partition_spec_id, manifest_list_entry.partition_summaries))
            {
                ProfileEvents::increment(ProfileEvents::IcebergPartitionPrunedManifestFiles);
                /// The data files of a skipped manifest are skipped by partition pruning just as the ones
                /// rejected entry by entry, so they are counted the same way; without this the counter
                /// silently drops to zero exactly when pruning got better.
                ProfileEvents::increment(ProfileEvents::IcebergPartitionPrunedFiles, manifest_list_entry.live_files_count);
                continue;
            }
            data_manifest_keys.push_back(manifest_list_entry);
        }
    }

    delete_manifests_left = delete_manifest_keys.size();
    ManifestFileCacheKeys manifest_keys = std::move(delete_manifest_keys);
    manifest_keys.insert(
        manifest_keys.end(), std::make_move_iterator(data_manifest_keys.begin()), std::make_move_iterator(data_manifest_keys.end()));
    manifests = std::make_unique<Iceberg::ManifestFilesPrefetcher>(
        object_storage, persistent_components, local_context, logger, std::move(manifest_keys));
}

void IcebergIterator::ensureDeletesReady()
{
    /// Threads arriving before the deletes are ready share the delete manifests; the last one to finish sorts.
    UniqueLock lock(deletes_mutex);
    while (!deletes_ready)
    {
        /// Stored rather than rethrown from a fresh parse, so that every `next` thread sees one error.
        if (deletes_exception)
            std::rethrow_exception(copyMutableException(deletes_exception));

        if (delete_manifests_left == 0)
        {
            if (delete_manifests_in_progress != 0)
            {
                delete_manifest_parsed.wait(lock.getUnderlyingLock());
                continue;
            }

            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergIteratorInitializationMicroseconds);
            /// Sort objects by common_partition_specification, partition_key_value and added_sequence_number.
            /// This is needed to efficiently match delete and data manifests in defineDeletesSpan().
            LOG_DEBUG(logger, "Taken {} position deletes file and {} equality deletes files in iceberg iterator", position_deletes_files.size(), equality_deletes_files.size());
            std::sort(equality_deletes_files.begin(), equality_deletes_files.end());
            std::sort(position_deletes_files.begin(), position_deletes_files.end());
            deletes_ready = true;
            delete_manifest_parsed.notify_all();
            break;
        }

        Iceberg::ManifestFilesPrefetcher::FetchPtr fetch;
        try
        {
            /// Lock order: `deletes_mutex`, then `data_files_mutex`.
            std::lock_guard data_lock(data_files_mutex);
            fetch = manifests->take();
        }
        catch (...)
        {
            deletes_exception = std::current_exception();
            delete_manifest_parsed.notify_all();
            throw;
        }

        if (!fetch)
        {
            /// Every slot is held by a delete manifest another thread is parsing.
            if (delete_manifests_in_progress == 0)
            {
                deletes_exception = std::make_exception_ptr(Exception(
                    ErrorCodes::LOGICAL_ERROR, "No read-ahead slot is free while no Iceberg delete manifest is being parsed"));
                delete_manifest_parsed.notify_all();
                continue;
            }
            delete_manifest_parsed.wait(lock.getUnderlyingLock());
            continue;
        }
        chassert(fetch->key().content_type == Iceberg::ManifestFileContentType::DELETE);
        --delete_manifests_left;
        ++delete_manifests_in_progress;
        lock.unlock();

        std::vector<Iceberg::ProcessedManifestFileEntryPtr> delete_files;
        std::exception_ptr error;
        try
        {
            ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergIteratorInitializationMicroseconds);
            auto manifest_file_iterator = createManifestIterator(fetch->key(), fetch->get());
            fetch.reset();
            while (auto entry = manifest_file_iterator->next())
                delete_files.push_back(std::move(entry));
        }
        catch (...)
        {
            error = std::current_exception();
        }
        fetch.reset();

        lock.lock();
        --delete_manifests_in_progress;
        if (!error)
        {
            try
            {
                for (auto & delete_file : delete_files)
                {
                    if (delete_file->parsed_entry->equality_ids.has_value())
                        equality_deletes_files.emplace_back(std::move(delete_file));
                    else
                        position_deletes_files.emplace_back(std::move(delete_file));
                }
            }
            catch (...)
            {
                error = std::current_exception();
            }
        }
        if (error && !deletes_exception)
            deletes_exception = error;
        delete_manifest_parsed.notify_all();
    }
}

Iceberg::ManifestIteratorPtr IcebergIterator::createManifestIterator(
    const ManifestFileCacheKey & manifest_list_entry, const Iceberg::ManifestFileCacheableInfo & manifest_file) const
{
    return Iceberg::ManifestFileIterator::create(
        manifest_file.getDeserializer(),
        manifest_list_entry.manifest_file_path,
        persistent_components.path_resolver,
        *persistent_components.schema_processor,
        manifest_list_entry.added_sequence_number,
        manifest_list_entry.added_snapshot_id,
        manifest_list_entry.first_row_id,
        local_context,
        manifest_filter_dag,
        table_state_snapshot->schema_id);
}

Iceberg::ProcessedManifestFileEntryPtr IcebergIterator::nextDataFileEntry()
{
    /// The threads calling `next` share the data manifests: each one continues an idle partially read
    /// manifest, or else parses the next one, so with one thread the files come in the manifests' order.
    UniqueLock lock(data_files_mutex);
    while (true)
    {
        if (data_files_exception)
            std::rethrow_exception(copyMutableException(data_files_exception));

        Iceberg::ManifestIteratorPtr manifest;
        Iceberg::ManifestFilesPrefetcher::FetchPtr fetch;
        if (!idle_data_manifests.empty())
        {
            manifest = std::move(idle_data_manifests.back());
            idle_data_manifests.pop_back();
        }
        else
        {
            try
            {
                fetch = manifests->take();
            }
            catch (...)
            {
                data_files_exception = std::current_exception();
                data_manifest_released.notify_all();
                throw;
            }

            if (!fetch)
            {
                if (data_manifests_in_use == 0)
                    return nullptr;
                data_manifest_released.wait(lock.getUnderlyingLock());
                continue;
            }
        }

        ++data_manifests_in_use;
        lock.unlock();

        Iceberg::ProcessedManifestFileEntryPtr entry;
        std::exception_ptr error;
        try
        {
            if (fetch)
            {
                manifest = createManifestIterator(fetch->key(), fetch->get());
                fetch.reset();
            }
            entry = manifest->next();
        }
        catch (...)
        {
            error = std::current_exception();
        }
        fetch.reset();
        if (!entry)
            manifest.reset();

        lock.lock();
        --data_manifests_in_use;
        if (!error && entry)
        {
            try
            {
                idle_data_manifests.push_back(std::move(manifest));
            }
            catch (...)
            {
                error = std::current_exception();
            }
        }
        if (error && !data_files_exception)
            data_files_exception = error;
        data_manifest_released.notify_all();

        if (!error && entry)
            return entry;
    }
}

ObjectInfoPtr IcebergIterator::next(size_t)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergMetadataReadWaitTimeMicroseconds);
    ensureDeletesReady();
    if (auto manifest_file_entry = nextDataFileEntry())
    {
        IcebergDataObjectInfoPtr object_info
            = std::make_shared<IcebergDataObjectInfo>(
                manifest_file_entry,
                persistent_components.path_resolver.resolve(manifest_file_entry->parsed_entry->file_path_key),
                table_state_snapshot->schema_id,
                Iceberg::getIdentityPartitionColumnValues(*manifest_file_entry, *persistent_components.schema_processor));
        for (const auto & position_delete :
             defineDeletesSpan(manifest_file_entry, position_deletes_files, /* is_equality_delete */ false, logger))
        {
            const auto & data_file_path = object_info->info.data_object_file_path_key;
            const auto & lower = position_delete->parsed_entry->lower_reference_data_file_path;
            const auto & upper = position_delete->parsed_entry->upper_reference_data_file_path;
            bool can_contain_data_file_deletes
                = (!lower.has_value() || *lower <= data_file_path)
                && (!upper.has_value() || *upper >= data_file_path);
            /// Skip position deletes that do not match the data file path.
            if (!can_contain_data_file_deletes)
            {
                ProfileEvents::increment(ProfileEvents::IcebergMinMaxPrunedDeleteFiles);
                LOG_TEST(
                    logger,
                    "Skipping position delete file `{}` for data file `{}` because position delete has out of bounds reference data file "
                    "bounds: "
                    "(lower bound: `{}`, upper bound: `{}`)",
                    position_delete->parsed_entry->file_path_key,
                    data_file_path,
                    lower.has_value() ? lower->serialize() : "[no lower bound]",
                    upper.has_value() ? upper->serialize() : "[no upper bound]");
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::IcebergMinMaxNonPrunedDeleteFiles);
                LOG_TEST(
                    logger,
                    "Processing position delete file `{}` for data file `{}` with reference data file bounds: "
                    "(lower bound: `{}`, upper bound: `{}`)",
                    position_delete->parsed_entry->file_path_key,
                    data_file_path,
                    lower.has_value() ? lower->serialize() : "[no lower bound]",
                    upper.has_value() ? upper->serialize() : "[no upper bound]");
                const auto resolved_delete_path = persistent_components.path_resolver.resolve(position_delete->parsed_entry->file_path_key);
                if (position_delete->parsed_entry->isDeletionVector())
                    object_info->addDeletionVector(position_delete, resolved_delete_path);
                else
                    object_info->addPositionDeleteFile(position_delete, resolved_delete_path);
            }
        }

        if (object_info->info.hasPositionDeletes())
        {
            LOG_DEBUG(
                logger,
                "Finally got {} position delete elements for data file {}",
                object_info->info.position_deletes_objects.size(),
                object_info->info.data_object_file_path_key);
        }

        for (const auto & equality_delete :
             defineDeletesSpan(manifest_file_entry, equality_deletes_files, /* is_equality_delete */ true, logger))
        {
            object_info->addEqualityDeleteObject(
                equality_delete, persistent_components.path_resolver.resolve(equality_delete->parsed_entry->file_path_key));
        }

        if (!object_info->info.equality_deletes_objects.empty())
        {
            LOG_DEBUG(
                logger,
                "Finally got {} equality delete elements for data file {}",
                object_info->info.equality_deletes_objects.size(),
                object_info->info.data_object_file_path_key);
        }

        ProfileEvents::increment(ProfileEvents::IcebergMetadataReturnedObjectInfos);

        if (callback)
            callback(FileProgress(0, size_t(manifest_file_entry->parsed_entry->file_size_in_bytes)));

        return object_info;
    }

    return nullptr;
}

size_t IcebergIterator::estimatedKeysCount()
{
    if (!data_snapshot)
        return 0;

    size_t data_files_count = 0;
    for (const auto & manifest_list_entry : data_snapshot->manifest_list_entries)
    {
        if (manifest_list_entry.content_type != Iceberg::ManifestFileContentType::DATA)
            continue;
        if (manifest_list_entry.live_files_count <= 0)
            return std::numeric_limits<size_t>::max();
        data_files_count += static_cast<size_t>(manifest_list_entry.live_files_count);
    }
    return data_files_count;
}

IcebergIterator::~IcebergIterator() = default;
}

#endif
