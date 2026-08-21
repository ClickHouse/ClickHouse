#include "config.h"
#include <Common/CurrentThread.h>
#if USE_AVRO

#include <cstddef>
#include <deque>
#include <memory>
#include <optional>
#include <base/scope_guard.h>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Processors/Formats/Impl/ParquetV3BlockInputFormat.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Stringifier.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>


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
#include <IO/SharedThreadPools.h>
#include <IO/Progress.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

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
extern const SettingsNonZeroUInt64 iceberg_delete_manifest_decode_concurrency;
};


using namespace Iceberg;

namespace
{

/// All entries of one delete manifest file, produced by a single decode task.
using DeleteManifestBatch = std::vector<ProcessedManifestFileEntryPtr>;

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
            return std::tie(*lhs->common_partition_specification, lhs->parsed_entry->partition_key_value)
                < std::tie(*rhs->common_partition_specification, rhs->parsed_entry->partition_key_value);
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

std::optional<ProcessedManifestFileEntryPtr> SingleThreadIcebergKeysIterator::next()
{
    if (!data_snapshot)
    {
        return std::nullopt;
    }

    while (true)
    {
        /// Try to get the next entry from the current manifest file iterator.
        if (current_manifest_file_iterator)
        {
            auto entry = current_manifest_file_iterator->next();
            if (entry)
                return entry;

            /// next() returned nullptr, meaning the manifest file is fully exhausted.
            current_manifest_file_iterator = nullptr;
        }

        /// Make sure a prefetch of the next matching manifest is in flight.
        schedulePrefetchIfPossible();
        if (!prefetched_manifest.has_value())
            return std::nullopt;

        /// Take ownership of the in-flight prefetch: move it out of the member and reset the
        /// optional (a moved-from optional is still engaged, so schedulePrefetchIfPossible would
        /// otherwise see an occupied slot and never start the next fetch).
        auto curr_prefetched = std::move(prefetched_manifest);
        prefetched_manifest.reset();
        /// While the future lives only in this local, the destructor's wait no longer covers it,
        /// so the task capturing `this` could outlive the iterator if anything below throws
        /// (for example scheduling the next prefetch while the pool is shutting down).
        SCOPE_EXIT({
            if (curr_prefetched.has_value() && curr_prefetched->future.valid())
                curr_prefetched->future.wait();
        });

        /// Start scheduling the next prefetch before we block and parse.
        schedulePrefetchIfPossible();

        auto manifest_file_cacheable_part = curr_prefetched->future.get();
        const auto & manifest_list_entry = data_snapshot->manifest_list_entries[curr_prefetched->manifest_list_index];

        current_manifest_file_iterator = Iceberg::ManifestFileIterator::create(
            manifest_file_cacheable_part.deserializer,
            manifest_list_entry.manifest_file_path,
            persistent_components.path_resolver,
            *persistent_components.schema_processor,
            manifest_list_entry.added_sequence_number,
            manifest_list_entry.added_snapshot_id,
            data_snapshot->snapshot_id,
            local_context,
            filter_dag,
            table_snapshot->schema_id);
    }
}

void SingleThreadIcebergKeysIterator::schedulePrefetchIfPossible()
{
    if (!data_snapshot || prefetched_manifest.has_value())
        return;

    while (manifest_file_index < data_snapshot->manifest_list_entries.size())
    {
        const size_t index = manifest_file_index++;
        const auto & manifest_list_entry = data_snapshot->manifest_list_entries[index];
        if (manifest_list_entry.content_type != manifest_file_content_type)
            continue;

        auto fetch = [this,
                      path = manifest_list_entry.manifest_file_path,
                      bytes = manifest_list_entry.manifest_file_byte_size]()
        {
            return Iceberg::getManifestFile(object_storage, persistent_components, local_context, log, path, bytes);
        };
        prefetched_manifest = PrefetchedManifest{index, prefetch_runner(std::move(fetch), Priority{})};
        return;
    }
}

SingleThreadIcebergKeysIterator::~SingleThreadIcebergKeysIterator()
{
    /// The scheduled task captures `this`, so it must not outlive the iterator.
    if (prefetched_manifest.has_value() && prefetched_manifest->future.valid())
        prefetched_manifest->future.wait();
}

SingleThreadIcebergKeysIterator::SingleThreadIcebergKeysIterator(
    ObjectStoragePtr object_storage_,
    ContextPtr local_context_,
    Iceberg::ManifestFileContentType manifest_file_content_type_,
    const ActionsDAG * filter_dag_,
    Iceberg::TableStateSnapshotPtr table_snapshot_,
    Iceberg::IcebergDataSnapshotPtr data_snapshot_,
    PersistentTableComponents persistent_components_)
    : object_storage(object_storage_)
    , filter_dag(makeManifestFilterDag(filter_dag_, local_context_))
    , local_context(local_context_)
    , table_snapshot(table_snapshot_)
    , data_snapshot(data_snapshot_)
    , persistent_components(persistent_components_)
    , log(getLogger("IcebergIterator"))
    , manifest_file_content_type(manifest_file_content_type_)
    , prefetch_runner(threadPoolCallbackRunnerUnsafe<Iceberg::ManifestFileCacheableInfo>(
          getIOThreadPool().get(), DB::ThreadName::ICEBERG_ITERATOR))
{
    /// Warm the first manifest fetch.
    schedulePrefetchIfPossible();
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
    , deletes_filter_dag(makeManifestFilterDag(filter_dag_, local_context_))
    , data_files_iterator(
          object_storage,
          local_context_,
          Iceberg::ManifestFileContentType::DATA,
          filter_dag_,
          table_snapshot_,
          data_snapshot_,
          persistent_components_)
    , blocking_queue(100)
    , callback(std::move(callback_))
{
    /// Decoding any manifest reads settings from the context, so a missing one is fatal either way.
    if (!local_context)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Context is required to construct IcebergIterator");

    producer_task = std::make_unique<ThreadFromGlobalPool>(
        [this, thread_group = CurrentThread::getGroup()]()
        {
            DB::ThreadGroupSwitcher switcher(thread_group, DB::ThreadName::ICEBERG_ITERATOR);
            while (!blocking_queue.isFinished())
            {
                std::optional<ProcessedManifestFileEntryPtr> entry;
                try
                {
                    entry = data_files_iterator.next();
                }
                catch (...)
                {
                    std::lock_guard lock(exception_mutex);
                    if (!exception)
                    {
                        exception = std::current_exception();
                    }
                    blocking_queue.finish();
                    break;
                }
                if (!entry.has_value())
                    break;
                while (!blocking_queue.push(std::move(entry.value())))
                {
                    if (blocking_queue.isFinished())
                    {
                        break;
                    }
                }
            }
            blocking_queue.finish();
        });
}

// ensureDeletesReady is called lazily so that delete and data manifest downloads overlap.
void IcebergIterator::ensureDeletesReady()
{
    std::lock_guard lock(deletes_mutex);
    if (!deletes_ready)
    {
        /// Deferred part of the iterator initialization, charged to the same event as the constructor.
        ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergIteratorInitializationMicroseconds);
        try
        {
            decodeDeleteManifests();
        }
        catch (...)
        {
            deletes_exception = std::current_exception();
            /// Every `next` rethrows this exception, so the buffered entries are dead and the producer must stop.
            blocking_queue.clearAndFinish();
        }
        deletes_ready = true;
    }
    /// Stored rather than rethrown from a fresh decode, so that every `next` thread sees one error.
    if (deletes_exception)
        std::rethrow_exception(deletes_exception);
}

void IcebergIterator::decodeDeleteManifests()
{
    std::vector<ManifestFileCacheKey> delete_manifests;
    if (data_snapshot)
    {
        for (const auto & manifest_list_entry : data_snapshot->manifest_list_entries)
        {
            if (manifest_list_entry.content_type == Iceberg::ManifestFileContentType::DELETE)
                delete_manifests.push_back(manifest_list_entry);
        }
    }

    /// Cap concurrency: each in-flight manifest holds its decoded contents.
    const size_t max_in_flight = local_context->getSettingsRef()[Setting::iceberg_delete_manifest_decode_concurrency];

    auto decode_runner
        = threadPoolCallbackRunnerUnsafe<DeleteManifestBatch>(getIOThreadPool().get(), DB::ThreadName::ICEBERG_ITERATOR);

    std::deque<std::future<DeleteManifestBatch>> in_flight;
    /// The tasks capture `this`, so none of them may still be running when this function is left.
    SCOPE_EXIT({
        for (auto & future : in_flight)
        {
            if (future.valid())
                future.wait();
        }
    });

    size_t next_to_decode = 0;
    while (next_to_decode < delete_manifests.size() || !in_flight.empty())
    {
        while (in_flight.size() < max_in_flight && next_to_decode < delete_manifests.size())
        {
            auto decode = [this, manifest_list_entry = delete_manifests[next_to_decode++]]()
            {
                auto manifest_file_cacheable_part = Iceberg::getManifestFile(
                    object_storage,
                    persistent_components,
                    local_context,
                    logger,
                    manifest_list_entry.manifest_file_path,
                    manifest_list_entry.manifest_file_byte_size);

                auto manifest_file_iterator = Iceberg::ManifestFileIterator::create(
                    manifest_file_cacheable_part.deserializer,
                    manifest_list_entry.manifest_file_path,
                    persistent_components.path_resolver,
                    *persistent_components.schema_processor,
                    manifest_list_entry.added_sequence_number,
                    manifest_list_entry.added_snapshot_id,
                    data_snapshot->snapshot_id,
                    local_context,
                    deletes_filter_dag,
                    table_state_snapshot->schema_id);

                DeleteManifestBatch batch;
                while (auto entry = manifest_file_iterator->next())
                    batch.push_back(entry);
                /// Iterator and deserializer die here, before the batch is handed over.
                return batch;
            };
            in_flight.push_back(decode_runner(std::move(decode), Priority{}));
        }

        auto pending = std::move(in_flight.front());
        in_flight.pop_front();
        /// Collected in manifest list order, so the failure reported is the first one in that order.
        for (auto & delete_file : pending.get())
        {
            if (delete_file->parsed_entry->equality_ids.has_value())
                equality_deletes_files.emplace_back(std::move(delete_file));
            else
                position_deletes_files.emplace_back(std::move(delete_file));
        }
    }
    chassert(in_flight.empty());
    chassert(next_to_decode == delete_manifests.size());

    /// Sort objects by common_partition_specification, partition_key_value and added_sequence_number.
    /// This is needed to efficiently match delete and data manifests in defineDeletesSpan().
    LOG_DEBUG(logger, "Taken {} position deletes file and {} equality deletes files in iceberg iterator", position_deletes_files.size(), equality_deletes_files.size());
    std::sort(equality_deletes_files.begin(), equality_deletes_files.end());
    std::sort(position_deletes_files.begin(), position_deletes_files.end());
}

ObjectInfoPtr IcebergIterator::next(size_t)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergMetadataReadWaitTimeMicroseconds);
    ensureDeletesReady();
    Iceberg::ProcessedManifestFileEntryPtr manifest_file_entry;
    if (blocking_queue.pop(manifest_file_entry))
    {
        IcebergDataObjectInfoPtr object_info
            = std::make_shared<IcebergDataObjectInfo>(
                manifest_file_entry,
                persistent_components.path_resolver.resolve(manifest_file_entry->parsed_entry->file_path_key),
                table_state_snapshot->schema_id);
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
                object_info->addPositionDeleteObject(
                    position_delete, persistent_components.path_resolver.resolve(position_delete->parsed_entry->file_path_key));
            }
        }

        if (!object_info->info.position_deletes_objects.empty())
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
    {
        std::lock_guard lock(exception_mutex);
        if (exception)
        {
            auto exception_message = getExceptionMessage(exception, true, true);
            auto exception_code = getExceptionErrorCode(exception);
            throw DB::Exception(exception_code, "Iceberg iterator is failed with exception: {}", exception_message);
        }
    }

    return nullptr;
}

size_t IcebergIterator::estimatedKeysCount()
{
    return std::numeric_limits<size_t>::max();
}

IcebergIterator::~IcebergIterator()
{
    blocking_queue.finish();
    if (producer_task)
    {
        producer_task->join();
    }
}
}

#endif
