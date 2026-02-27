#include "config.h"
#if USE_AVRO

#include <cstddef>
#include <memory>
#include <optional>
#include <Formats/FormatFilterInfo.h>
#include <Formats/FormatParserSharedResources.h>
#include <Processors/Formats/Impl/ParquetBlockInputFormat.h>
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
extern const Event IcebergPartitionPrunedFiles;
extern const Event IcebergMinMaxIndexPrunedFiles;
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
};


using namespace Iceberg;

template <typename HeavyCPUProcessingFunction>
size_t HomogeneousIcebergKeysIterator<HeavyCPUProcessingFunction>::ManifestFileWeightFunction::operator()(
    const Iceberg::ManifestFilePtr & manifest_file) const
{
    return manifest_file->getFileBytesSize();
}

template <typename HeavyCPUProcessingFunction>
HomogeneousIcebergKeysIterator<HeavyCPUProcessingFunction>::ManifestFilesAsyncronousIterator::ManifestFilesAsyncronousIterator(
    Iceberg::ManifestFileContentType manifest_file_content_type_,
    const HomogeneousIcebergKeysIterator & parent_,
    size_t max_sum_size_of_manifest_files_in_queue_,
    size_t number_of_workers_)
    : blocking_queue(max_sum_size_of_manifest_files_in_queue_)
    , index(0)
    , manifest_file_content_type(manifest_file_content_type_)
    , parent(parent_)
    , number_of_workers(number_of_workers_)
{
    for (size_t i = 0; i < number_of_workers; ++i)
    {
        workers.push_back(ThreadFromGlobalPool(
            [this]()
            {
                while (!is_cancelled)
                {
                    auto iterator = task();
                    if (!iterator)
                    {
                        break;
                    }
                    while (!is_cancelled.load() && !blocking_queue.push(iterator))
                    {
                    }
                }
            }));
    }
}

template <typename HeavyCPUProcessingFunction>
void HomogeneousIcebergKeysIterator<HeavyCPUProcessingFunction>::ManifestFilesAsyncronousIterator::cancel()
{
    is_cancelled.store(true);
    for (auto & worker : workers)
    {
        worker.join();
    }
}

template <typename HeavyCPUProcessingFunction>
Iceberg::ManifestFilePtr HomogeneousIcebergKeysIterator<HeavyCPUProcessingFunction>::ManifestFilesAsyncronousIterator::task()
{
    while (!is_cancelled.load())
    {
        auto current_index = index.fetch_add(1);
        if (current_index >= parent.data_snapshot->manifest_list_entries.size())
        {
            return nullptr;
        }
        if (parent.persistent_components.format_version > 1
            && parent.data_snapshot->manifest_list_entries[current_index].content_type != manifest_file_content_type)
        {
            continue;
        }

        auto manifest_file_cacheable_part = Iceberg::getManifestFile(
            parent.object_storage,
            parent.persistent_components,
            parent.local_context,
            parent.log,
            parent.data_snapshot->manifest_list_entries[current_index].manifest_file_path,
            parent.data_snapshot->manifest_list_entries[current_index].manifest_file_byte_size);

        if (is_cancelled.load())
        {
            return nullptr;
        }

        // TODO: recall what was the difference between file_path and filename
        return std::make_shared<Iceberg::ManifestFileIterator>(
            manifest_file_cacheable_part.deserializer,
            parent.data_snapshot->manifest_list_entries[current_index].manifest_file_path,
            parent.persistent_components.format_version,
            parent.persistent_components.table_path,
            *parent.persistent_components.schema_processor,
            parent.data_snapshot->manifest_list_entries[current_index].added_sequence_number,
            parent.data_snapshot->manifest_list_entries[current_index].added_snapshot_id,
            parent.persistent_components.table_location,
            parent.local_context,
            parent.data_snapshot->manifest_list_entries[current_index].manifest_file_path,
            parent.use_partition_pruning,
            parent.filter_dag,
            parent.table_snapshot->schema_id);
    }

    return nullptr;
}

template <typename HeavyCPUProcessingFunction>
Iceberg::ManifestFilePtr HomogeneousIcebergKeysIterator<HeavyCPUProcessingFunction>::ManifestFilesAsyncronousIterator::next() {
    ManifestFilePtr manifest_file;
    if (!blocking_queue.pop(manifest_file))
    {
        return nullptr;
    }
    return manifest_file;
}

namespace
{
std::span<const ManifestFileEntryPtr> defineDeletesSpan(
    ManifestFileEntryPtr data_object_, const std::vector<ManifestFileEntryPtr> & deletes_objects, bool is_equality_delete, LoggerPtr logger)
{
    if (deletes_objects.empty())
    {
        return {};
    }
    ///Object in deletes_objects are sorted by common_partition_specification, partition_key_value and added_sequence_number.
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
        [](const ManifestFileEntryPtr & lhs, const ManifestFileEntryPtr & rhs)
        {
            return std::tie(*lhs->common_partition_specification, lhs->pure_entry->partition_key_value)
                < std::tie(*rhs->common_partition_specification, rhs->pure_entry->partition_key_value);
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
        assert(*beg_it);
        assert(*previous_it);
        LOG_DEBUG(
            logger,
            "Preliminary check got {} {} delete elements for data file {}, taken data file object info: {}, first taken delete object info is "
            "{}, last taken "
            "delete object info is {}",
            std::distance(beg_it, end_it),
            is_equality_delete ? "equality" : "position",
            data_object_->file_path,
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
            data_object_->file_path,
            data_object_->dumpDeletesMatchingInfo());
    }
    return {beg_it, end_it};
}

}

template <typename HeavyCPUProcessingFunction>
std::optional<typename HomogeneousIcebergKeysIterator<HeavyCPUProcessingFunction>::Result>
HomogeneousIcebergKeysIterator<HeavyCPUProcessingFunction>::next()
{
    ManifestFilePtr reference_to_iterator;
    while (true)
    {
        {
            std::lock_guard lock(current_manifest_file_mutex);
            if (!current_manifest_file_iterator || current_manifest_file_iterator->isInitialized())
            {
                current_manifest_file_iterator = manifest_files_asyncronous_iterator.next();
            }
            if (current_manifest_file_iterator)
            {
                reference_to_iterator = current_manifest_file_iterator;
            } else {
                return std::nullopt;
            }
        }
        auto file_entry = reference_to_iterator->next();
        if (!file_entry)
        {   
            continue;
        }
        return processing_function(file_entry);
    }
}

template <typename HeavyCPUProcessingFunction>
HomogeneousIcebergKeysIterator<HeavyCPUProcessingFunction>::HomogeneousIcebergKeysIterator(
        ObjectStoragePtr object_storage_,
        ContextPtr local_context_,
        Iceberg::ManifestFileContentType manifest_file_content_type_,
        const ActionsDAG * filter_dag_,
        TableStateSnapshotPtr table_snapshot_,
        IcebergDataSnapshotPtr data_snapshot_,
        PersistentTableComponents persistent_components_,
        HeavyCPUProcessingFunction processing_function_)
    : object_storage(object_storage_)
    , filter_dag(filter_dag_ ? std::make_shared<ActionsDAG>(filter_dag_->clone()) : nullptr)
    , local_context(local_context_)
    , table_snapshot(table_snapshot_)
    , data_snapshot(data_snapshot_)
    , use_partition_pruning(
          [this]()
          {
              if (!local_context && filter_dag)
              {
                  throw DB::Exception(
                      DB::ErrorCodes::LOGICAL_ERROR,
                      "Context is required with non-empty filter_dag to implement "
                      "partition pruning for Iceberg table");
              }
              return filter_dag && local_context->getSettingsRef()[Setting::use_iceberg_partition_pruning].value;
          }())
    , persistent_components(persistent_components_)
    , log(getLogger("IcebergIterator"))
    , manifest_file_content_type(manifest_file_content_type_)
    , processing_function(std::move(processing_function_))
    // TODO: make it configurable
    , manifest_files_asyncronous_iterator(
        manifest_file_content_type_,
        *this,
        20,
    5)
{
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
    , filter_dag(filter_dag_ ? std::make_shared<ActionsDAG>(filter_dag_->clone()) : nullptr)
    , object_storage(std::move(object_storage_))
    , table_state_snapshot(table_snapshot_)
    , persistent_components(persistent_components_)
    , deletes_iterator(
          object_storage,
          local_context_,
          Iceberg::ManifestFileContentType::DELETE,
          filter_dag.get(),
          table_snapshot_,
          data_snapshot_,
          persistent_components_,
        [](Iceberg::ManifestFileEntryPtr manifest_file_entry){return manifest_file_entry;})
    , callback(std::move(callback_))
{
    auto delete_file = deletes_iterator.next();
    while (delete_file.has_value())
    {
        if (delete_file.value()->pure_entry->equality_ids.has_value())
        {
            equality_deletes_files.emplace_back(std::move(delete_file.value()));
        }
        else
        {
            position_deletes_files.emplace_back(std::move(delete_file.value()));
        }
        delete_file = deletes_iterator.next();
    }
    LOG_DEBUG(logger, "Taken {} position deletes file and {} equality deletes files in iceberg iterator", position_deletes_files.size(), equality_deletes_files.size());
    std::sort(equality_deletes_files.begin(), equality_deletes_files.end());
    std::sort(position_deletes_files.begin(), position_deletes_files.end());
    data_files_iterator.emplace(
          object_storage,
          local_context_,
          Iceberg::ManifestFileContentType::DATA,
          filter_dag.get(),
          table_snapshot_,
          data_snapshot_,
          persistent_components_,
          [this](Iceberg::ManifestFileEntryPtr manifest_file_entry){return convertToObjectInfo(manifest_file_entry);});
}

ObjectInfoPtr IcebergIterator::convertToObjectInfo(const Iceberg::ManifestFileEntryPtr & manifest_file_entry)
{
    IcebergDataObjectInfoPtr object_info
            = std::make_shared<IcebergDataObjectInfo>(manifest_file_entry, table_state_snapshot->schema_id);
    for (const auto & position_delete :
             defineDeletesSpan(manifest_file_entry, position_deletes_files, /* is_equality_delete */ false, logger))
        {
            const auto & data_file_path = object_info->info.data_object_file_path_key;
            const auto & lower = position_delete->pure_entry->lower_reference_data_file_path;
            const auto & upper = position_delete->pure_entry->upper_reference_data_file_path;
            bool can_contain_data_file_deletes
                = (!lower.has_value() || lower.value() <= data_file_path) && (!upper.has_value() || upper.value() >= data_file_path);
            /// Skip position deletes that do not match the data file path.
            if (!can_contain_data_file_deletes)
            {
                ProfileEvents::increment(ProfileEvents::IcebergMinMaxPrunedDeleteFiles);
                LOG_TEST(
                    logger,
                    "Skipping position delete file `{}` for data file `{}` because position delete has out of bounds reference data file "
                    "bounds: "
                    "(lower bound: `{}`, upper bound: `{}`)",
                    position_delete->file_path,
                    data_file_path,
                    lower.value_or("[no lower bound]"),
                    upper.value_or("[no upper bound]"));
            }
            else
            {
                ProfileEvents::increment(ProfileEvents::IcebergMinMaxNonPrunedDeleteFiles);
                LOG_TEST(
                    logger,
                    "Processing position delete file `{}` for data file `{}` with reference data file bounds: "
                    "(lower bound: `{}`, upper bound: `{}`)",
                    position_delete->file_path,
                    data_file_path,
                    lower.value_or("[no lower bound]"),
                    upper.value_or("[no upper bound]"));
                object_info->addPositionDeleteObject(position_delete);
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
            object_info->addEqualityDeleteObject(equality_delete);
        }

        if (!object_info->info.equality_deletes_objects.empty())
        {
            LOG_DEBUG(
                logger,
                "Finally got {} equality delete elements for data file {}",
                object_info->info.equality_deletes_objects.size(),
                object_info->info.data_object_file_path_key);
        }



    return object_info;
}

ObjectInfoPtr IcebergIterator::next(size_t)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergMetadataReadWaitTimeMicroseconds);
    
    ProfileEvents::increment(ProfileEvents::IcebergMetadataReturnedObjectInfos);
    auto object_info = data_files_iterator->next();
    return object_info.has_value() ? object_info.value() : nullptr;
}


size_t IcebergIterator::estimatedKeysCount()
{
    return std::numeric_limits<size_t>::max();
}


void IcebergIterator::cancel()
{
    if (data_files_iterator)
        data_files_iterator->cancel();
    deletes_iterator.cancel();
}
}

#endif
