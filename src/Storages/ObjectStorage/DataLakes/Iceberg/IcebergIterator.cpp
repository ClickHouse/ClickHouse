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
            return std::tie(lhs->common_partition_specification, lhs->partition_key_value)
                < std::tie(rhs->common_partition_specification, rhs->partition_key_value);
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

std::optional<ManifestFileEntryPtr> SingleThreadIcebergKeysIterator::next()
{
    if (!data_snapshot)
    {
        return std::nullopt;
    }

    while (manifest_file_index < data_snapshot->manifest_list_entries.size())
    {
        if (!current_manifest_file_content)
        {
            if (persistent_components.format_version > 1 && data_snapshot->manifest_list_entries[manifest_file_index].content_type != manifest_file_content_type)
            {
                ++manifest_file_index;
                continue;
            }
            current_manifest_file_content = Iceberg::getManifestFile(
                object_storage,
                persistent_components,
                local_context,
                log,
                data_snapshot->manifest_list_entries[manifest_file_index].manifest_file_path,
                data_snapshot->manifest_list_entries[manifest_file_index].added_sequence_number,
                data_snapshot->manifest_list_entries[manifest_file_index].added_snapshot_id);
            internal_data_index = 0;
            files = files_generator(current_manifest_file_content);
        }
        while (internal_data_index < files.size())
        {
            const ManifestFileEntryPtr & manifest_file_entry = files[internal_data_index++];
            if ((manifest_file_entry->schema_id != previous_entry_schema) && (use_partition_pruning))
            {
                previous_entry_schema = manifest_file_entry->schema_id;
                if (previous_entry_schema > manifest_file_entry->schema_id)
                {
                    LOG_WARNING(
                        log,
                        "Manifest entries in file {} are not sorted by schema id",
                        current_manifest_file_content->getPathToManifestFile());
                }
                current_pruner.emplace(
                    *persistent_components.schema_processor,
                    table_snapshot->schema_id,
                    manifest_file_entry->schema_id,
                    filter_dag.get(),
                    *current_manifest_file_content,
                    local_context);
            }
            auto pruning_status = current_pruner ? current_pruner->canBePruned(manifest_file_entry) : PruningReturnStatus::NOT_PRUNED;
            insertRowToLogTable(
                local_context,
                "",
                DB::IcebergMetadataLogLevel::ManifestFileEntry,
                persistent_components.table_path,
                current_manifest_file_content->getPathToManifestFile(),
                manifest_file_entry->row_number,
                pruning_status);
            switch (pruning_status)
            {
                case PruningReturnStatus::NOT_PRUNED:
                    return manifest_file_entry;
                case PruningReturnStatus::MIN_MAX_INDEX_PRUNED: {
                    ProfileEvents::increment(ProfileEvents::IcebergMinMaxIndexPrunedFiles, 1);
                    break;
                }
                case PruningReturnStatus::PARTITION_PRUNED: {
                    ProfileEvents::increment(ProfileEvents::IcebergPartitionPrunedFiles, 1);
                    break;
                }
            }
        }
        current_manifest_file_content = nullptr;
        files.clear();
        current_pruner = std::nullopt;
        ++manifest_file_index;
        internal_data_index = 0;
        previous_entry_schema = -1;
    }

    return std::nullopt;
}

SingleThreadIcebergKeysIterator::SingleThreadIcebergKeysIterator(
    ObjectStoragePtr object_storage_,
    ContextPtr local_context_,
    FilesGenerator files_generator_,
    Iceberg::ManifestFileContentType manifest_file_content_type_,
    const ActionsDAG * filter_dag_,
    Iceberg::TableStateSnapshotPtr table_snapshot_,
    Iceberg::IcebergDataSnapshotPtr data_snapshot_,
    PersistentTableComponents persistent_components_)
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
    , files_generator(files_generator_)
    , log(getLogger("IcebergIterator"))
    , manifest_file_content_type(manifest_file_content_type_)
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
    , filter_dag(filter_dag_ ? std::make_unique<ActionsDAG>(filter_dag_->clone()) : nullptr)
    , object_storage(std::move(object_storage_))
    , table_state_snapshot(table_snapshot_)
    , persistent_components(persistent_components_)
    , data_files_iterator(
          object_storage,
          local_context_,
          [](const Iceberg::ManifestFilePtr & manifest_file)
          { return manifest_file->getFilesWithoutDeleted(Iceberg::FileContentType::DATA); },
          Iceberg::ManifestFileContentType::DATA,
          filter_dag.get(),
          table_snapshot_,
          data_snapshot_,
          persistent_components_)
    , deletes_iterator(
          object_storage,
          local_context_,
          [](const Iceberg::ManifestFilePtr & manifest_file)
          {
              auto position_deletes = manifest_file->getFilesWithoutDeleted(Iceberg::FileContentType::POSITION_DELETE);
              auto equality_deletes = manifest_file->getFilesWithoutDeleted(Iceberg::FileContentType::EQUALITY_DELETE);
              position_deletes.insert(position_deletes.end(), equality_deletes.begin(), equality_deletes.end());
              return position_deletes;
          },
          Iceberg::ManifestFileContentType::DELETE,
          filter_dag.get(),
          table_snapshot_,
          data_snapshot_,
          persistent_components_)
    , blocking_queue(100)
    , producer_task(std::nullopt)
    , callback(std::move(callback_))
{
    auto delete_file = deletes_iterator.next();
    while (delete_file.has_value())
    {
        if (delete_file.value()->equality_ids.has_value())
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
    producer_task.emplace(
        [this, thread_group = CurrentThread::getGroup()]()
        {
            DB::ThreadGroupSwitcher switcher(thread_group, DB::ThreadName::ICEBERG_ITERATOR);
            while (!blocking_queue.isFinished())
            {
                std::optional<ManifestFileEntryPtr> entry;
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

ObjectInfoPtr IcebergIterator::next(size_t)
{
    ProfileEventTimeIncrement<Microseconds> watch(ProfileEvents::IcebergMetadataReadWaitTimeMicroseconds);
    Iceberg::ManifestFileEntryPtr manifest_file_entry;
    if (blocking_queue.pop(manifest_file_entry))
    {
        IcebergDataObjectInfoPtr object_info
            = std::make_shared<IcebergDataObjectInfo>(manifest_file_entry, table_state_snapshot->schema_id);
        for (const auto & position_delete :
             defineDeletesSpan(manifest_file_entry, position_deletes_files, /* is_equality_delete */ false, logger))
        {
            const auto & data_file_path = object_info->info.data_object_file_path_key;
            const auto & lower = position_delete->lower_reference_data_file_path;
            const auto & upper = position_delete->upper_reference_data_file_path;
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

        ProfileEvents::increment(ProfileEvents::IcebergMetadataReturnedObjectInfos);
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
