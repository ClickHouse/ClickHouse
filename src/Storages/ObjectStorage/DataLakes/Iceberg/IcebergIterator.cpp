
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
#include <Disks/ObjectStorages/StoredObject.h>
#include <Formats/FormatFactory.h>
#include <IO/ReadBufferFromFileBase.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/Context.h>

#include <IO/CompressedReadBufferWrapper.h>
#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/Common.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergIterator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>

#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>

#include <Common/SharedLockGuard.h>
#include <Common/logger_useful.h>

namespace ProfileEvents
{
extern const Event IcebergPartitionPrunedFiles;
extern const Event IcebergMinMaxIndexPrunedFiles;
};


namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}
namespace Setting
{
extern const SettingsBool use_roaring_bitmap_iceberg_positional_deletes;
extern const SettingsBool use_iceberg_partition_pruning;
};


using namespace Iceberg;

std::optional<ManifestFileEntry> SingleThreadIcebergKeysIterator::next()
{
    if (!data_snapshot)
    {
        return std::nullopt;
    }

    while (manifest_file_index < data_snapshot->manifest_list_entries.size())
    {
        if (!current_manifest_file_content)
        {
            if (data_snapshot->manifest_list_entries[manifest_file_index].content_type != manifest_file_content_type)
            {
                ++manifest_file_index;
                continue;
            }
            current_manifest_file_content = Iceberg::getManifestFile(
                object_storage,
                configuration.lock(),
                iceberg_metadata_cache,
                schema_processor,
                format_version,
                table_location,
                local_context,
                log,
                data_snapshot->manifest_list_entries[manifest_file_index].manifest_file_path,
                data_snapshot->manifest_list_entries[manifest_file_index].added_sequence_number,
                data_snapshot->manifest_list_entries[manifest_file_index].added_snapshot_id);
            internal_data_index = 0;
        }
        while (internal_data_index < current_manifest_file_content->getFilesWithoutDeleted(content_type).size())
        {
            const auto & manifest_file_entry = current_manifest_file_content->getFilesWithoutDeleted(content_type)[internal_data_index++];
            if ((manifest_file_entry.schema_id != previous_entry_schema) && (use_partition_pruning))
            {
                previous_entry_schema = manifest_file_entry.schema_id;
                if (previous_entry_schema > manifest_file_entry.schema_id)
                {
                    LOG_WARNING(
                        log,
                        "Manifest entries in file {} are not sorted by schema id",
                        current_manifest_file_content->getPathToManifestFile());
                }
                current_pruner.emplace(
                    *schema_processor,
                    table_snapshot->schema_id,
                    manifest_file_entry.schema_id,
                    filter_dag.get(),
                    *current_manifest_file_content,
                    local_context);
            }
            auto pruning_status = current_pruner ? current_pruner->canBePruned(manifest_file_entry) : PruningReturnStatus::NOT_PRUNED;
            switch (pruning_status)
            {
                case PruningReturnStatus::NOT_PRUNED:
                    return manifest_file_entry;
                case PruningReturnStatus::MIN_MAX_INDEX_PRUNED: {
                    ++min_max_index_pruned_files;
                    break;
                }
                case PruningReturnStatus::PARTITION_PRUNED: {
                    ++partition_pruned_files;
                    break;
                }
            }
        }
        current_manifest_file_content = nullptr;
        current_pruner = std::nullopt;
        ++manifest_file_index;
        internal_data_index = 0;
        previous_entry_schema = -1;
    }

    return std::nullopt;
}

SingleThreadIcebergKeysIterator::~SingleThreadIcebergKeysIterator()
{
    if (partition_pruned_files > 0)
        ProfileEvents::increment(ProfileEvents::IcebergPartitionPrunedFiles, partition_pruned_files);
    if (min_max_index_pruned_files > 0)
        ProfileEvents::increment(ProfileEvents::IcebergMinMaxIndexPrunedFiles, min_max_index_pruned_files);
}

SingleThreadIcebergKeysIterator::SingleThreadIcebergKeysIterator(
    ObjectStoragePtr object_storage_,
    ContextPtr local_context_,
    Iceberg::FileContentType content_type_,
    StorageObjectStorageConfigurationWeakPtr configuration_,
    const ActionsDAG * filter_dag_,
    Iceberg::IcebergTableStateSnapshotPtr table_snapshot_,
    Iceberg::IcebergDataSnapshotPtr data_snapshot_,
    IcebergMetadataFilesCachePtr iceberg_metadata_cache_,
    IcebergSchemaProcessorPtr schema_processor_,
    Int32 format_version_,
    String table_location_)
    : object_storage(object_storage_)
    , filter_dag(filter_dag_ ? std::make_shared<ActionsDAG>(filter_dag_->clone()) : nullptr)
    , local_context(local_context_)
    , table_snapshot(table_snapshot_)
    , data_snapshot(data_snapshot_)
    , iceberg_metadata_cache(iceberg_metadata_cache_)
    , schema_processor(schema_processor_)
    , configuration(std::move(configuration_))
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
    , format_version(format_version_)
    , table_location(std::move(table_location_))
    , log(getLogger("IcebergIterator"))
    , content_type(content_type_)
    , manifest_file_content_type(
          content_type_ == Iceberg::FileContentType::DATA ? Iceberg::ManifestFileContentType::DATA
                                                          : Iceberg::ManifestFileContentType::DELETE)
{
}

ObjectInfoPtr IcebergIterator::next(size_t)
{
    Iceberg::ManifestFileEntry manifest_file_entry;
    if (blocking_queue.pop(manifest_file_entry))
    {
        return std::make_shared<IcebergDataObjectInfo>(manifest_file_entry, position_deletes_files, format);
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
    producer_task->deactivate();
}

std::shared_ptr<ISimpleTransform> IcebergIterator::getPositionDeleteTransformer(
    const ObjectInfoPtr & object_info,
    const SharedHeader & header,
    const std::optional<FormatSettings> & format_settings,
    ContextPtr context_) const
{
    auto iceberg_object_info = std::dynamic_pointer_cast<IcebergDataObjectInfo>(object_info);
    if (!iceberg_object_info)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The object with path '{}' info is not IcebergDataObjectInfo", object_info->getPath());

    if (!context_->getSettingsRef()[Setting::use_roaring_bitmap_iceberg_positional_deletes].value)
        return std::make_shared<IcebergStreamingPositionDeleteTransform>(
            header, iceberg_object_info, object_storage, format_settings, context_, format, compression_method, position_deletes_files);
    else
        return std::make_shared<IcebergBitmapPositionDeleteTransform>(
            header, iceberg_object_info, object_storage, format_settings, context_, format, compression_method, position_deletes_files);
}
}

#endif
