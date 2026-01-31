
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
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/Utils.h>
#include <Interpreters/IcebergMetadataLog.h>


#include <Common/ProfileEvents.h>
#include <Common/SharedLockGuard.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

namespace Setting
{
extern const SettingsIcebergMetadataLogLevel iceberg_metadata_log_level;
}

namespace Iceberg
{
Iceberg::ManifestFilePtr getManifestFile(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    LoggerPtr log,
    const String & absolute_path,
    Int64 inherited_sequence_number,
    Int64 inherited_snapshot_id,
    SecondaryStorages & secondary_storages)
{
    auto log_level = local_context->getSettingsRef()[Setting::iceberg_metadata_log_level].value;

    bool use_iceberg_metadata_cache
        = (persistent_table_components.metadata_cache && log_level < DB::IcebergMetadataLogLevel::ManifestFileMetadata);

    auto create_fn = [&, use_iceberg_metadata_cache]()
    {
        auto [storage_to_use, resolved_key_in_storage] = resolveObjectStorageForPath(
            persistent_table_components.table_location, absolute_path, object_storage, secondary_storages, local_context);

        RelativePathWithMetadata manifest_object_info(resolved_key_in_storage);

        auto read_settings = local_context->getReadSettings();
        /// Do not utilize filesystem cache if more precise cache enabled
        if (use_iceberg_metadata_cache)
            read_settings.enable_filesystem_cache = false;

        auto buffer = createReadBuffer(manifest_object_info, storage_to_use, local_context, log, read_settings);
        Iceberg::AvroForIcebergDeserializer manifest_file_deserializer(std::move(buffer), resolved_key_in_storage, getFormatSettings(local_context));

        return std::make_shared<Iceberg::ManifestFileContent>(
            manifest_file_deserializer,
            resolved_key_in_storage,
            persistent_table_components.format_version,
            persistent_table_components.table_path,
            *persistent_table_components.schema_processor,
            inherited_sequence_number,
            inherited_snapshot_id,
            persistent_table_components.table_location,
            local_context,
            absolute_path);
    };

    if (use_iceberg_metadata_cache && persistent_table_components.table_uuid.has_value())
    {
        auto manifest_file = persistent_table_components.metadata_cache->getOrSetManifestFile(
            IcebergMetadataFilesCache::getKey(persistent_table_components.table_uuid.value(), absolute_path), create_fn);
        return manifest_file;
    }
    return create_fn();
}

ManifestFileCacheKeys getManifestList(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    const String & absolute_path,
    LoggerPtr log,
    SecondaryStorages & secondary_storages)
{
    IcebergMetadataLogLevel log_level = local_context->getSettingsRef()[Setting::iceberg_metadata_log_level].value;

    bool use_iceberg_metadata_cache
        = (persistent_table_components.metadata_cache && log_level < DB::IcebergMetadataLogLevel::ManifestListMetadata);

    auto create_fn = [&, use_iceberg_metadata_cache]()
    {
        auto [storage_to_use, key_in_storage] = resolveObjectStorageForPath(
            persistent_table_components.table_location, absolute_path, object_storage, secondary_storages, local_context);

        RelativePathWithMetadata object_info(key_in_storage);

        auto read_settings = local_context->getReadSettings();
        /// Do not utilize filesystem cache if more precise cache enabled
        if (use_iceberg_metadata_cache)
            read_settings.enable_filesystem_cache = false;

        auto manifest_list_buf = createReadBuffer(object_info, storage_to_use, local_context, log, read_settings);
        AvroForIcebergDeserializer manifest_list_deserializer(std::move(manifest_list_buf), key_in_storage, getFormatSettings(local_context));

        ManifestFileCacheKeys manifest_file_cache_keys;

            insertRowToLogTable(
            local_context,
            manifest_list_deserializer.getMetadataContent(),
            DB::IcebergMetadataLogLevel::ManifestListMetadata,
            persistent_table_components.table_path,
            key_in_storage,
            std::nullopt,
            std::nullopt);

        for (size_t i = 0; i < manifest_list_deserializer.rows(); ++i)
        {
            const std::string file_path
                = manifest_list_deserializer.getValueFromRowByName(i, f_manifest_path, TypeIndex::String).safeGet<std::string>();

            const auto manifest_absolute_path = makeAbsolutePath(persistent_table_components.table_location, file_path);
            Int64 added_sequence_number = 0;
            auto added_snapshot_id = manifest_list_deserializer.getValueFromRowByName(i, f_added_snapshot_id);
            if (added_snapshot_id.isNull())
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "Manifest list entry at index {} has null value for field '{}', but it is required",
                    i,
                    f_added_snapshot_id);

            ManifestFileContentType content_type = ManifestFileContentType::DATA;
            if (persistent_table_components.format_version > 1)
            {
                added_sequence_number
                    = manifest_list_deserializer.getValueFromRowByName(i, f_sequence_number, TypeIndex::Int64).safeGet<Int64>();
                content_type = Iceberg::ManifestFileContentType(
                    manifest_list_deserializer.getValueFromRowByName(i, f_content, TypeIndex::Int32).safeGet<Int32>());
            }
            manifest_file_cache_keys.emplace_back(
                manifest_absolute_path, added_sequence_number, added_snapshot_id.safeGet<Int64>(), content_type);

            insertRowToLogTable(
                local_context,
                manifest_list_deserializer.getContent(i),
                DB::IcebergMetadataLogLevel::ManifestListEntry,
                persistent_table_components.table_path,
                key_in_storage,
                i,
                std::nullopt);
        }
        /// We only return the list of {file name, seq number} for cache.
        /// Because ManifestList holds a list of ManifestFilePtr which consume much memory space.
        /// ManifestFilePtr is shared pointers can be held for too much time, so we cache ManifestFile separately.
        return manifest_file_cache_keys;
    };

    ManifestFileCacheKeys manifest_file_cache_keys;
    if (use_iceberg_metadata_cache && persistent_table_components.table_uuid.has_value())
        manifest_file_cache_keys = persistent_table_components.metadata_cache->getOrSetManifestFileCacheKeys(
            IcebergMetadataFilesCache::getKey(persistent_table_components.table_uuid.value(), absolute_path), create_fn);
    else
        manifest_file_cache_keys = create_fn();
    return manifest_file_cache_keys;
}

}
}

#endif
