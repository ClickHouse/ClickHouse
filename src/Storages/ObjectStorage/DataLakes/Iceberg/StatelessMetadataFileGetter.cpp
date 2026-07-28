#include "config.h"
#if USE_AVRO

#include <cstddef>
#include <memory>
#include <optional>
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
#include <Interpreters/ExpressionActions.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/DataLakes/DataLakeStorageSettings.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadataFilesCache.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

#include <Interpreters/IcebergMetadataLog.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Common/AvroForIcebergDeserializer.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/Utils.h>


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
Iceberg::ManifestFileCacheableInfo getManifestFile(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    LoggerPtr log,
    const IcebergPathFromMetadata & filename,
    size_t bytes_size)
{
    auto log_level = local_context->getSettingsRef()[Setting::iceberg_metadata_log_level].value;

    bool use_iceberg_metadata_cache
        = (persistent_table_components.metadata_cache && log_level < DB::IcebergMetadataLogLevel::ManifestFileMetadata);

    auto create_fn = [&, use_iceberg_metadata_cache]()
    {
        RelativePathWithMetadata manifest_object_info(persistent_table_components.path_resolver.resolve(filename));

        auto read_settings = local_context->getReadSettings();
        /// Do not utilize filesystem cache if more precise cache enabled
        if (use_iceberg_metadata_cache)
            read_settings.enable_filesystem_cache = false;

        auto buffer = createReadBuffer(manifest_object_info, object_storage, local_context, log, read_settings);
        auto manifest_file_deserializer = std::make_unique<Iceberg::AvroForIcebergDeserializer>(
            std::move(buffer), filename, getFormatSettings(local_context));

        return Iceberg::ManifestFileCacheableInfo{std::move(manifest_file_deserializer), bytes_size};
    };

    if (use_iceberg_metadata_cache && persistent_table_components.table_uuid.has_value())
    {
        auto manifest_file = persistent_table_components.metadata_cache->getOrSetManifestFile(
            IcebergMetadataFilesCache::getKey(persistent_table_components.table_uuid.value(), filename.serialize()), create_fn);
        return manifest_file;
    }
    return create_fn();
}

Iceberg::ManifestFileIterator::ManifestFileEntriesHandle getManifestFileEntriesHandle(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    LoggerPtr log,
    const ManifestFileCacheKey & cache_key,
    Int32 table_snapshot_schema_id)
{
    auto cacheable_info = getManifestFile(
        object_storage,
        persistent_table_components,
        local_context,
        log,
        cache_key.manifest_file_path,
        static_cast<size_t>(cache_key.manifest_file_byte_size));

    auto iterator = Iceberg::ManifestFileIterator::create(
        cacheable_info.deserializer,
        cache_key.manifest_file_path,
        persistent_table_components.format_version,
        persistent_table_components.path_resolver,
        *persistent_table_components.schema_processor,
        cache_key.added_sequence_number,
        cache_key.added_snapshot_id,
        local_context,
        nullptr,
        table_snapshot_schema_id);

    while (iterator->next())
    {
    }

    return iterator->getFilesWithoutDeletedHandle();
}

ManifestFileCacheKeys getManifestList(
    ObjectStoragePtr object_storage,
    const PersistentTableComponents & persistent_table_components,
    ContextPtr local_context,
    const IcebergPathFromMetadata & filename,
    LoggerPtr log)
{
    IcebergMetadataLogLevel log_level = local_context->getSettingsRef()[Setting::iceberg_metadata_log_level].value;

    bool use_iceberg_metadata_cache
        = (persistent_table_components.metadata_cache && log_level < DB::IcebergMetadataLogLevel::ManifestListMetadata);

    auto create_fn = [&, use_iceberg_metadata_cache]()
    {
        RelativePathWithMetadata object_info(persistent_table_components.path_resolver.resolve(filename));

        auto read_settings = local_context->getReadSettings();
        /// Do not utilize filesystem cache if more precise cache enabled
        if (use_iceberg_metadata_cache)
            read_settings.enable_filesystem_cache = false;

        auto manifest_list_buf = createReadBuffer(object_info, object_storage, local_context, log, read_settings);
        AvroForIcebergDeserializer manifest_list_deserializer(std::move(manifest_list_buf), filename, getFormatSettings(local_context));

        ManifestFileCacheKeys manifest_file_cache_keys;

        insertRowToLogTable(
            local_context,
            manifest_list_deserializer.getMetadataContent(),
            DB::IcebergMetadataLogLevel::ManifestListMetadata,
            persistent_table_components.path_resolver.getTableRoot(),
            filename,
            std::nullopt,
            std::nullopt);

        for (size_t i = 0; i < manifest_list_deserializer.rows(); ++i)
        {
            const IcebergPathFromMetadata manifest_file_name = IcebergPathFromMetadata::deserialize(
                manifest_list_deserializer.getValueFromRowByName(i, f_manifest_path, TypeIndex::String).safeGet<std::string>());
            Int64 added_sequence_number = 0;
            auto added_snapshot_id = manifest_list_deserializer.getValueFromRowByName(i, f_added_snapshot_id);
            if (added_snapshot_id.isNull())
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "Manifest list entry at index {} has null value for field '{}', but it is required",
                    i,
                    f_added_snapshot_id);

            ManifestFileContentType content_type = ManifestFileContentType::DATA;
            Int64 manifest_length
                = manifest_list_deserializer.getValueFromRowByName(i, f_manifest_length, TypeIndex::Int64).safeGet<Int64>();
            if (manifest_length < 0)
            {
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "Manifest list entry at index {} has negative value for field '{}', but it is required",
                    i,
                    f_manifest_length);
            }
            if (persistent_table_components.format_version > 1)
            {
                added_sequence_number
                    = manifest_list_deserializer.getValueFromRowByName(i, f_sequence_number, TypeIndex::Int64).safeGet<Int64>();
                content_type = Iceberg::ManifestFileContentType(
                    manifest_list_deserializer.getValueFromRowByName(i, f_content, TypeIndex::Int32).safeGet<Int32>());
            }
            manifest_file_cache_keys.emplace_back(
                manifest_file_name, manifest_length, added_sequence_number, added_snapshot_id.safeGet<Int64>(), content_type);

            insertRowToLogTable(
                local_context,
                manifest_list_deserializer.getContent(i),
                DB::IcebergMetadataLogLevel::ManifestListEntry,
                persistent_table_components.path_resolver.getTableRoot(),
                filename,
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
            IcebergMetadataFilesCache::getKey(persistent_table_components.table_uuid.value(), filename.serialize()), create_fn);
    else
        manifest_file_cache_keys = create_fn();
    return manifest_file_cache_keys;
}

}
}

#endif
