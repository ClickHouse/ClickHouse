
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
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFilesPruning.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PositionDeleteTransform.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/StatelessMetadataFileGetter.h>
#include <Storages/ObjectStorage/Utils.h>


#include <Common/ProfileEvents.h>
#include <Common/SharedLockGuard.h>
#include <Common/logger_useful.h>

namespace DB {

using namespace Iceberg;

namespace ErrorCodes
{
extern const int ICEBERG_SPECIFICATION_VIOLATION;
}

Iceberg::ManifestFilePtr getManifestFile(
    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationWeakPtr configuration,
    IcebergMetadataFilesCachePtr iceberg_metadata_cache,
    IcebergSchemaProcessor schema_processor,
    Int32 format_version,
    String table_location,
    ContextPtr local_context, 
    LoggerPtr log,
    const String & filename, Int64 inherited_sequence_number, Int64 inherited_snapshot_id)
{
    auto configuration_ptr = configuration.lock();
    auto create_fn = [&]()
    {
        RelativePathWithMetadata manifest_object_info(filename);

        auto read_settings = local_context->getReadSettings();
        /// Do not utilize filesystem cache if more precise cache enabled
        if (iceberg_metadata_cache)
            read_settings.enable_filesystem_cache = false;

        auto buffer = createReadBuffer(manifest_object_info, object_storage, local_context, log, read_settings);
        Iceberg::AvroForIcebergDeserializer manifest_file_deserializer(std::move(buffer), filename, getFormatSettings(local_context));

        return std::make_shared<Iceberg::ManifestFileContent>(
            manifest_file_deserializer,
            filename,
            format_version,
            configuration_ptr->getPathForRead().path,
            schema_processor,
            inherited_sequence_number,
            inherited_snapshot_id,
            table_location,
            local_context);
    };

    if (iceberg_metadata_cache)
    {
        auto manifest_file
            = iceberg_metadata_cache->getOrSetManifestFile(IcebergMetadataFilesCache::getKey(configuration_ptr, filename), create_fn);
        return manifest_file;
    }
    return create_fn();
}

ManifestFileCacheKeys getManifestList(    ObjectStoragePtr object_storage,
    StorageObjectStorageConfigurationWeakPtr configuration,
    Int32 format_version,
    IcebergMetadataFilesCachePtr iceberg_metadata_cache,
    ContextPtr local_context, const String & filename)
{
    auto configuration_ptr = configuration.lock();
    if (configuration_ptr == nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");

    auto create_fn = [&]()
    {
        StorageObjectStorage::ObjectInfo object_info(filename);

        auto read_settings = local_context->getReadSettings();
        /// Do not utilize filesystem cache if more precise cache enabled
        if (iceberg_metadata_cache)
            read_settings.enable_filesystem_cache = false;

        auto manifest_list_buf = StorageObjectStorageSource::createReadBuffer(object_info, object_storage, local_context, log, read_settings);
        AvroForIcebergDeserializer manifest_list_deserializer(std::move(manifest_list_buf), filename, getFormatSettings(local_context));

        ManifestFileCacheKeys manifest_file_cache_keys;

        for (size_t i = 0; i < manifest_list_deserializer.rows(); ++i)
        {
            const std::string file_path = manifest_list_deserializer.getValueFromRowByName(i, f_manifest_path, TypeIndex::String).safeGet<std::string>();
            const auto manifest_file_name = getProperFilePathFromMetadataInfo(file_path, configuration_ptr->getPathForRead().path, table_location);
            Int64 added_sequence_number = 0;
            auto added_snapshot_id = manifest_list_deserializer.getValueFromRowByName(i, f_added_snapshot_id);
            if (added_snapshot_id.isNull())
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "Manifest list entry at index {} has null value for field '{}', but it is required",
                    i,
                    f_added_snapshot_id);

            if (format_version > 1)
            {
                added_sequence_number = manifest_list_deserializer.getValueFromRowByName(i, f_sequence_number, TypeIndex::Int64).safeGet<Int64>();
            }
            manifest_file_cache_keys.emplace_back(manifest_file_name, added_sequence_number, added_snapshot_id.safeGet<Int64>());
        }
        /// We only return the list of {file name, seq number} for cache.
        /// Because ManifestList holds a list of ManifestFilePtr which consume much memory space.
        /// ManifestFilePtr is shared pointers can be held for too much time, so we cache ManifestFile separately.
        return manifest_file_cache_keys;
    };

    ManifestFileCacheKeys manifest_file_cache_keys;
    if (iceberg_metadata_cache)
        manifest_file_cache_keys = iceberg_metadata_cache->getOrSetManifestFileCacheKeys(
            IcebergMetadataFilesCache::getKey(configuration_ptr, filename), create_fn);
    else
        manifest_file_cache_keys = create_fn();
    return manifest_file_cache_keys;
}

}
