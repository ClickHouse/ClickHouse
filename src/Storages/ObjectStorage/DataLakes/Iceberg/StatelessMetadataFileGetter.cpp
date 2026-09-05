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
#include <Common/FailPoint.h>


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

namespace FailPoints
{
extern const char iceberg_slow_manifest_read[];
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
    const IcebergPathFromMetadata & filename)
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

        // Test-only: simulate per-object latency.
        fiu_do_on(FailPoints::iceberg_slow_manifest_read,
        {
            std::this_thread::sleep_for(std::chrono::milliseconds(400));
        });

        auto buffer = createReadBuffer(manifest_object_info, object_storage, local_context, log, read_settings);
        auto manifest_file_deserializer = std::make_unique<Iceberg::AvroForIcebergDeserializer>(
            std::move(buffer), filename, getFormatSettings(local_context));

        const size_t manifest_file_bytes = manifest_file_deserializer->bytesRead();
        return Iceberg::ManifestFileCacheableInfo{std::move(manifest_file_deserializer), manifest_file_bytes};
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
        cache_key.manifest_file_path);

    auto iterator = Iceberg::ManifestFileIterator::create(
        cacheable_info.deserializer,
        cache_key.manifest_file_path,
        persistent_table_components.path_resolver,
        *persistent_table_components.schema_processor,
        cache_key.added_sequence_number,
        cache_key.added_snapshot_id,
        /// Stateless path passes `nullptr` as the filter, so the manifest prune cache is disabled
        /// and `table_snapshot_id` is unused. `added_snapshot_id` is passed as a placeholder.
        cache_key.added_snapshot_id,
        cache_key.first_row_id,
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

        /// The manifest list's own Avro metadata governs how it is parsed. A table whose
        /// `format-version` was upgraded from v1 to v2 by an external tool (e.g. Spark) may
        /// still reference v1 manifest lists, and those do not carry the v2-only
        /// `sequence_number`/`content` columns.
        const Int64 manifest_list_format_version = manifest_list_deserializer.getFormatVersionFromManifestFileMetadata();

        ManifestFileCacheKeys manifest_file_cache_keys;

        insertRowToLogTable(
            local_context,
            [&] { return manifest_list_deserializer.getMetadataContent(); },
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
            if (manifest_list_format_version > 1 && manifest_list_deserializer.hasPath(f_sequence_number))
                added_sequence_number
                    = manifest_list_deserializer.getValueFromRowByName(i, f_sequence_number, TypeIndex::Int64).safeGet<Int64>();
            if (manifest_list_format_version > 1 && manifest_list_deserializer.hasPath(f_content))
            {
                /// The value comes from the file: casting an arbitrary integer to the enum and
                /// comparing it with the enumerators below would be undefined behaviour, and an
                /// out-of-range value would be silently treated as a data manifest.
                const auto content_type_value
                    = manifest_list_deserializer.getValueFromRowByName(i, f_content, TypeIndex::Int32).safeGet<Int32>();
                if (content_type_value < Int32(Iceberg::ManifestFileContentType::DATA)
                    || content_type_value > Int32(Iceberg::ManifestFileContentType::DELETE))
                    throw Exception(
                        ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                        "Manifest list entry at index {} has an unexpected value {} of the field '{}'",
                        i,
                        content_type_value,
                        f_content);
                content_type = Iceberg::ManifestFileContentType(content_type_value);
            }
            if (!manifest_list_deserializer.hasPath(f_partition_spec_id))
                throw Exception(
                    ErrorCodes::ICEBERG_SPECIFICATION_VIOLATION,
                    "Manifest list entry at index {} is missing required field '{}'",
                    i,
                    f_partition_spec_id);
            Int32 partition_spec_id = static_cast<Int32>(
                manifest_list_deserializer.getValueFromRowByName(i, f_partition_spec_id, TypeIndex::Int32).safeGet<Int32>());

            std::optional<UInt64> first_row_id;
            if (manifest_list_format_version > 2 && manifest_list_deserializer.hasPath(f_manifest_first_row_id))
            {
                auto first_row_id_value = manifest_list_deserializer.getValueFromRowByName(i, f_manifest_first_row_id);
                if (!first_row_id_value.isNull())
                    first_row_id = first_row_id_value.safeGet<Int64>();
            }

            manifest_file_cache_keys.emplace_back(
                manifest_file_name, manifest_length, added_sequence_number, added_snapshot_id.safeGet<Int64>(), content_type,
                partition_spec_id, first_row_id);

            insertRowToLogTable(
                local_context,
                [&] { return manifest_list_deserializer.getContent(i); },
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
