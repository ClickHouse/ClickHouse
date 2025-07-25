#include <filesystem>
#include <Poco/Logger.h>
#include "Common/logger_useful.h"
#include "config.h"

#if USE_AVRO

#    include <cstddef>
#    include <memory>
#    include <unordered_map>
#    include <utility>
#    include <vector>
#    include <IO/ReadHelpers.h>
#    include <Storages/ObjectStorage/DataLakes/Paimon/Constant.h>
#    include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#    include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#    include <Common/Exception.h>
#    include <Common/assert_cast.h>
#    include <Core/NamesAndTypes.h>
#    include <Disks/IStoragePolicy.h>
#    include <IO/WriteHelpers.h>
#    include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#    include <Storages/ObjectStorage/DataLakes/Paimon/PaimonMetadata.h>
#    include <Storages/ObjectStorage/IObjectIterator.h>
#    include <base/defines.h>

#    include <Columns/ColumnString.h>
#    include <Columns/ColumnTuple.h>
#    include <Columns/ColumnsNumber.h>
#    include <Columns/IColumn.h>
#    include <DataTypes/DataTypeTuple.h>
#    include <Formats/FormatFactory.h>
#    include <Storages/ObjectStorage/DataLakes/Iceberg/Utils.h>
#    include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#    include <fmt/format.h>
#    include <Types.hh>


namespace DB
{
using namespace Paimon;
namespace ErrorCodes
{
extern const int FILE_DOESNT_EXIST;
extern const int ILLEGAL_COLUMN;
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

DataLakeMetadataPtr PaimonMetadata::create(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & local_context)
{
    auto configuration_ptr = configuration.lock();
    LOG_DEBUG(
        &Poco::Logger::get("PaimonMetadata"), "path: {} full path: {}", configuration_ptr->getPath(), configuration_ptr->getFullPath());
    PaimonTableClientPtr table_client_ptr = std::make_shared<PaimonTableClient>(object_storage, configuration, local_context);
    auto schema_json = table_client_ptr->getTableSchemaJson(table_client_ptr->getLastTableSchemaInfo());
    return std::make_unique<PaimonMetadata>(object_storage, configuration_ptr, local_context, schema_json, table_client_ptr);
}

bool PaimonMetadata::updateState()
{
    /// update schema
    if (!table_schema.has_value())
    {
        table_schema = PaimonTableSchema(last_metadata_object);
    }
    else
    {
        table_schema->update(last_metadata_object);
    }
    if (!table_schema.has_value())
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot parse paimon table schema");
    }
    checkSupportCofiguration();
    /// init snapshot, now only support latest snapshot
    auto snapshot_meta_info = table_client_ptr->getLastTableSnapshotInfo();
    if (snapshot.has_value() && snapshot_meta_info.first != snapshot->version)
    {
        return false;
    }
    snapshot = table_client_ptr->getSnapshot(snapshot_meta_info);
    /// init manifest by snapshot
    std::vector<PaimonManifestFileMeta> base_manifest_list = table_client_ptr->getManifestMeta(snapshot->base_manifest_list);
    std::vector<PaimonManifestFileMeta> delta_manifest_list = table_client_ptr->getManifestMeta(snapshot->delta_manifest_list);
    for (const auto & manifest_meta : base_manifest_list)
    {
        base_manifest.emplace_back(table_client_ptr->getDataManifest(manifest_meta.file_name, *table_schema, PARTITION_DEFAULT_NAME));
    }
    for (const auto & manifest_meta : delta_manifest_list)
    {
        delta_manifest.emplace_back(table_client_ptr->getDataManifest(manifest_meta.file_name, *table_schema, PARTITION_DEFAULT_NAME));
    }
    return true;
}

void PaimonMetadata::checkSupportCofiguration()
{
    chassert(table_schema.has_value());
    auto it = table_schema->options.find(PAIMON_SCAN_MODE);
    if (it != table_schema->options.end() && (it->second != "latest" || it->second != "latest-full" || it->second != "default"))
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "mode {} is unsupported.", it->second);
    }
}

PaimonMetadata::PaimonMetadata(
    ObjectStoragePtr object_storage_,
    StorageObjectStorageConfigurationWeakPtr configuration_,
    const DB::ContextPtr & context_,
    const Poco::JSON::Object::Ptr & schema_json_object_,
    PaimonTableClientPtr table_client_ptr_)
    : WithContext(context_)
    , object_storage(std::move(object_storage_))
    , configuration(std::move(configuration_))
    , log(getLogger("PaimonMetadata"))
    , table_client_ptr(table_client_ptr_)
    , last_metadata_object(schema_json_object_)
{
    updateState();
}

NamesAndTypesList PaimonMetadata::getTableSchema() const
{
    NamesAndTypesList names_types_list;
    if (!table_schema.has_value())
        return names_types_list;

    for (const auto & field : table_schema->fields)
    {
        names_types_list.emplace_back(field.name, field.type.clickhouse_data_type);
    }
    return names_types_list;
}

bool PaimonMetadata::update(const ContextPtr &)
{
    const auto schema_meta_info = table_client_ptr->getLastTableSchemaInfo();
    if (!table_schema.has_value() || schema_meta_info.first != table_schema->version)
    {
        last_metadata_object = table_client_ptr->getTableSchemaJson(schema_meta_info);
    }
    return updateState();
}

ObjectIterator PaimonMetadata::iterate(
    const ActionsDAG * /* filter_dag */, FileProgressCallback callback, size_t /* list_batch_size */, ContextPtr /* context */) const
{
    auto configuration_ptr = configuration.lock();
    Strings data_files;
    for (const auto & entry : base_manifest)
    {
        for (const auto & file_entry : entry.entries)
        {
            if (file_entry.kind != PaimonManifestEntry::Kind::DELETE)
            {
                LOG_DEBUG(&Poco::Logger::get("PaimonMetadata"), "data file: {}", file_entry.file.bucket_path);
                data_files.emplace_back(std::filesystem::path(configuration_ptr->getPath()) / file_entry.file.file_name);
            }
        }
    }

    for (const auto & entry : delta_manifest)
    {
        for (const auto & file_entry : entry.entries)
        {
            if (file_entry.kind != PaimonManifestEntry::Kind::DELETE)
            {
                LOG_DEBUG(
                    &Poco::Logger::get("PaimonMetadata"),
                    "data file: {}",
                    std::filesystem::path(file_entry.file.bucket_path) / file_entry.file.file_name);
                data_files.emplace_back(
                    std::filesystem::path(configuration_ptr->getPath()) / file_entry.file.bucket_path / file_entry.file.file_name);
            }
        }
    }
    return createKeysIterator(std::move(data_files), object_storage, callback);
}

}
#endif
