#include "config.h"

#if USE_AVRO

#include <cstddef>
#include <memory>
#include <unordered_map>
#include <utility>
#include <vector>
#include <Core/NamesAndTypes.h>
#include <Disks/IStoragePolicy.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Constant.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonClient.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/PaimonMetadata.h>
#include <Storages/ObjectStorage/IObjectIterator.h>
#include <Storages/ObjectStorage/StorageObjectStorageSettings.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>
#include <base/defines.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/SharedLockGuard.h>
#include <Common/logger_useful.h>

#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeTuple.h>
#include <Formats/FormatFactory.h>
#include <Storages/ObjectStorage/DataLakes/Paimon/Utils.h>
#include <fmt/format.h>


namespace DB
{
using namespace Paimon;
namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
}

DataLakeMetadataPtr PaimonMetadata::create(
    const ObjectStoragePtr & object_storage,
    const StorageObjectStorageConfigurationWeakPtr & configuration,
    const ContextPtr & local_context)
{
    auto configuration_ptr = configuration.lock();
    LOG_TEST(
        &Poco::Logger::get("PaimonMetadata"),
        "path: {} raw path: {}",
        configuration_ptr->getPathForRead().path,
        configuration_ptr->getRawPath().path);
    PaimonTableClientPtr table_client_ptr = std::make_shared<PaimonTableClient>(object_storage, configuration, local_context);
    auto schema_json = table_client_ptr->getTableSchemaJSON(table_client_ptr->getLastestTableSchemaInfo());
    Int32 version = -1;
    Paimon::getValueFromJSON(version, schema_json, "version");
    if (version != 3)
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Paimon table schema version {} is unsupported.", version);
    }
    return std::make_unique<PaimonMetadata>(object_storage, configuration_ptr, local_context, schema_json, table_client_ptr);
}

void PaimonMetadata::updateState()
{
    std::lock_guard lock(mutex);
    /// update schema
    const auto schema_meta_info = table_client_ptr->getLastestTableSchemaInfo();
    if (!table_schema.has_value() || schema_meta_info.first != table_schema->id)
    {
        last_metadata_object = table_client_ptr->getTableSchemaJSON(schema_meta_info);
    }
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
        std::stringstream ss;// STYLE_CHECK_ALLOW_STD_STRING_STREAM
        Poco::JSON::Stringifier::stringify(last_metadata_object, ss);
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot parse paimon table schema, json object: {}", ss.str());
    }
    auto it = table_schema->options.find(PAIMON_SCAN_MODE);
    if (it != table_schema->options.end() && (it->second != "latest" || it->second != "latest-full" || it->second != "default"))
    {
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "mode {} is unsupported.", it->second);
    }
    /// init snapshot, now only support latest snapshot
    auto snapshot_meta_info = table_client_ptr->getLastestTableSnapshotInfo();
    if (snapshot.has_value() && snapshot_meta_info.first == snapshot->id)
    {
        return;
    }
    snapshot = table_client_ptr->getSnapshot(snapshot_meta_info);
    /// init manifest by snapshot
    std::vector<PaimonManifestFileMeta> base_manifest_list = table_client_ptr->getManifestMeta(snapshot->base_manifest_list);
    std::vector<PaimonManifestFileMeta> delta_manifest_list = table_client_ptr->getManifestMeta(snapshot->delta_manifest_list);

    auto getOrDefault = [](const std::string & key, const std::string & default_value, std::unordered_map<String, String> & options) -> std::string
    {
        auto inner_it = options.find(key);
        return inner_it != options.end() ? inner_it->second : default_value;
    };

    for (const auto & manifest_meta : base_manifest_list)
    {
        base_manifest.emplace_back(table_client_ptr->getDataManifest(manifest_meta.file_name, *table_schema, getOrDefault(PAIMON_DEFAULT_PARTITION_NAME, PARTITION_DEFAULT_VALUE, table_schema->options)));
    }
    for (const auto & manifest_meta : delta_manifest_list)
    {
        delta_manifest.emplace_back(table_client_ptr->getDataManifest(manifest_meta.file_name, *table_schema, getOrDefault(PAIMON_DEFAULT_PARTITION_NAME, PARTITION_DEFAULT_VALUE, table_schema->options)));
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

NamesAndTypesList PaimonMetadata::getTableSchema(ContextPtr /*local_context*/) const
{
    SharedLockGuard shared_lock(mutex);
    NamesAndTypesList names_types_list;
    if (!table_schema.has_value())
        return names_types_list;

    for (const auto & field : table_schema->fields)
    {
        names_types_list.emplace_back(field.name, field.type.clickhouse_data_type);
    }
    return names_types_list;
}

void PaimonMetadata::update(const ContextPtr &)
{
    updateState();
}

ObjectIterator PaimonMetadata::iterate(
    const ActionsDAG * /* filter_dag */,
    FileProgressCallback callback,
    size_t /* list_batch_size */,
    StorageMetadataPtr /*storage_metadata*/,
    ContextPtr /* context */) const
{
    SharedLockGuard shared_lock(mutex);
    auto configuration_ptr = configuration.lock();
    Strings data_files;
    for (const auto & entry : base_manifest)
    {
        for (const auto & file_entry : entry.entries)
        {
            if (file_entry.kind != PaimonManifestEntry::Kind::DELETE)
            {
                data_files.emplace_back(
                    std::filesystem::path(configuration_ptr->getPathForRead().path) / file_entry.file.bucket_path
                    / file_entry.file.file_name);
                LOG_TEST(log, "base_manifest data file: {}", data_files.back());
            }
        }
    }

    for (const auto & entry : delta_manifest)
    {
        for (const auto & file_entry : entry.entries)
        {
            if (file_entry.kind != PaimonManifestEntry::Kind::DELETE)
            {
                data_files.emplace_back(
                    std::filesystem::path(configuration_ptr->getPathForRead().path) / file_entry.file.bucket_path
                    / file_entry.file.file_name);
                LOG_TEST(log, "delta_manifest data file: {}", data_files.back());
            }
        }
    }
    return createKeysIterator(std::move(data_files), object_storage, callback);
}

}
#endif
