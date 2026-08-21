#pragma once

#include <string>
#include <string_view>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

#include <Columns/IColumn.h>
#include <Core/SortDescription.h>
#include <Storages/KeyDescription.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#if USE_AVRO

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/CompressedReadBufferWrapper.h>
#include <IO/CompressionMethod.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>
#include <Storages/ObjectStorage/StorageObjectStorageSource.h>

namespace DB::Iceberg
{

void writeMessageToFile(
    const String & data,
    const String & filename,
    DB::ObjectStoragePtr object_storage,
    DB::ContextPtr context,
    const std::string & write_if_none_match,
    const std::string & write_if_match = "",
    DB::CompressionMethod compression_method = DB::CompressionMethod::None);

/// Tries to write metadata file and version hint file. Uses If-None-Match header to avoid overwriting existing files.
/// Maybe return false if failed to write metadata.json
/// Will try to write hint multiple times, but will not report failure to write hint.
bool writeMetadataFileAndVersionHint(
    const std::string & metadata_file_path,
    const std::string & metadata_file_content,
    const std::string & version_hint_path,
    std::string version_hint_content,
    DB::ObjectStoragePtr object_storage,
    DB::ContextPtr context,
    DB::CompressionMethod compression_method,
    bool try_write_version_hint
);

std::string getProperFilePathFromMetadataInfo(std::string_view data_path, std::string_view common_path, std::string_view table_location);

struct TransformAndArgument
{
    String transform_name;
    std::optional<size_t> argument;
};

std::optional<TransformAndArgument> parseTransformAndArgument(const String & transform_name_src);

Poco::JSON::Object::Ptr getMetadataJSONObject(
    const String & metadata_file_path,
    ObjectStoragePtr object_storage,
    IcebergMetadataFilesCachePtr metadata_cache,
    const ContextPtr & local_context,
    LoggerPtr log,
    CompressionMethod compression_method,
    const std::optional<String> & table_uuid);

struct MetadataFileWithInfo
{
    Int32 version;
    String path;
    CompressionMethod compression_method;
};

std::pair<Poco::Dynamic::Var, bool> getIcebergType(DataTypePtr type, Int32 & iter);
Poco::Dynamic::Var getAvroType(DataTypePtr type);

/// Spec: https://iceberg.apache.org/spec/?h=metadata.json#table-metadata-fields
std::pair<Poco::JSON::Object::Ptr, String> createEmptyMetadataFile(
    String path_location,
    const ColumnsDescription & columns,
    ASTPtr partition_by,
    ASTPtr order_by,
    ContextPtr context,
    UInt64 format_version = 2);

MetadataFileWithInfo getLatestOrExplicitMetadataFileAndVersion(
    const ObjectStoragePtr & object_storage,
    const String & table_path,
    const DataLakeStorageSettings & data_lake_settings,
    IcebergMetadataFilesCachePtr metadata_cache,
    const ContextPtr & local_context,
    Poco::Logger * log,
    const std::optional<String> & table_uuid);

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV1Method(const Poco::JSON::Object::Ptr & metadata_object);
std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV2Method(const Poco::JSON::Object::Ptr & metadata_object);
std::string normalizeUuid(const std::string & uuid);

DataTypePtr getFunctionResultType(const String & iceberg_transform_name, DataTypePtr source_type);

KeyDescription getSortingKeyDescriptionFromMetadata(
    Poco::JSON::Object::Ptr metadata_object, const NamesAndTypesList & ch_schema, ContextPtr local_context);
void sortBlockByKeyDescription(Block & block, const KeyDescription & sort_description, ContextPtr context);
}

#endif
