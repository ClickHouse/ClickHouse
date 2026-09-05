#pragma once

#include "config.h"

#if USE_AVRO

#include <optional>
#include <string>
#include <Storages/ObjectStorage/DataLakes/Iceberg/FileNamesGenerator.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/PersistentTableComponents.h>

#include <Columns/IColumn.h>
#include <Core/SortDescription.h>
#include <Storages/KeyDescription.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/CompressedReadBufferWrapper.h>
#include <IO/CompressionMethod.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/SchemaProcessor.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/Snapshot.h>

namespace avro
{
class GenericDatum;
}

namespace DB
{
struct StorageID;
}

namespace DataLake
{
class ICatalog;
}

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
    const IcebergPathResolver & resolver,
    const DB::GeneratedMetadataFileWithInfo & metadata_file_info,
    const std::string & metadata_file_content,
    const IcebergPathFromMetadata & version_hint_path,
    DB::ObjectStoragePtr object_storage,
    DB::ContextPtr context,
    bool try_write_version_hint);

struct TransformAndArgument
{
    String transform_name;
    std::optional<size_t> argument;
};

std::optional<TransformAndArgument> parseTransformAndArgument(const String & transform_name_src);

CompressionMethod getCompressionMethodFromMetadataFile(const String & path);

Poco::JSON::Object::Ptr getMetadataJSONObject(
    const String & metadata_file_path,
    ObjectStoragePtr object_storage,
    IcebergMetadataFilesCachePtr metadata_cache,
    const ContextPtr & local_context,
    LoggerPtr log,
    CompressionMethod compression_method,
    const std::optional<String> & table_uuid);


/// The last word before publishing new metadata.
///
/// Publishing wins by version number: `getLatestOrExplicitMetadataFileAndVersion` prefers the
/// highest version, and the version-hint CAS only detects another writer of the same table racing
/// for the same version. A table dropped and recreated at the same root restarts its numbering, so
/// a statement validated for the previous table would publish over the replacement instead of
/// losing to it, and the incarnation counter alone cannot see that: it only moves when a
/// replacement is observed through `IcebergMetadata::update`. Re-read the metadata that is in
/// storage right now and refuse to publish when its own `table-uuid` describes another table.
void checkStorageStillHoldsValidatedTable(
    const PersistentTableComponents & persistent_table_components,
    const ObjectStoragePtr & object_storage,
    const DataLakeStorageSettings & data_lake_settings,
    const ContextPtr & context,
    std::optional<UInt64> validated_incarnation,
    std::string_view operation);

std::pair<Poco::Dynamic::Var, bool> getIcebergType(DataTypePtr type, Int32 & iter);
Poco::Dynamic::Var getAvroType(DataTypePtr type, Int32 field_id);

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
    const std::optional<String> & table_uuid,
    CompressionMethod known_compression_method,
    bool force_fetch_latest_metadata = true,
    bool ignore_explicit_metadata_file_path = false);

MetadataFileWithInfo getLatestMetadataFileAndVersionWithCatalog(
    const ObjectStoragePtr & object_storage,
    const std::shared_ptr<DataLake::ICatalog> & catalog,
    const String & table_identifier,
    const String & table_path,
    const DataLakeStorageSettings & data_lake_settings,
    IcebergMetadataFilesCachePtr metadata_cache,
    const ContextPtr & local_context,
    Poco::Logger * log,
    const std::optional<String> & table_uuid,
    CompressionMethod known_compression_method,
    bool ignore_explicit_metadata_file_path = true);

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV1Method(const Poco::JSON::Object::Ptr & metadata_object);
std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV2Method(const Poco::JSON::Object::Ptr & metadata_object);
std::string normalizeUuid(const std::string & uuid);

DataTypePtr getFunctionResultType(const String & iceberg_transform_name, DataTypePtr source_type);

enum class FileCategory : uint8_t
{
    DATA_FILE,
    POSITION_DELETE_FILE,
    EQUALITY_DELETE_FILE,
    MANIFEST_FILE,
    MANIFEST_LIST,
    METADATA_JSON,
    STATISTICS_FILE,
};

FileCategory inspectFileCategory(const String & relative_path);

KeyDescription getSortingKeyDescriptionFromMetadata(
    Poco::JSON::Object::Ptr metadata_object, const NamesAndTypesList & ch_schema, ContextPtr local_context);
void sortBlockByKeyDescription(Block & block, const KeyDescription & sort_description, ContextPtr context);

void forEachAvroEntry(
    const String & filename,
    ObjectStoragePtr object_storage,
    ContextPtr context,
    const String & logger_name,
    std::function<void(const avro::GenericDatum &)> callback);

using PartitionColumnValues = std::vector<std::pair<String, DB::Field>>;

PartitionColumnValues getIdentityPartitionColumnValues(
    const ProcessedManifestFileEntry & manifest_file_entry, const IcebergSchemaProcessor & schema_processor);
}

#endif
