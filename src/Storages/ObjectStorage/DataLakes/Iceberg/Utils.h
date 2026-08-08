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
    const std::optional<String> & table_uuid,
    const String & data_source_description,
    String & raw_json_out);

Poco::JSON::Object::Ptr getMetadataJSONObject(
    const String & metadata_file_path,
    ObjectStoragePtr object_storage,
    IcebergMetadataFilesCachePtr metadata_cache,
    const ContextPtr & local_context,
    LoggerPtr log,
    CompressionMethod compression_method,
    const std::optional<String> & table_uuid,
    const String & data_source_description);


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
    const std::optional<String> & table_uuid,
    const String & data_source_description,
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
    const String & data_source_description,
    CompressionMethod known_compression_method,
    bool ignore_explicit_metadata_file_path = true);

std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV1Method(const Poco::JSON::Object::Ptr & metadata_object);
std::pair<Poco::JSON::Object::Ptr, Int32> parseTableSchemaV2Method(const Poco::JSON::Object::Ptr & metadata_object);
std::string normalizeUuid(const std::string & uuid);

/// Whether `cached_location` (the Iceberg `location` field of a cached metadata JSON) refers to
/// this table, identified by its storage `table_namespace` (bucket/container, e.g. from
/// `IObjectStorageConfiguration::getNamespace`), `table_root` (the storage engine's configured key
/// path, e.g. from `getPathForRead().path`), and `table_backend_type` (e.g. from
/// `IObjectStorageConfiguration::getTypeName`, used to reject cross-backend collisions such as an
/// Azure `wasb://` location matching an S3 bucket of the same name; equivalences mirror
/// `DataLake::parseStorageTypeFromString`: `file` -> local, `s3a`/`gs`/`oss` -> s3, `abfss` -> azure).
/// A schemeless `cached_location` -- as ClickHouse itself writes by default
/// (`write_full_path_in_iceberg_metadata = 0`), regardless of backend -- carries no authority to
/// validate, so it is only accepted when `table_namespace` is itself empty (nothing to validate
/// against); when `table_namespace` is non-empty it is treated as unverifiable and rejected, since
/// two different tables in different buckets/containers could otherwise produce the same
/// schemeless location for the same key path. Scheme-bearing URIs are handled, including the
/// authority-bearing form used by Spark/Azure
/// (`wasb://container@account.blob.core.windows.net/...`) or HDFS
/// (`hdfs://namenode:8020/...`, `hdfs://user@nameservice/...`). The key-path comparison is always
/// exact, not a suffix match, so a same-named key in a different bucket/container is correctly
/// rejected. When `table_namespace` is empty (namespace-less backends), any authority is accepted
/// since there is nothing to validate it against -- but the backend family must still match.
bool cachedLocationMatchesTableRoot(
    std::string_view cached_location, std::string_view table_namespace, std::string_view table_root, std::string_view table_backend_type);

/// Derives the namespace/authority to validate against in `cachedLocationMatchesTableRoot`.
/// Returns `configuration_namespace` (e.g. `IObjectStorageConfiguration::getNamespace`) as-is when
/// non-empty. Otherwise (namespace-less backends like HDFS, where `getNamespace` is always empty
/// even though the table identity still includes the namenode/nameservice) falls back to the
/// authority component of `configuration_raw_uri` (e.g. `getRawURI`), so two different HDFS
/// clusters sharing the same key path are not treated as the same table. Backends with no scheme
/// in their raw URI at all (e.g. Local) still yield an empty result, which is intentionally
/// permissive since there is no cluster identity to validate.
std::string deriveTableNamespaceForLocationCheck(std::string_view configuration_namespace, std::string_view configuration_raw_uri);

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
}

#endif
