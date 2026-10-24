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

/// Writes the metadata file with If-None-Match; false means the write was not confirmed or the path already exists.
bool writeMetadataFile(
    const IcebergPathResolver & resolver,
    const DB::GeneratedMetadataFileWithInfo & metadata_file_info,
    const std::string & metadata_file_content,
    DB::ObjectStoragePtr object_storage,
    DB::ContextPtr context);

/// Advances the hint, creating it when requested; exact mode confirms that it resolves to this metadata file.
bool tryWriteVersionHintFile(
    const IcebergPathResolver & resolver,
    const DB::GeneratedMetadataFileWithInfo & metadata_file_info,
    const IcebergPathFromMetadata & version_hint_path,
    DB::ObjectStoragePtr object_storage,
    DB::ContextPtr context,
    bool create,
    bool assert_version_exactly);

/// Existing callers keep the opportunistic hint update, including updates of an already-existing hint.
bool writeMetadataFileAndVersionHint(
    const IcebergPathResolver & resolver,
    const DB::GeneratedMetadataFileWithInfo & metadata_file_info,
    const std::string & metadata_file_content,
    const IcebergPathFromMetadata & version_hint_path,
    DB::ObjectStoragePtr object_storage,
    DB::ContextPtr context,
    bool try_write_version_hint);

/// Whether the exception being handled reports a commit whose metadata file may still be published.
/// The caller must not clean up the files that metadata references then, or a reader that still sees it
/// would find a corrupted snapshot.
bool isUnknownCommitStatus();

/// A freshly created `version-hint.text` is only useful if later commits can advance it under a
/// compare-and-swap on its `ETag`, and that header is optional. Reads the tag back through the same
/// read path later commits use and acts only when the object storage reports none while the hint
/// still holds `version`, the one this operation wrote. Otherwise the table would become
/// uncommittable on its first later write, which refuses to advance a hint without a tag.
/// With `no_concurrent_writer` (`CREATE TABLE`, whose files nobody can have built on yet), removes the
/// hint and then, once it is known to be gone, `files_to_take_back`, and throws `UNSUPPORTED_METHOD`;
/// a failed read takes the files back the same way before the error propagates.
/// Without it (a commit whose metadata file is already visible to listing-based readers, which may
/// have built on it), removes only the hint and returns normally, so the commit is kept without a
/// hint; `files_to_take_back` is not used. A failed read leaves everything in place.
void takeBackVersionHintWithoutETag(
    const DB::ObjectStoragePtr & object_storage,
    const std::string & storage_version_hint_path,
    Int32 version,
    const std::vector<std::string> & files_to_take_back,
    const DB::ContextPtr & context,
    bool no_concurrent_writer);

struct TransformAndArgument
{
    String transform_name;
    std::optional<size_t> argument;
};

std::optional<TransformAndArgument> parseTransformAndArgument(const String & transform_name_src);

/// Maps a `PARTITION BY` alias of an Iceberg date transform (e.g. `toRelativeDayNum`) to its `iceberg*` function.
String normalizeIcebergTransformFunctionName(const String & function_name);

CompressionMethod getCompressionMethodFromMetadataFile(const String & path);

Poco::JSON::Object::Ptr getMetadataJSONObject(
    const String & metadata_file_path,
    ObjectStoragePtr object_storage,
    IcebergMetadataFilesCachePtr metadata_cache,
    const ContextPtr & local_context,
    LoggerPtr log,
    CompressionMethod compression_method,
    const std::optional<String> & table_uuid);

String readMetadataJSONString(
    const String & metadata_file_path,
    ObjectStoragePtr object_storage,
    bool use_metadata_cache,
    const ContextPtr & local_context,
    LoggerPtr log,
    CompressionMethod compression_method);

Poco::JSON::Object::Ptr parseMetadataJSONString(const String & metadata_json_str);

Poco::JSON::Object::Ptr deepCopy(const Poco::JSON::Object::Ptr & object);


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

/// `ignore_metadata_pointer_overrides` distrusts the version a configured pointer names
/// (`iceberg_metadata_file_path`, `version-hint.text`) and resolves by listing; the scheme that name
/// is spelled in still counts, and a listing these callers cannot order unambiguously is refused.
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
    bool ignore_metadata_pointer_overrides = false);

String getCatalogMetadataFilePath(const std::shared_ptr<DataLake::ICatalog> & catalog, const String & table_identifier);

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
    bool ignore_metadata_pointer_overrides = true);

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

/// Iceberg stores some partition values in a form that does not directly correspond to the ClickHouse
/// type of the partition column: a decimal is an Avro `fixed` holding the two's-complement big-endian
/// unscaled value, and older ClickHouse versions wrote a `DateTime64` partition value as a plain `long`.
/// Bring such a value to the column type; any other value is returned unchanged.
/// The result of a decimal always uses `Decimal256` as its carrier, independently of the width of the
/// ClickHouse type, because `DB::Field` compares and orders on its variant tag first and Iceberg allows
/// widening `decimal(P, S)` to `decimal(P', S)` between schemas of the same table.
DB::Field normalizePartitionValue(const DB::Field & value, const DB::DataTypePtr & type);

/// `normalizePartitionValue`, followed by bringing a decimal back from the canonical carrier to the
/// carrier of `type`. For a consumer that hands the value over to code typed by the column, such as a
/// `KeyCondition` built on the partition key.
DB::Field convertPartitionValueToType(const DB::Field & value, const DB::DataTypePtr & type);

/// Apply `normalizePartitionValue` to every value of a manifest partition tuple whose transform keeps
/// the type of the source column, so that the rest of the code compares, groups and formats partition
/// values in one representation, independently of how a particular manifest encoded them.
DB::Row normalizePartitionKeyValue(
    const DB::Row & partition_key_value,
    const PartitionSpecification & partition_specification,
    const IcebergSchemaProcessor & schema_processor,
    Int32 schema_id);

using PartitionColumnValues = std::vector<std::pair<String, DB::Field>>;

PartitionColumnValues getIdentityPartitionColumnValues(
    const ProcessedManifestFileEntry & manifest_file_entry, const IcebergSchemaProcessor & schema_processor);
}

#endif
