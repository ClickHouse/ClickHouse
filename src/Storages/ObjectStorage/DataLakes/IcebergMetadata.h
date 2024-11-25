#pragma once

#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include "config.h"

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

/**
 * Iceberg supports the next data types (see https://iceberg.apache.org/spec/#schemas-and-data-types):
 * - Primitive types:
 *   - boolean
 *   - int
 *   - long
 *   - float
 *   - double
 *   - decimal(P, S)
 *   - date
 *   - time (time of day in microseconds since midnight)
 *   - timestamp (in microseconds since 1970-01-01)
 *   - timestamptz (timestamp with timezone, stores values in UTC timezone)
 *   - string
 *   - uuid
 *   - fixed(L) (fixed-length byte array of length L)
 *   - binary
 * - Complex types:
 *   - struct(field1: Type1, field2: Type2, ...) (tuple of typed values)
 *   - list(nested_type)
 *   - map(Key, Value)
 *
 * Example of table schema in metadata:
 * {
 *     "type" : "struct",
 *     "schema-id" : 0,
 *     "fields" : [
 *     {
 *         "id" : 1,
 *         "name" : "id",
 *         "required" : false,
 *         "type" : "long"
 *     },
 *     {
 *         "id" : 2,
 *         "name" : "array",
 *         "required" : false,
 *         "type" : {
 *             "type" : "list",
 *             "element-id" : 5,
 *             "element" : "int",
 *             "element-required" : false
 *     },
 *     {
 *         "id" : 3,
 *         "name" : "data",
 *         "required" : false,
 *         "type" : "binary"
 *     }
 * }
 */
class IcebergSchemaProcessor
{
    using Node = ActionsDAG::Node;

public:
    void addIcebergTableSchema(Poco::JSON::Object::Ptr schema_ptr);
    std::shared_ptr<NamesAndTypesList> getClickhouseTableSchemaById(Int32 id);
    std::shared_ptr<const ActionsDAG> getSchemaTransformationDagByIds(Int32 old_id, Int32 new_id);

private:
    std::unordered_map<Int32, Poco::JSON::Object::Ptr> iceberg_table_schemas_by_ids;
    std::unordered_map<Int32, std::shared_ptr<NamesAndTypesList>> clickhouse_table_schemas_by_ids;
    std::map<std::pair<Int32, Int32>, std::shared_ptr<ActionsDAG>> transform_dags_by_ids;

    NamesAndTypesList getSchemaType(const Poco::JSON::Object::Ptr & schema);
    DataTypePtr getComplexTypeFromObject(const Poco::JSON::Object::Ptr & type);
    DataTypePtr getFieldType(const Poco::JSON::Object::Ptr & field, const String & type_key, bool required);
    DataTypePtr getSimpleType(const String & type_name);

    bool allowPrimitiveTypeConversion(const String & old_type, const String & new_type);
    const Node * getDefaultNodeForField(const Poco::JSON::Object::Ptr & field);

    std::shared_ptr<ActionsDAG> getSchemaTransformationDag(
        const Poco::JSON::Object::Ptr & old_schema, const Poco::JSON::Object::Ptr & new_schema, Int32 old_id, Int32 new_id);

    std::mutex mutex;
};


/**
 * Useful links:
 * - https://iceberg.apache.org/spec/
 *
 * Iceberg has two format versions, v1 and v2. The content of metadata files depends on the version.
 *
 * Unlike DeltaLake, Iceberg has several metadata layers: `table metadata`, `manifest list` and `manifest_files`.
 * Metadata file - json file.
 * Manifest list – an Avro file that lists manifest files; one per snapshot.
 * Manifest file – an Avro file that lists data or delete files; a subset of a snapshot.
 * All changes to table state create a new metadata file and replace the old metadata with an atomic swap.
 *
 * In order to find out which data files to read, we need to find the `manifest list`
 * which corresponds to the latest snapshot. We find it by checking a list of snapshots
 * in metadata's "snapshots" section.
 *
 * Example of metadata.json file.
 * {
 *     "format-version" : 1,
 *     "table-uuid" : "ca2965ad-aae2-4813-8cf7-2c394e0c10f5",
 *     "location" : "/iceberg_data/db/table_name",
 *     "last-updated-ms" : 1680206743150,
 *     "last-column-id" : 2,
 *     "schema" : { "type" : "struct", "schema-id" : 0, "fields" : [ {<field1_info>}, {<field2_info>}, ... ] },
 *     "current-schema-id" : 0,
 *     "schemas" : [ ],
 *     ...
 *     "current-snapshot-id" : 2819310504515118887,
 *     "refs" : { "main" : { "snapshot-id" : 2819310504515118887, "type" : "branch" } },
 *     "snapshots" : [ {
 *       "snapshot-id" : 2819310504515118887,
 *       "timestamp-ms" : 1680206743150,
 *       "summary" : {
 *         "operation" : "append", "spark.app.id" : "local-1680206733239",
 *         "added-data-files" : "1", "added-records" : "100",
 *         "added-files-size" : "1070", "changed-partition-count" : "1",
 *         "total-records" : "100", "total-files-size" : "1070", "total-data-files" : "1", "total-delete-files" : "0",
 *         "total-position-deletes" : "0", "total-equality-deletes" : "0"
 *       },
 *       "manifest-list" : "/iceberg_data/db/table_name/metadata/snap-2819310504515118887-1-c87bfec7-d36c-4075-ad04-600b6b0f2020.avro",
 *       "schema-id" : 0
 *     } ],
 *     "statistics" : [ ],
 *     "snapshot-log" : [ ... ],
 *     "metadata-log" : [ ]
 * }
 */
class IcebergMetadata : public IDataLakeMetadata, private WithContext
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;

    static constexpr auto name = "Iceberg";

    IcebergMetadata(
        ObjectStoragePtr object_storage_,
        ConfigurationObserverPtr configuration_,
        const DB::ContextPtr & context_,
        Int32 metadata_version_,
        Int32 format_version_,
        String manifest_list_file_,
        const Poco::JSON::Object::Ptr& object);

    /// Get data files. On first request it reads manifest_list file and iterates through manifest files to find all data files.
    /// All subsequent calls will return saved list of files (because it cannot be changed without changing metadata file)
    Strings getDataFiles() const override;

    /// Get table schema parsed from metadata.
    NamesAndTypesList getTableSchema() const override { return schema; }

    const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const override { return column_name_to_physical_name; }

    const DataLakePartitionColumns & getPartitionColumns() const override { return partition_columns; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * iceberg_metadata = dynamic_cast<const IcebergMetadata *>(&other);
        return iceberg_metadata && getVersion() == iceberg_metadata->getVersion();
    }

    static DataLakeMetadataPtr
    create(const ObjectStoragePtr & object_storage, const ConfigurationObserverPtr & configuration, const ContextPtr & local_context);

    size_t getVersion() const { return metadata_version; }

    std::shared_ptr<NamesAndTypesList> getInitialSchemaByPath(const String & data_path) const override
    {
        auto version_if_outdated = getSchemaVersionByFileIfOutdated(data_path);
        return version_if_outdated.has_value() ? schema_processor.getClickhouseTableSchemaById(version_if_outdated.value()) : nullptr;
    }

    std::shared_ptr<const ActionsDAG> getSchemaTransformer(const String & data_path) const override
    {
        auto version_if_outdated = getSchemaVersionByFileIfOutdated(data_path);
        return version_if_outdated.has_value()
            ? schema_processor.getSchemaTransformationDagByIds(version_if_outdated.value(), current_schema_id)
            : nullptr;
    }

    bool supportsExternalMetadataChange() const override { return true; }

private:
    mutable std::unordered_map<String, Int32> schema_id_by_data_file;

    const ObjectStoragePtr object_storage;
    const ConfigurationObserverPtr configuration;
    Int32 metadata_version;
    Int32 format_version;
    String manifest_list_file;
    Int32 current_schema_id;
    mutable Strings data_files;
    std::unordered_map<String, String> column_name_to_physical_name;
    DataLakePartitionColumns partition_columns;
    NamesAndTypesList schema;
    mutable IcebergSchemaProcessor schema_processor;
    LoggerPtr log;

    mutable std::mutex get_data_files_mutex;

    std::optional<Int32> getSchemaVersionByFileIfOutdated(String data_path) const
    {
        auto schema_id = schema_id_by_data_file.find(data_path);
        if (schema_id == schema_id_by_data_file.end())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot find schema version for data file: {}", data_path);
        }
        if (schema_id->second == current_schema_id)
            return std::nullopt;
        return std::optional{schema_id->second};
    }
};

}

#endif
