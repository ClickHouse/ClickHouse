#pragma once

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

#include <Interpreters/Context_fwd.h>
#include <Core/Types.h>
#include <Disks/ObjectStorages/IObjectStorage.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>

#    include <Poco/JSON/Array.h>
#    include <Poco/JSON/Object.h>
#    include <Poco/JSON/Parser.h>

namespace DB
{


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

    struct SimpleTypeRepresentation
    {
        std::optional<String> name;
        bool required;
        String type;

        // static std::strong_ordering operator<=>(const optional<String> & that, const optional<String> & other) const
        // {
        //     if (that.has_value() && other.has_value())
        //     {
        //         return that.value() <=> other.value();
        //     }
        //     else
        //     {
        //         if (other.has_value())
        //         {
        //             return -1;
        //         }
        //         else if (that.has_value())
        //         {
        //             return 1;
        //         }
        //         return 0;
        //     }
        // }

        bool operator!=(const SimpleTypeRepresentation & other) const
        {
            return std::tie(name, required, type) != std::tie(other.name, other.required, other.type);
        }
    };

public:
    void addIcebergTableSchema(Poco::JSON::Object::Ptr ptr);
    std::shared_ptr<NamesAndTypesList> getClickhouseTableSchemaById(Int32 id);
    std::shared_ptr<const ActionsDAG> getSchemaTransformationDagByIds(Int32 old_id, Int32 new_id);
    std::optional<NameAndTypePair> getSimpleNameAndTypeByVersion(Int32 field_id, std::optional<Int32> schema_id = std::nullopt);

private:
    std::map<Int32, Poco::JSON::Object::Ptr> iceberg_table_schemas_by_ids;
    std::map<Int32, std::shared_ptr<NamesAndTypesList>> clickhouse_table_schemas_by_ids;
    std::map<std::pair<Int32, Int32>, std::shared_ptr<ActionsDAG>> transform_dags_by_ids;
    std::map<Int32, std::vector<std::pair<Int32, std::optional<SimpleTypeRepresentation>>>> simple_type_by_field_id;
    std::map<Int32, Int32> parents;

    NamesAndTypesList getSchemaType(const Poco::JSON::Object::Ptr & schema);
    DataTypePtr getComplexTypeFromObject(const Poco::JSON::Object::Ptr & type, Int32 parent_id);
    DataTypePtr
    getFieldType(const Poco::JSON::Object::Ptr & field, const String & type_key, const String & id_key, bool required, Int32 parent_id);
    DataTypePtr getSimpleType(const String & type_name);
    std::shared_ptr<ActionsDAG> getSchemaTransformationDag(
        [[maybe_unused]] const Poco::JSON::Object::Ptr & old_schema, [[maybe_unused]] const Poco::JSON::Object::Ptr & new_schema);
    void refresh_parent_info(Int32 parent_id, Int32 current_id);

    bool allowPrimitiveTypeConversion(const String & old_type, const String & new_type);

    const Node * getDefaultNodeForField(const Poco::JSON::Object::Ptr & field);

    std::optional<Int32> current_old_id;
    std::optional<Int32> current_new_id;

    std::optional<Int32> last_schema_id;
    std::optional<Int32> current_schema_id;

    // std::pair<const Node *, const Node *>
    // getRemappingForStructField(const Poco::JSON::Array::Ptr & old_node, const Poco::JSON::Array::Ptr & new_node, const Node * input_node);
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
    enum class PartitionTransform
    {
        Year,
        Month,
        Day,
        Hour,
        Identity,
        Unsupported
    };

    static PartitionTransform getTransform(const String & transform_name)
    {
        if (transform_name == "year")
        {
            return PartitionTransform::Year;
        }
        // else if (transform_name == "month")
        // {
        //     return PartitionTransform::Month;
        // }
        // else if (transform_name == "day")
        // {
        //     return PartitionTransform::Day;
        // }
        // else if (transform_name == "hour")
        // {
        //     return PartitionTransform::Hour;
        // }
        // else if (transform_name == "identity")
        // {
        //     return PartitionTransform::Identity;
        // }
        else
        {
            return PartitionTransform::Unsupported;
        }
    }

    using ConfigurationPtr = StorageObjectStorage::ConfigurationPtr;

    static constexpr auto name = "Iceberg";

    IcebergMetadata(
        ObjectStoragePtr object_storage_,
        ConfigurationPtr configuration_,
        ContextPtr context_,
        Int32 metadata_version_,
        Int32 format_version_,
        String manifest_list_file_,
        Int32 current_schema_id_,
        IcebergSchemaProcessor schema_processor);

    /// Get data files. On first request it reads manifest_list file and iterates through manifest files to find all data files.
    /// All subsequent calls will return saved list of files (because it cannot be changed without changing metadata file)
    DataFileInfos getDataFileInfos(const ActionsDAG * filter_dag) const override;

    /// Get table schema parsed from metadata.
    NamesAndTypesList getTableSchema() const override { return schema; }

    const std::unordered_map<String, String> & getColumnNameToPhysicalNameMapping() const override { return column_name_to_physical_name; }

    const DataLakePartitionColumns & getPartitionColumns() const override { return fake_partition_columns; }

    bool operator ==(const IDataLakeMetadata & other) const override
    {
        const auto * iceberg_metadata = dynamic_cast<const IcebergMetadata *>(&other);
        return iceberg_metadata && getVersion() == iceberg_metadata->getVersion();
    }

    static DataLakeMetadataPtr create(
        ObjectStoragePtr object_storage,
        ConfigurationPtr configuration,
        ContextPtr local_context);

    size_t getVersion() const { return metadata_version; }

private:
    const ObjectStoragePtr object_storage;
    const ConfigurationPtr configuration;
    Int32 metadata_version;
    Int32 format_version;
    String manifest_list_file;
    const Int32 current_schema_id;
    // mutable DataFileInfos data_file_infos;
    std::unordered_map<String, String> column_name_to_physical_name;
    mutable IcebergSchemaProcessor schema_processor;
    NamesAndTypesList schema;
    LoggerPtr log;
    const DataLakePartitionColumns fake_partition_columns{};
};

}

#endif
