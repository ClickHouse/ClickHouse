#pragma once

#include "Columns/ColumnString.h"
#include "Columns/ColumnsDateTime.h"
#include "Columns/IColumn.h"
#include "Core/NamesAndTypes.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypeTuple.h"
#include "Disks/IStoragePolicy.h"
#include "base/Decimal.h"
#include <memory>
#include <mutex>
#include <optional>
#include <unordered_map>
#include "config.h"

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

#    include <Core/Types.h>
#    include <Disks/ObjectStorages/IObjectStorage.h>
#    include <Interpreters/Context_fwd.h>
#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/StorageObjectStorage.h>

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
    NameAndTypePair getFieldCharacteristics(Int32 schema_version, Int32 source_id) const;

private:
    std::unordered_map<Int32, Poco::JSON::Object::Ptr> iceberg_table_schemas_by_ids;
    std::unordered_map<Int32, std::shared_ptr<NamesAndTypesList>> clickhouse_table_schemas_by_ids;
    std::map<std::pair<Int32, Int32>, NameAndTypePair> clickhouse_types_by_source_ids;
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

enum class PartitionTransform
{
    Year,
    Month,
    Day,
    Hour,
    Identity,
    Void,
    Unsupported
};

enum class ManifestEntryStatus : uint8_t
{
    EXISTING = 0,
    ADDED = 1,
    DELETED = 2,
};

enum class DataFileContent : uint8_t
{
    DATA = 0,
    POSITION_DELETES = 1,
    EQUALITY_DELETES = 2,
};


struct CommonPartitionInfo
{
    std::vector<ColumnPtr> partition_columns;
    std::vector<PartitionTransform> partition_transforms;
    std::vector<Int32> partition_source_ids;
    ColumnPtr file_path_column;
    ColumnPtr status_column;
    String manifest_file;
};

struct SpecificSchemaPartitionInfo
{
    std::vector<std::vector<Range>> ranges;
    NamesAndTypesList partition_names_and_types;
};

class PartitionPruningProcessor
{
public:
    CommonPartitionInfo getCommonPartitionInfo(
        const Poco::JSON::Array::Ptr & partition_specification,
        const ColumnTuple * big_partition_column,
        ColumnPtr file_path_column,
        ColumnPtr status_column,
        const String & manifest_file) const;

    SpecificSchemaPartitionInfo
    getSpecificPartitionInfo(const CommonPartitionInfo & common_info, Int32 schema_version, const IcebergSchemaProcessor & processor) const;

    std::vector<bool> getPruningMask(
        const String & manifest_file,
        const SpecificSchemaPartitionInfo & specific_info,
        const ActionsDAG * filter_dag,
        ContextPtr context) const;

    Strings getDataFiles(
        const std::vector<CommonPartitionInfo> & manifest_partitions_infos,
        const std::vector<SpecificSchemaPartitionInfo> & specific_infos,
        const ActionsDAG * filter_dag,
        ContextPtr context,
        const std::string & common_path) const;

private:
    static PartitionTransform getTransform(const String & transform_name)
    {
        if (transform_name == "year")
        {
            return PartitionTransform::Year;
        }
        else if (transform_name == "month")
        {
            return PartitionTransform::Month;
        }
        else if (transform_name == "day")
        {
            return PartitionTransform::Day;
        }
        else if (transform_name == "hour")
        {
            return PartitionTransform::Hour;
        }
        else if (transform_name == "identity")
        {
            return PartitionTransform::Identity;
        }
        else if (transform_name == "void")
        {
            return PartitionTransform::Void;
        }
        else
        {
            return PartitionTransform::Unsupported;
        }
    }

    static DateLUTImpl::Values getValues(Int32 value, PartitionTransform transform)
    {
        if (transform == PartitionTransform::Year)
        {
            return DateLUT::instance().lutIndexByYearSinceEpochStartsZeroIndexing(value);
        }
        else if (transform == PartitionTransform::Month)
        {
            return DateLUT::instance().lutIndexByMonthSinceEpochStartsZeroIndexing(static_cast<UInt32>(value));
        }
        else if (transform == PartitionTransform::Day)
        {
            return DateLUT::instance().getValues(static_cast<ExtendedDayNum>(value));
        }
        else if (transform == PartitionTransform::Hour)
        {
            DateLUTImpl::Values values = DateLUT::instance().getValues(static_cast<ExtendedDayNum>(value / 24));
            values.date += (value % 24) * 3600;
            return values;
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition transform for get day function: {}", transform);
    }

    static Int64 getTime(Int32 value, PartitionTransform transform)
    {
        DateLUTImpl::Values values = getValues(value, transform);
        return values.date;
    }

    static Int16 getDay(Int32 value, PartitionTransform transform)
    {
        DateLUTImpl::Time got_time = getTime(value, transform);
        // LOG_DEBUG(&Poco::Logger::get("Get field"), "Time: {}", got_time);
        return DateLUT::instance().toDayNum(got_time);
    }

    static Range
    getPartitionRange(PartitionTransform partition_transform, UInt32 index, ColumnPtr partition_column, DataTypePtr column_data_type)
    {
        if (partition_transform == PartitionTransform::Unsupported)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition transform: {}", partition_transform);
        }
        auto column = dynamic_cast<const ColumnNullable *>(partition_column.get())->getNestedColumnPtr();
        if (partition_transform == PartitionTransform::Identity)
        {
            Field entry = (*column.get())[index];
            return Range{entry, true, entry, true};
        }
        auto [nested_data_type, value] = [&]() -> std::pair<DataTypePtr, Int32>
        {
            if (column->getDataType() == TypeIndex::Int32)
            {
                const auto * casted_innner_column = assert_cast<const ColumnInt32 *>(column.get());
                Int32 begin_value = static_cast<Int32>(casted_innner_column->getInt(index));
                LOG_DEBUG(
                    &Poco::Logger::get("Partition"), "Partition value: {}, transform: {}, column_type: int", begin_value, partition_transform);
                return {dynamic_cast<const DataTypeNullable *>(column_data_type.get())->getNestedType(), begin_value};
            }
            else if (column->getDataType() == TypeIndex::Date && (partition_transform == PartitionTransform::Day))
            {
                const auto * casted_innner_column = assert_cast<const ColumnDate *>(column.get());
                Int32 begin_value = static_cast<Int32>(casted_innner_column->getInt(index));
                LOG_DEBUG(
                    &Poco::Logger::get("Partition"), "Partition value: {}, transform: {}, column type: date", begin_value, partition_transform);
                return {dynamic_cast<const DataTypeNullable *>(column_data_type.get())->getNestedType(), begin_value};
            }
            else
            {
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition column type: {}", column->getFamilyName());
            }
        }();
        if (WhichDataType(nested_data_type).isDate() && (partition_transform != PartitionTransform::Hour))
        {
            const UInt16 begin_range_value = getDay(value, partition_transform);
            const UInt16 end_range_value = getDay(value + 1, partition_transform);
            LOG_DEBUG(&Poco::Logger::get("Partition"), "Range begin: {}, range end {}", begin_range_value, end_range_value);
            return Range{begin_range_value, true, end_range_value, false};
        }
        else if (WhichDataType(nested_data_type).isDateTime64())
        {
            const UInt64 begin_range_value = getTime(value, partition_transform);
            const UInt64 end_range_value = getTime(value + 1, partition_transform);
            return Range{begin_range_value, true, end_range_value, false};
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Partition transform {} is not supported for the type: {}",
                partition_transform,
                nested_data_type);
        }
    }
};


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

    bool operator==(const IDataLakeMetadata & other) const override
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

    Strings makePartitionPruning(const ActionsDAG & filter_dag) override
    {
        LOG_DEBUG(&Poco::Logger::get("Make partition pruning"), "Make partition pruning");
        auto configuration_ptr = configuration.lock();
        if (!configuration_ptr)
        {
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Configuration is expired");
        }
        return pruning_processor.getDataFiles(
            common_partition_infos, specific_partition_infos, &filter_dag, getContext(), configuration_ptr->getPath());
    }

    bool supportsPartitionPruning() override { return true; }

private:
    mutable std::unordered_map<String, Int32> schema_id_by_data_file;

    const ObjectStoragePtr object_storage;
    const ConfigurationObserverPtr configuration;
    Int32 metadata_version;
    Int32 format_version;
    String manifest_list_file;
    Int32 current_schema_id;
    NamesAndTypesList schema;
    mutable Strings data_files;
    std::unordered_map<String, String> column_name_to_physical_name;
    DataLakePartitionColumns partition_columns;
    mutable IcebergSchemaProcessor schema_processor;
    LoggerPtr log;

    std::vector<std::pair<String, Int32>> manifest_files_with_start_index;

    std::vector<CommonPartitionInfo> partition_infos;
    mutable Strings manifest_files;

    PartitionPruningProcessor pruning_processor;
    mutable std::vector<CommonPartitionInfo> common_partition_infos;
    mutable std::vector<SpecificSchemaPartitionInfo> specific_partition_infos;
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
