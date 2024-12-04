#pragma once

#include "Columns/IColumn.h"
#include "Core/NamesAndTypes.h"
#include "DataTypes/DataTypeNullable.h"
#include "DataTypes/DataTypeTuple.h"
#include "config.h"

#if USE_AVRO /// StorageIceberg depending on Avro to parse metadata with Avro format.

#    include <Core/Types.h>
#    include <Disks/ObjectStorages/IObjectStorage.h>
#    include <Interpreters/Context_fwd.h>
#    include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#    include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{


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
        const ColumnTuple * data_file_tuple_column,
        const DataTypeTuple & data_file_tuple_type) const;

    SpecificSchemaPartitionInfo getSpecificPartitionInfo(
        const CommonPartitionInfo & common_info,
        Int32 schema_version,
        const std::unordered_map<Int32, NameAndTypePair> & name_and_type_by_source_id) const;

    std::vector<bool>
    getPruningMask(const SpecificSchemaPartitionInfo & specific_info, const ActionsDAG * filter_dag, ContextPtr context) const;

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
            return DateLUT::instance().getValues(static_cast<UInt16>(value));
        }
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition transform for get day function: {}", transform);
    }

    static Int64 getTime(Int32 value, PartitionTransform transform)
    {
        DateLUTImpl::Values values = getValues(value, transform);
        // LOG_DEBUG(&Poco::Logger::get("Get field"), "Values: {}", values);
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
        if (partition_transform == PartitionTransform::Year || partition_transform == PartitionTransform::Month
            || partition_transform == PartitionTransform::Day)
        {
            auto column = dynamic_cast<const ColumnNullable *>(partition_column.get())->getNestedColumnPtr();
            const auto * casted_innner_column = assert_cast<const ColumnInt32 *>(column.get());
            Int32 value = static_cast<Int32>(casted_innner_column->getInt(index));
            LOG_DEBUG(&Poco::Logger::get("Partition"), "Partition value: {}, transform: {}", value, partition_transform);
            auto nested_data_type = dynamic_cast<const DataTypeNullable *>(column_data_type.get())->getNestedType();
            if (WhichDataType(nested_data_type).isDate())
            {
                const UInt16 begin_range_value = getDay(value, partition_transform);
                const UInt16 end_range_value = getDay(value + 1, partition_transform);
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
        else
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported partition transform: {}", partition_transform);
        }
    }
};


class IcebergMetadata : public IDataLakeMetadata, private WithContext
{
public:
    using ConfigurationObserverPtr = StorageObjectStorage::ConfigurationObserverPtr;

    static constexpr auto name = "Iceberg";

    enum class PartitionTransform
    {
        Year,
        Month,
        Day,
        Hour,
        Unsupported
    };

    IcebergMetadata(
        ObjectStoragePtr object_storage_,
        ConfigurationObserverPtr configuration_,
        ContextPtr context_,
        Int32 metadata_version_,
        Int32 format_version_,
        String manifest_list_file_,
        Int32 current_schema_id_,
        NamesAndTypesList schema_);

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

    static DataLakeMetadataPtr create(ObjectStoragePtr object_storage, ConfigurationObserverPtr configuration, ContextPtr local_context);

    Strings makePartitionPruning(const ActionsDAG & filter_dag) override
    {
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
    size_t getVersion() const { return metadata_version; }

    const ObjectStoragePtr object_storage;
    const ConfigurationObserverPtr configuration;
    Int32 metadata_version;
    Int32 format_version;
    String manifest_list_file;
    Int32 current_schema_id;
    NamesAndTypesList schema;
    std::unordered_map<String, String> column_name_to_physical_name;
    DataLakePartitionColumns partition_columns;
    LoggerPtr log;

    std::unordered_map<Int32, NameAndTypePair> name_and_type_by_source_id;

    std::vector<std::pair<String, Int32>> manifest_files_with_start_index;

    mutable Strings data_files;
    std::vector<CommonPartitionInfo> partition_infos;
    mutable Strings manifest_files;

    PartitionPruningProcessor pruning_processor;
    mutable std::vector<CommonPartitionInfo> common_partition_infos;
    mutable std::vector<SpecificSchemaPartitionInfo> specific_partition_infos;
};

}

#endif
