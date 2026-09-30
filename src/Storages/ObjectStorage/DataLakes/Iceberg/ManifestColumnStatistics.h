#pragma once

#include "config.h"

#if USE_AVRO

#include <Core/Field.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

#include <optional>
#include <set>
#include <unordered_map>
#include <vector>

namespace DB
{
class ColumnsDescription;
}

namespace DB::Iceberg
{

class IcebergSchemaProcessor;

/// Aggregates the manifest metrics of the requested columns over the data files that a read keeps after pruning.
class ManifestColumnStatistics
{
public:
    /// Keeps the columns of `column_names` that are physical, not `Array`, `Map`, `Tuple` or `Variant`, and have a
    /// field id in the schema `schema_id`.
    ManifestColumnStatistics(
        const Names & column_names,
        const ColumnsDescription & columns,
        const IcebergSchemaProcessor & schema_processor_,
        Int32 schema_id);

    /// Adds a data file with a non-negative `record_count`.
    void addFile(const ProcessedManifestFileEntry & entry, const IcebergPathFromMetadata & path_to_manifest_file);

    /// `rows` is the sum of `record_count` over the added files; empty when it is 0.
    std::unordered_map<String, DataLakeColumnEstimate> finalize(UInt64 rows) const;

private:
    struct Target
    {
        String name;
        Int32 field_id;
        DataTypePtr storage_type;
        /// Without `Nullable`.
        DataTypePtr nested_type;
        bool tracks_min_max;
    };

    /// The targets in the schema that a data file was written with.
    struct FileSchemaColumns
    {
        /// Aligned with `targets`; nullptr where the schema predates the column.
        std::vector<DataTypePtr> file_types;
        /// Field id -> type in the file's schema, for the targets with `tracks_min_max`.
        std::unordered_map<Int32, DataTypePtr> bound_types;
    };

    struct Accumulator
    {
        UInt64 nulls = 0;
        bool nulls_known = true;
        UInt64 sizes = 0;
        bool sizes_known = true;
        /// Typed as the storage column.
        std::optional<Field> min_value;
        std::optional<Field> max_value;
        bool bounds_missing = false;
        std::set<Field> identity_partition_values;
        bool identity_partition_known = true;
    };

    const FileSchemaColumns & getFileSchemaColumns(Int32 file_schema_id);

    const IcebergSchemaProcessor & schema_processor;
    std::vector<Target> targets;
    std::vector<Accumulator> accumulators;
    std::unordered_map<Int32, FileSchemaColumns> file_schema_columns;
};

}

#endif
