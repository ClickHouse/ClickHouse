#pragma once

#include "config.h"

#if USE_AVRO

#include <Core/Field.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/Common/DataLakeColumnStatistics.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>

#include <unordered_map>
#include <vector>

/// How a column is found in the manifest metrics of a data file. `metadata.json` keeps every schema the table had, and
/// each data file was written with one of them:
///
///     metadata.json
///       schemas:
///         schema-id 0: { field 1: k int }
///         schema-id 1: { field 1: k long }      after MODIFY COLUMN k Int64
///         schema-id 2: { field 1: w long }      after RENAME COLUMN k TO w: same field id
///       current-schema-id: 2
///     manifest entries:
///       file A, added by a snapshot of schema 0: lower_bounds {1: 4 bytes}, null_value_counts {1: ...}, ...
///       file B, added by a snapshot of schema 1: lower_bounds {1: 8 bytes}, ...
///
/// - The field id names a column across all schemas, so a rename keeps it, and the metrics are keyed by it.
/// - The schema id with which a file was written (`ProcessedManifestFileEntry::resolved_schema_id`) says how to read the
///   file's metrics: file A's bounds are an `int`, although the column is now `long`.
/// - A column missing from a file's schema was added after the file was written, so the file holds only NULLs in it.
/// Here the storage column name gives the field id once, in the schema of the read; then each file's schema gives the
/// type its bounds are decoded with, before they are converted to the storage type.

namespace DB
{
class ColumnsDescription;
}

namespace DB::Iceberg
{

class IcebergSchemaProcessor;

/// Extracts the manifest metrics of the requested columns from the data files that a read keeps after pruning and
/// merges them with `DataLakeColumnStatistics`.
class ManifestColumnStatistics
{
public:
    /// Keeps the columns of `column_names` that are physical, not `Array`, `Map`, `Tuple` or `Variant`, and have a
    /// field id in the schema with id `schema_id`, the schema the read uses.
    ManifestColumnStatistics(
        const Names & column_names,
        const ColumnsDescription & columns,
        const IcebergSchemaProcessor & schema_processor_,
        Int32 schema_id,
        ContextPtr context,
        const std::unordered_map<Int32, UInt64> & table_distinct_values);

    /// Adds a data file with a non-negative `record_count`.
    void addFile(const ProcessedManifestFileEntry & entry, const IcebergPathFromMetadata & path_to_manifest_file);

    /// Removes deleted rows from the row count; the column statistics keep them. Called after the last file.
    void subtractDeletedRows(UInt64 rows);

    /// Sets the rows of `estimate` and its estimator: the merged statistics of the requested columns with their numbers of
    /// distinct values. Called once, after the last file.
    void finalize(DataLakeReadEstimate & estimate);

private:
    using ColumnMetrics = DataLakeColumnStatistics::ColumnMetrics;

    /// A target's type in one schema.
    struct FieldType
    {
        /// Without `LowCardinality` and `Nullable`; the file's bounds and partition values are decoded with it.
        DataTypePtr nested_type;
        /// The type can have min/max statistics (numbers and dates), so its bounds are decoded.
        bool tracks_min_max;
        /// Index in `targets`.
        size_t target_index;
    };

    /// Field id -> type, for the targets that a schema has.
    using SchemaTypes = std::unordered_map<Int32, FieldType>;

    /// The types of the targets in the schema with id `file_schema_id`, the schema id with which a data file was written.
    const SchemaTypes & getSchemaTypes(Int32 file_schema_id);

    /// The metrics of every target in a data file with `rows` rows, aligned with `targets`.
    std::vector<ColumnMetrics> extractFileMetrics(
        const ProcessedManifestFileEntry & entry, const IcebergPathFromMetadata & path_to_manifest_file, UInt64 rows);

    /// Resolves field ids and the column types of each file's schema.
    const IcebergSchemaProcessor & schema_processor;
    /// The targets, their merged statistics and numbers of distinct values.
    DataLakeColumnStatistics statistics;
    /// Iceberg field ids of the targets, which survive a rename; the manifest metrics are keyed by them.
    std::vector<Int32> field_ids;
    /// By the schema id with which a file was written, so the files written with one schema share the lookups.
    std::unordered_map<Int32, SchemaTypes> schema_types;
};

}

#endif
