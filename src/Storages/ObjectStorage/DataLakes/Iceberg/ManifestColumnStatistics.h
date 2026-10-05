#pragma once

#include "config.h"

#if USE_AVRO

#include <Core/Field.h>
#include <Core/Names.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/ObjectStorage/DataLakes/IDataLakeMetadata.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/IcebergPath.h>
#include <Storages/ObjectStorage/DataLakes/Iceberg/ManifestFile.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <set>
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

/// Merges the manifest metrics of the requested columns over the data files that a read keeps after pruning, as the
/// `Basic` statistics of MergeTree parts are merged, and estimates the number of distinct values of each column.
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
        ContextPtr context);

    /// Adds a data file with a non-negative `record_count`.
    void addFile(const ProcessedManifestFileEntry & entry, const IcebergPathFromMetadata & path_to_manifest_file);

    /// Sets the merged statistics and the numbers of distinct values of `estimate`, whose `rows` is the sum of
    /// `record_count` over the added files; nothing is set when `rows` is unknown or 0.
    void finalize(DataLakeReadEstimate & estimate) const;

private:
    /// A requested column that gets statistics.
    struct Target
    {
        /// Storage column name, the key of its statistics and its number of distinct values.
        String name;
        /// Iceberg field id, which survives a rename; the manifest metrics are keyed by it.
        Int32 field_id;
        /// The statistics are built with this type, so the estimator accepts them for the column.
        DataTypePtr storage_type;
        /// `storage_type` without `LowCardinality` and `Nullable`; bounds and partition values are converted to it.
        DataTypePtr nested_type;
    };

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

    /// What a data file's manifest entry says about one target, typed as the storage column.
    struct ColumnMetrics
    {
        /// 0 when the metric is missing or invalid.
        UInt64 nulls = 0;
        /// Null when the bounds are missing or do not convert.
        Field min_value;
        Field max_value;
        /// nullopt when `column_sizes` has no valid entry.
        std::optional<UInt64> size;
        /// nullopt when the column is not identity-partitioned in the file's spec or the value does not convert; Null for a
        /// NULL partition value.
        std::optional<Field> identity_partition_value;
    };

    /// What the number of distinct values needs beyond the merged statistics. Unlike those, it is known only when
    /// every file gives it.
    struct DistinctValuesInputs
    {
        /// Sum of `column_sizes` over the files, for rule 3.
        UInt64 sizes = 0;
        /// False once a file has no `column_sizes` entry for the column.
        bool sizes_known = true;
        /// Distinct identity-partition values over the files, for rule 1.
        std::set<Field> identity_partition_values;
        /// False once a file has no identity-partition value for the column.
        bool identity_partition_known = true;
    };

    /// The types of the targets in the schema with id `file_schema_id`, the schema id with which a data file was written.
    const SchemaTypes & getSchemaTypes(Int32 file_schema_id);

    /// The metrics of every target in a data file with `rows` rows, aligned with `targets`.
    std::vector<ColumnMetrics> extractFileMetrics(
        const ProcessedManifestFileEntry & entry, const IcebergPathFromMetadata & path_to_manifest_file, UInt64 rows);

    /// Adds the size and the identity-partition value that a file gives for a target.
    static void addToDistinctValuesInputs(DistinctValuesInputs & inputs, const ColumnMetrics & metrics);

    /// The number of distinct values of a target by the first rule that applies (plan O3), clamped to [1, non-NULL rows];
    /// `column` holds its merged statistics over `rows` rows.
    static UInt64 estimateDistinctValues(const Target & target, const DistinctValuesInputs & inputs, const ColumnStats & column, UInt64 rows);

    /// Resolves field ids and the column types of each file's schema.
    const IcebergSchemaProcessor & schema_processor;
    std::vector<Target> targets;
    /// Aligned with `targets`.
    std::vector<DistinctValuesInputs> distinct_values_inputs;
    /// By the schema id with which a file was written, so the files written with one schema share the lookups.
    std::unordered_map<Int32, SchemaTypes> schema_types;
    /// Merges the per-file `Basic` statistics of each target, as those of MergeTree parts.
    ConditionSelectivityEstimatorBuilder statistics_builder;
};

}

#endif
