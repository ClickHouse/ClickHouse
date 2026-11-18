#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/Statistics/ConditionSelectivityEstimator.h>

#include <optional>
#include <set>
#include <vector>

namespace DB
{

struct ColumnStats;
struct DataLakeReadEstimate;

/// Merges the per-file statistics that a data lake keeps in its metadata over the data files a read keeps after
/// pruning, as the `Basic` statistics of MergeTree parts are merged, and estimates the number of distinct values of
/// each column. The format-specific code extracts the `ColumnMetrics` of each data file.
class DataLakeColumnStatistics
{
public:
    /// A requested column that gets statistics.
    struct Target
    {
        /// Storage column name, the key of its statistics and its number of distinct values.
        String name;
        /// The statistics are built with this type, so the estimator accepts them for the column.
        DataTypePtr storage_type;
        /// `storage_type` without `LowCardinality` and `Nullable`; bounds and partition values are converted to it.
        DataTypePtr nested_type;
        /// The number of distinct values that the table metadata gives for the whole column, if any.
        std::optional<UInt64> table_distinct_values;
    };

    /// What the metadata of a data file says about one target, typed as the storage column.
    struct ColumnMetrics
    {
        /// nullopt when the metric is missing or invalid.
        std::optional<UInt64> nulls;
        /// Null when the bounds are missing or do not convert.
        Field min_value;
        Field max_value;
        /// nullopt when the column is not identity-partitioned in the file or the value does not convert; Null for a
        /// NULL partition value.
        std::optional<Field> identity_partition_value;

        /// Every row of a file with `rows` rows holds `value`, so it stands in for a missing NULL count and bounds.
        void setIdentityPartitionValue(Field value, UInt64 rows, bool tracks_min_max);
    };

    explicit DataLakeColumnStatistics(ContextPtr context);

    /// Whether a column of this type, without `LowCardinality` and `Nullable`, can be a target.
    static bool canBeTarget(const IDataType & nested_type);

    void addTarget(Target target);
    [[nodiscard]] const std::vector<Target> & getTargets() const { return targets; }

    /// Adds a data file with `rows` rows; `file_metrics` is aligned with the targets.
    void addFile(UInt64 rows, const std::vector<ColumnMetrics> & file_metrics);

    /// Removes deleted rows from the row count; the column statistics keep them. Called after the last file.
    void subtractDeletedRows(UInt64 rows);

    /// Sets the rows of `estimate` and its estimator: the merged statistics of the targets with their numbers of
    /// distinct values. Called once, after the last file.
    void finalize(DataLakeReadEstimate & estimate);

private:
    /// What the number of distinct values needs beyond the merged statistics. Unlike those, it is known only when
    /// every file gives it.
    struct DistinctValuesInputs
    {
        /// Distinct identity-partition values over the files, for rule 1.
        std::set<Field> identity_partition_values;
        /// False once a file has no identity-partition value for the column.
        bool identity_partition_known = true;
    };

    /// The number of distinct values of a target by the first rule that applies (1: identity-partition values, 2: the
    /// range of the bounds, 3: the table statistics), clamped to [1, non-NULL rows]; std::nullopt if none does. `column`
    /// holds its merged statistics over `rows` rows.
    static std::optional<UInt64> estimateDistinctValues(
        const Target & target, const DistinctValuesInputs & inputs, const ColumnStats & column, UInt64 rows);

    std::vector<Target> targets;
    /// Aligned with `targets`.
    std::vector<DistinctValuesInputs> distinct_values_inputs;
    /// Sum of the rows of the added files, less the subtracted deleted rows.
    UInt64 total_rows = 0;
    /// Merges the per-file `Basic` statistics of each target, as those of MergeTree parts.
    ConditionSelectivityEstimatorBuilder statistics_builder;
};

}
