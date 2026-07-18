#pragma once

#include "config.h"

#if USE_PARQUET

#include <Core/Field.h>
#include <Core/NamesAndTypes.h>
#include <Interpreters/ActionsDAG.h>
#include <Interpreters/Context_fwd.h>
#include <Storages/MergeTree/KeyCondition.h>

#include <optional>
#include <unordered_map>
#include <vector>

namespace DB
{

struct DuckLakeDataFileEntry;
struct DuckLakePartitionField;

namespace DuckLake
{

/// Parse a DuckLake-serialized stats/partition value (plain numbers, ISO dates/timestamps,
/// raw strings, DuckDB booleans) into a Field of `type`. Returns nullopt when the value
/// cannot be parsed unambiguously for the type (caller must not prune on it).
std::optional<Field> parseStatsValue(const String & value, const DataTypePtr & type);

/// Closed interval of calendar bucket numbers (year, month 1-12 or day 1-31); unbounded
/// sides are nullopt.
struct BucketRange
{
    std::optional<int32_t> lo;
    std::optional<int32_t> hi;

    bool contains(int64_t value) const
    {
        return (!lo.has_value() || value >= *lo) && (!hi.has_value() || value <= *hi);
    }
};

/// Per-column pruning constraints on the calendar buckets of the partition transforms.
struct CalendarConstraints
{
    BucketRange year;
    BucketRange month;
    BucketRange day;
};

/// Prunes DuckLake data files with the query filter DAG using
///  - ducklake_file_column_stats (min/max per column), and
///  - ducklake_file_partition_value + the partition spec of the file:
///    * identity transforms via a single-point KeyCondition,
///    * year/month/day transforms via exact calendar bucket constraints, computed from
///      source-column ranges (KeyCondition::extractPlainRanges) and from function-form
///      predicates (`toYear(col) = 2025` etc.); bucket transforms cannot be pruned.
/// The filter DAG pointer must stay valid for the lifetime of the pruner.
class FilePruner
{
public:
    /// `filter_dag` may be null (then nothing is pruned).
    /// `field_id_map` is the ColumnMapper encoding (dotted name -> column_id).
    /// `column_types` maps column_id -> current name+type of every visible column.
    FilePruner(
        const ActionsDAG * filter_dag,
        const std::unordered_map<String, Int64> & field_id_map,
        const std::unordered_map<Int64, NameAndTypePair> & column_types,
        ContextPtr context);

    /// True when the file provably contains no rows matching the filter.
    /// `partition_spec` is the spec referenced by the file (null when the table is not
    /// partitioned or the spec is not visible anymore).
    bool canBePruned(
        const DuckLakeDataFileEntry & file,
        const std::vector<DuckLakePartitionField> * partition_spec) const;

private:
    struct MinMaxCondition
    {
        Int64 column_id;
        DataTypePtr type; /// non-nullable
        KeyCondition condition;
    };

    struct PartitionKeyCondition
    {
        DataTypes key_data_types;
        KeyCondition condition;
    };

    std::vector<MinMaxCondition> min_max_conditions;

    const ActionsDAG * filter_dag;
    std::unordered_map<Int64, NameAndTypePair> column_types_by_id;
    ContextPtr context;

    /// Identity partition key conditions depend on the partition spec of each file; they
    /// are built on first use per spec (files of one table almost always share one spec).
    mutable std::optional<std::vector<DuckLakePartitionField>> cached_spec;
    mutable std::optional<PartitionKeyCondition> cached_partition_condition;

    /// Null when the spec has no identity fields or unknown columns.
    const PartitionKeyCondition * getIdentityCondition(const std::vector<DuckLakePartitionField> & spec) const;

    /// Calendar constraints from function-form predicates (`toYear(col) = 2025` and
    /// range comparisons on `toYear`/`toMonth`/`toDayOfMonth`), per source column name.
    /// Computed once from the filter DAG.
    std::unordered_map<String, CalendarConstraints> function_form_constraints;
    void collectFunctionFormConstraints();

    /// Calendar constraints from source-column ranges (`col >= a AND col < b`), per
    /// column; built lazily because extracting plain ranges needs a KeyCondition per
    /// column. Columns whose type cannot produce unambiguous buckets (timestamps
    /// without an explicit timezone) yield no constraints.
    mutable std::unordered_map<String, CalendarConstraints> source_range_constraints;
    const CalendarConstraints & getCalendarConstraints(const String & column_name, const DataTypePtr & column_type) const;
};

}

}

#endif
