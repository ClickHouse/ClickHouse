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

/// Prunes DuckLake data files with the query filter DAG using
///  - ducklake_file_column_stats (min/max per column), and
///  - ducklake_file_partition_value + the partition spec of the file
///    (identity/year/month/day/hour transforms; bucket transforms cannot be pruned).
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

    /// Partition key conditions depend on the partition spec of each file; they are built
    /// on first use per spec (files of one table almost always share one spec).
    mutable std::optional<std::vector<DuckLakePartitionField>> cached_spec;
    mutable std::optional<PartitionKeyCondition> cached_partition_condition;

    /// Null when the spec contains unsupported transforms (bucket) or unknown columns.
    const PartitionKeyCondition * getPartitionCondition(const std::vector<DuckLakePartitionField> & spec) const;
};

}

}

#endif
