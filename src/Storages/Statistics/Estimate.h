#pragma once

#include <Core/Field.h>
#include <Storages/StatisticsDescription.h>

#include <optional>
#include <set>
#include <unordered_map>

namespace DB
{

/// The per-column estimate produced by the explicit column statistics (see `ColumnStatistics::getEstimate`)
/// and, for the counts, by sampling the written data (see `EstimatesBuilder`). Kept in a small leaf header
/// so the serialization code can use it without depending on the statistics subsystem.
struct Estimate
{
    std::set<StatisticsType> types;
    UInt64 rows_count = 0;
    std::optional<UInt64> estimated_cardinality;
    std::optional<Field> estimated_min;
    std::optional<Field> estimated_max;
    std::optional<UInt64> estimated_null_count;
    /// Number of default values in the column. Set when it is known: exactly from `Basic` external
    /// statistics on a sparse-capable column, or sampled by `EstimatesBuilder` from the data.
    /// Together with `rows_count` it drives the choice of sparse serialization.
    std::optional<UInt64> num_defaults;
};

/// Per-column estimates keyed by column name; the serialization machinery also stores per-subcolumn
/// entries keyed by `subcolumnEstimateKey` paths.
using Estimates = std::unordered_map<String, Estimate>;

/// Key of a subcolumn (currently only a `Tuple` element) in a flat, path-keyed `Estimates` map.
/// Deliberately not `Nested::concatenateName`: a column named `t.a` is legal (`Nested` flattening
/// routinely produces dotted names) and must not share a key with element `a` of a tuple column `t`,
/// so the components are joined with a byte that does not occur in practical column names.
inline String subcolumnEstimateKey(const String & prefix, const String & subcolumn_name)
{
    String key;
    key.reserve(prefix.size() + subcolumn_name.size() + 1);
    key += prefix;
    key += '\0';
    key += subcolumn_name;
    return key;
}

}
