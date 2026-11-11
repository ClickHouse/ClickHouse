#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <base/types.h>

#include <algorithm>
#include <optional>
#include <unordered_map>

namespace DB
{

struct ColumnStats
{
    UInt64 num_distinct_values = 0;
    /// NDV of the column in the whole relation it comes from, before any filter; zero when unknown.
    /// Two filtered sets of the column's values overlap in proportion to their shares of it.
    UInt64 domain_distinct_values = 0;
    /// Rows of the table the column is read from, and the rows of it that the read selects; zero when unknown.
    UInt64 table_rows = 0;
    UInt64 table_selected_rows = 0;
    /// Average uncompressed size of one value; 0 means unknown.
    Float64 avg_bytes = 0;
    /// Value range, as values of `value_type`; unset when unknown. It comes from `basic`/`minmax` statistics and
    /// a monotonic function can map it. A user must check the type: a `JOIN USING` can change the type of the
    /// column and keep the range. A step that can add values outside the range clears it.
    std::optional<Field> min_value = {};
    std::optional<Field> max_value = {};
    DataTypePtr value_type = nullptr;
    /// Fraction of NULL values; unset when unknown.
    std::optional<Float64> null_fraction = {};
    /// Most distinct values the column can have, proven by a filter (`x = c`, `x IN (c1, ...)`) and kept
    /// by what follows it; zero when nothing proves a limit. Unlike `num_distinct_values` it never
    /// comes from a sketch, so the row bounds may use it.
    UInt64 max_distinct_values = 0;
    /// Rows of the column that its statistics do not describe (parts without statistics of the column); each can add
    /// a value that `num_distinct_values` does not count.
    UInt64 rows_without_statistics = 0;
    /// Every value of the column is on at most one row: the column is the only key of an aggregation. A join on it
    /// matches each row of the other side at most once. Whatever can repeat a row (a join, a union) drops the flag.
    bool unique = false;

    /// Whether the rows without statistics could add as many values as `num_distinct_values` counts, or more, within
    /// the proven limit: a group count, a join or a filter that uses the NDV is then not precise.
    bool hasImpreciseDistinctValues() const
    {
        if (rows_without_statistics == 0 || num_distinct_values == 0)
            return false;
        UInt64 new_values = rows_without_statistics;
        if (max_distinct_values != 0)
            new_values = std::min(new_values, max_distinct_values - std::min(max_distinct_values, num_distinct_values));
        return new_values >= num_distinct_values;
    }

    void clearValueRange()
    {
        min_value.reset();
        max_value.reset();
        value_type = nullptr;
    }
};

struct RelationProfile
{
    UInt64 rows = 0;
    std::unordered_map<String, ColumnStats> column_stats = {};
    /// A predicate the statistics could not estimate took the default selectivity, so `rows` is a guess.
    bool estimate_from_defaults = false;
};

}
