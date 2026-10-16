#pragma once

#include <DataTypes/IDataType.h>
#include <base/types.h>

namespace DB
{

struct StatisticsBuildOptions;

ColumnPtr getRawColumnForUniqBuild(const ColumnPtr & column);
bool canUseAssumedAllDistinctForUniqBuild(const ColumnPtr & column);

/// Shared predicate for all uniq-like statistics (real sketches and the assumed-all-distinct
/// materialization): supported for types whose values are represented by numbers, and for strings.
bool dataTypeSupportsUniqStatistics(const DataTypePtr & data_type);

struct StatisticsUniqStringProbeResult
{
    ColumnPtr unprobed_column_tail;
    UInt64 assumed_cardinality = 0; // Cardinality already accounted for by the probed non-NULL rows.
    UInt64 total_probe_rows = 0;
    UInt64 total_probe_non_null_rows = 0;
    UInt64 total_probe_bytes = 0;
    bool need_more_rows = false;
    bool assume_all_distinct = false;
};

class StatisticsUniqStringProbe
{
public:
    bool canProbe(const ColumnPtr & column, const DataTypePtr & inner_data_type, const StatisticsBuildOptions & options) const;
    StatisticsUniqStringProbeResult probe(const ColumnPtr & column, bool may_have_nulls, const StatisticsBuildOptions & options);

private:
    void update(const ColumnPtr & column, bool may_have_nulls);

    UInt64 rows = 0;
    UInt64 non_null_rows = 0;
    UInt64 bytes = 0;
    bool finished = false;
};

}
