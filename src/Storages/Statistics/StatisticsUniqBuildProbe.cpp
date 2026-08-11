#include <Storages/Statistics/Statistics.h>
#include <Storages/Statistics/StatisticsUniqBuildProbe.h>

#include <algorithm>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnSparse.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/IDataType.h>
#include <Common/typeid_cast.h>

namespace DB
{

ColumnPtr getRawColumnForUniqBuild(const ColumnPtr & column)
{
    /// For sparse and low cardinality columns an extra default
    /// value may be added. That is ok since the uniq count is an estimation.
    if (const auto * column_sparse = typeid_cast<const ColumnSparse *>(column.get()))
        return column_sparse->getValuesPtr();
    if (const auto * column_low_cardinality = typeid_cast<const ColumnLowCardinality *>(column.get()))
        return column_low_cardinality->getDictionary().getNestedColumn();
    return column;
}

bool canUseAssumedAllDistinctForUniqBuild(const ColumnPtr & column)
{
    /// LowCardinality and Sparse columns intentionally store fewer physical values than
    /// logical rows: their representation alone signals repeated values, so an all-distinct
    /// assumption is inappropriate. Note that this is a representation check only; the
    /// decision code additionally applies a data-based default-ratio guard, because on the
    /// insert path a default-dominated column arrives as a full (not yet sparse) column.
    return !typeid_cast<const ColumnLowCardinality *>(column.get()) && !typeid_cast<const ColumnSparse *>(column.get());
}

bool dataTypeSupportsUniqStatistics(const DataTypePtr & data_type)
{
    DataTypePtr inner_data_type = removeNullable(data_type);
    inner_data_type = removeLowCardinalityAndNullable(inner_data_type);
    return inner_data_type->isValueRepresentedByNumber() || isStringOrFixedString(inner_data_type);
}

bool StatisticsUniqStringProbe::canProbe(
    const ColumnPtr & column, const DataTypePtr & inner_data_type, const StatisticsBuildOptions & options) const
{
    return canUseAssumedAllDistinctForUniqBuild(column) && options.assume_long_strings_distinct && isStringOrFixedString(inner_data_type)
        && options.long_string_distinct_probe_rows != 0 && !finished;
}

StatisticsUniqStringProbeResult
StatisticsUniqStringProbe::probe(const ColumnPtr & column, bool may_have_nulls, const StatisticsBuildOptions & options)
{
    StatisticsUniqStringProbeResult result;

    const UInt64 remaining_probe_rows = options.long_string_distinct_probe_rows > rows ? options.long_string_distinct_probe_rows - rows : 0;
    const UInt64 rows_to_probe = std::min<UInt64>(column->size(), remaining_probe_rows);

    if (rows_to_probe > 0)
    {
        auto probe_column = rows_to_probe == column->size() ? column : column->cut(0, rows_to_probe);
        update(probe_column, may_have_nulls);
    }

    result.total_probe_rows = rows;
    result.total_probe_non_null_rows = non_null_rows;
    result.total_probe_bytes = bytes;

    if (rows < options.long_string_distinct_probe_rows)
    {
        result.need_more_rows = true;
        return result;
    }

    finished = true;
    result.assume_all_distinct = non_null_rows != 0 && bytes / non_null_rows >= options.long_string_distinct_min_length;
    if (result.assume_all_distinct)
        result.assumed_cardinality = non_null_rows;

    if (rows_to_probe < column->size())
        result.unprobed_column_tail = column->cut(rows_to_probe, column->size() - rows_to_probe);

    return result;
}

void StatisticsUniqStringProbe::update(const ColumnPtr & column, bool may_have_nulls)
{
    auto raw_column = getRawColumnForUniqBuild(column);
    rows += column->size();
    for (size_t i = 0, size = raw_column->size(); i < size; ++i)
    {
        if (may_have_nulls && raw_column->isNullAt(i))
            continue;
        ++non_null_rows;
        bytes += raw_column->getDataAt(i).size();
    }
}

}
