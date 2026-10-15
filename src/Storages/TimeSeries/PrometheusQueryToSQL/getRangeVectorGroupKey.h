#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// The key of an aggregation over the series of a range vector, and the expression producing the group of each result row.
struct RangeVectorGroupKey
{
    /// An expression over `group_by_key` returning the group (UInt64) of a series, for the SELECT list.
    ASTPtr group_expression;

    /// Goes to the GROUP BY list.
    ASTPtr group_by_key;
};

/// Returns what to put into the SELECT list and the GROUP BY list of a query aggregating the series of a range vector
/// (one result row per series).
///
/// Raw data (see StoreMethod::RAW_DATA) is grouped by `id`: the samples table is sorted by `(id, timestamp)`, so with
/// `optimize_aggregation_in_order` the aggregation keeps the state of one series at a time instead of all series.
/// The group is then calculated from the key: `timeSeriesIdToGroup(id)`.
///
/// A vector grid already has one row per series, so it is grouped by the column `group` itself.
///
/// The range vector must be stored as RAW_DATA or VECTOR_GRID.
RangeVectorGroupKey getRangeVectorGroupKey(const SQLQueryPiece & range_vector, const ConverterContext & context);

}
