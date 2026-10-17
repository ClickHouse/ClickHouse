#pragma once

#include <Parsers/ASTFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// The pieces of a query aggregating the series of a range vector (one result row per series).
struct RangeVectorAggregation
{
    /// An expression returning the group (UInt64) of a series, for the SELECT list.
    ASTPtr group_expression;

    /// Goes to the GROUP BY list.
    ASTPtr group_by_key;

    /// The aggregate function over the samples of a series, for the SELECT list.
    ASTPtr aggregate_function;
};

/// Prepares the aggregation of `aggregate_function` (with its parameters and arguments) over the series of a range vector,
/// which must be stored as RAW_DATA or VECTOR_GRID.
///
/// A vector grid already has one row per series: it is grouped by the column `group` and the function is applied as is.
///
/// Raw data (see StoreMethod::RAW_DATA) evaluated at one point is grouped by `timeSeriesIdToGroup(id)`: a set of tags
/// can be stored under several ids, and the group merges their samples.
///
/// Raw data evaluated at several points (see `ConverterContext::aggregate_range_vectors_in_order`) is aggregated in two levels.
/// The SELECT query of `range_vector` is replaced with the first level
///     SELECT timeSeriesIdToGroup(id) AS group, <aggregate_function>State(<parameters>)(<arguments>) AS state FROM <raw_data> GROUP BY id
/// and the returned pieces make the second level `SELECT group, <aggregate_function>Merge(<parameters>)(state) ... GROUP BY group`.
/// The samples table is sorted by `(id, timestamp)`, so with `optimize_aggregation_in_order` the first level runs in that order
/// and holds the state of one series at a time; the second level merges the states of the ids of one set of tags.
RangeVectorAggregation aggregateRangeVector(SQLQueryPiece & range_vector, boost::intrusive_ptr<ASTFunction> aggregate_function, ConverterContext & context);

}
