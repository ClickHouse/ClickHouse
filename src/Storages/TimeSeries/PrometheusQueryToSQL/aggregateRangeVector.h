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

    /// Goes to the GROUP BY list. Null if the SELECT query of the range vector already has one row per series:
    /// `aggregate_function` is then a column of that query and the consumer does not group.
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
/// Raw data evaluated at several points (see `ConverterContext::aggregate_range_vectors_in_order`) is aggregated per id
/// in the order of the samples table, `(id, timestamp)`, so with `optimize_aggregation_in_order` the aggregation holds
/// the state of one series at a time:
///     SELECT id, timeSeriesIdToGroup(id) AS group, <aggregate_function>(<parameters>)(<arguments>) AS values_by_series
///     FROM <raw_data> GROUP BY id
/// The ids whose set of tags is stored under several matching ids are excluded from that result with an anti join
/// to `timeSeriesSelector(..., 'ids_sharing_tags')`, and their samples are aggregated by group as at one point from
/// `timeSeriesSelector(..., 'samples_of_ids_sharing_tags')`, which reads nothing when there are no such ids.
/// The SELECT query of `range_vector` is replaced with the union of both parts, and the returned pieces select
/// its columns `group` and `values_by_series` without grouping.
RangeVectorAggregation aggregateRangeVector(SQLQueryPiece & range_vector, boost::intrusive_ptr<ASTFunction> aggregate_function, ConverterContext & context);

}
