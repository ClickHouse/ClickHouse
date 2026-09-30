#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <functional>

namespace DB::PrometheusQueryToSQL
{
struct ConverterContext;

/// Returns true if the specified function is sort(), sort_desc(), sort_by_label() or sort_by_label_desc().
bool isSortFunction(std::string_view function_name);

/// Applies sort(), sort_desc(), sort_by_label() or sort_by_label_desc() to an instant vector.
/// These functions do not change the values — they order the vector at the call site: sort()/sort_desc()
/// by sample value, sort_by_label()/sort_by_label_desc() by the natural sort order of the given label values.
SQLQueryPiece applySortFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context);

/// Re-keys the sort rank map of `query_piece` (if any) after a transform changed the series ids (`group`);
/// `transform_group` must build the same group-changing expression the transform applied to the data.
void rekeySortRankSubquery(
    SQLQueryPiece & query_piece, const std::function<ASTPtr(ASTPtr)> & transform_group, ConverterContext & context);

/// Makes `query` (a vector grid with a column `sort_source`) the value of `query_piece`,
/// ordering each row as its `sort_source` series is ordered in `source_rank_subquery`.
void setVectorGridRankedBySource(
    SQLQueryPiece & query_piece, ASTPtr && query, const String & source_rank_subquery, ConverterContext & context);

/// Makes `query` (an `or` result with a column `sort_from_right`) the value of `query_piece`,
/// keeping the order of the left rows and putting the right rows after them in their own order.
void setOrResultWithSortRank(
    SQLQueryPiece & query_piece,
    ASTPtr && query,
    const String & left_rank_subquery,
    const String & right_rank_subquery,
    ConverterContext & context);
}
