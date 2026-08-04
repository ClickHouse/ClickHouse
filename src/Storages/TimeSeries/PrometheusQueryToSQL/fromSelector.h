#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Makes a SQL query to read from an instant selector, for example
/// http_requests{job="prometheus"}
SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context);

/// Makes a SQL query to read from a range selector, for example
/// http_requests{job="prometheus"}[20m]
SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context);

/// Makes a SQL query reading the raw (timestamp, value) samples of an instant selector as a range vector, using
/// `node`'s own evaluation range (in particular its window, e.g. the default 5-minute lookback for an instant
/// selector). This is the same conversion `fromSelector(InstantSelector*)` uses before applying `last_over_time`;
/// it's also used by `applyFunctionTimestamp()` to read the raw samples of an instant selector it peeled a
/// timestamp() argument expression down to.
SQLQueryPiece fromRangeSelector(std::string_view instant_selector_text, const Node * node, ConverterContext & context);

}
