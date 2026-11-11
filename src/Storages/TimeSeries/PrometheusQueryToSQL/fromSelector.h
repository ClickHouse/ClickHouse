#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>

#include <optional>


namespace DB::PrometheusQueryToSQL
{

/// Makes a SQL query to read from an instant selector, for example
/// http_requests{job="prometheus"}
SQLQueryPiece fromSelector(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context);

/// Makes a SQL query to evaluate the PromQL function timestamp() on an instant selector, for example
/// timestamp(http_requests{job="prometheus"})
/// It selects the same samples as `fromSelector(instant_selector_node)` does, but returns their timestamps
/// (in seconds since epoch) instead of their values.
SQLQueryPiece fromSelectorSampleTimestamps(const PrometheusQueryTree::InstantSelector * instant_selector_node, ConverterContext & context);

/// Same, but reads the series matching `instant_selector_text` over the evaluation range of `node`,
/// only the samples with timestamps within `time_bounds` if it's set, only the series satisfying `series_filter` (an expression
/// of the column `group`) if it's set. With `exact_ticks` the value of a sample is its timestamp in ticks of the timestamp scale of the
/// table since the start of the range read (exact, unlike Float64 seconds), so that timestamps can be compared for equality.
SQLQueryPiece fromSelectorSampleTimestamps(
    const Node * node,
    const String & instant_selector_text,
    std::optional<std::pair<TimestampType, TimestampType>> time_bounds,
    ASTPtr series_filter,
    bool exact_ticks,
    ConverterContext & context);

/// Makes a SQL query to read from a range selector, for example
/// http_requests{job="prometheus"}[20m]
SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context);

}
