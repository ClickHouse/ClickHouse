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

/// How the value of a sample is made of its timestamp: Float64 seconds, which can't tell apart the timestamps of a DateTime64(9) table,
/// or the number of ticks of the timestamp scale of the table since the start of the range read, which is exact if the range is
/// shorter than 2^53 ticks. For a longer range the ticks are split into the high part (ticks >> 26) and the low part (ticks & (2^26 - 1)).
enum class SampleTimestampKind
{
    SECONDS,
    TICKS,
    TICKS_HIGH,
    TICKS_LOW,
};

/// Same, but reads the series matching `instant_selector_text` (or the SQL expression `selector_argument` if it's set) over the
/// evaluation range of `node`, only the samples with timestamps within `time_bounds` if it's set, only the series satisfying
/// `series_filter` (an expression of the column `group`) if it's set.
SQLQueryPiece fromSelectorSampleTimestamps(
    const Node * node,
    const String & instant_selector_text,
    std::optional<std::pair<TimestampType, TimestampType>> time_bounds,
    ASTPtr series_filter,
    ASTPtr selector_argument,
    SampleTimestampKind timestamp_kind,
    ConverterContext & context);

/// Makes a SQL query to read from a range selector, for example
/// http_requests{job="prometheus"}[20m]
SQLQueryPiece fromSelector(const PrometheusQueryTree::RangeSelector * range_selector_node, ConverterContext & context);

}
