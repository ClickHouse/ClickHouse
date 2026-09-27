#pragma once

#include <base/Decimal.h>
#include <optional>


namespace DB
{
class PrometheusQueryTree;

/// Helper utilities for parsing prometheus queries. These utilities are used by PrometheusQueryTree.
struct PrometheusQueryParsingUtil
{
    using ScalarType = Float64;
    using TimestampType = DateTime64;
    using DurationType = Decimal64;

    /// Parses a prometheus query.
    /// Scale `timestamp_scale` is used to parse decimals representing timestamps and durations.
    static bool tryParseQuery(std::string_view input,
                              UInt32 timestamp_scale,
                              PrometheusQueryTree & res_query,
                              String * error_message = nullptr,
                              size_t * error_pos = nullptr);

    /// Converts a quoted string literal to its unquoted version: "abc" -> abc
    /// Accepts an input string in quotes or double quotes or backticks, and also handles escape sequences
    /// according to the PromQL rules (see https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals).
    static bool tryParseStringLiteral(std::string_view input,
                                      String & res_string,
                                      String * error_message = nullptr,
                                      size_t * error_pos = nullptr);

    /// Parses a scalar which can be either an integer number or a floating-point number (e.g. 237e6), or Inf, or Nan,
    /// or a hexadecimal number (e.g. 0xA7CD), or a duration with time units, for example 1m30s.
    /// Also underscores (_) can be used in between decimal or hexadecimal digits and they don't mean anything.
    static bool tryParseScalar(std::string_view input,
                               ScalarType & res_scalar,
                               String * error_message = nullptr,
                               size_t * error_pos = nullptr);

    /// Parses a timestamp which can be either an integer or floating-point number of seconds since epoch (1 January 1970),
    /// or a hexadecimal number of seconds since epoch, or a duration with time units since epoch.
    /// Also underscores (_) can be used in between decimal or hexadecimal digits and they don't mean anything.
    static bool tryParseTimestamp(std::string_view input,
                                  UInt32 timestamp_scale,
                                  TimestampType & res_timestamp,
                                  String * error_message = nullptr,
                                  size_t * error_pos = nullptr);

    /// Parses a timestamp which can be either an integer or floating-point number of seconds,
    /// or a hexadecimal number of seconds, or a duration with time units.
    /// Also underscores (_) can be used in between decimal or hexadecimal digits and they don't mean anything.
    static bool tryParseDuration(std::string_view input,
                                 UInt32 timestamp_scale,
                                 DurationType & res_duration,
                                 String * error_message = nullptr,
                                 size_t * error_pos = nullptr);

    /// Parses the range in a range selector, for example for "[1h30m]" the function parses "1h30m".
    static bool tryParseSelectorRange(std::string_view input,
                                      UInt32 timestamp_scale,
                                      DurationType & res_range,
                                      String * error_message = nullptr,
                                      size_t * error_pos = nullptr);

    /// Parses the range and optionally the step in a subquery, for example for "[1h:5m]" the function parses "1h" and "5m".
    static bool tryParseSubqueryRange(std::string_view input,
                                      UInt32 timestamp_scale,
                                      DurationType & res_range,
                                      std::optional<DurationType> & res_step,
                                      String * error_message = nullptr,
                                      size_t * error_pos = nullptr);
};

}
