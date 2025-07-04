#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB
{
/// Helper utilities for parsing prometheus queries. These utilities are used by PrometheusQueryTree.
struct PrometheusQueryParsingUtil
{
public:
    using TimestampType = PrometheusQueryTree::TimestampType;
    using OffsetType = PrometheusQueryTree::OffsetType;
    using ScalarType = PrometheusQueryTree::ScalarType;

    /// Parses a prometheus query.
    /// Scale `timestamp_scale` is used to parse decimals representing timestamps and offsets.
    static bool parseQuery(std::string_view input, UInt32 timestamp_scale, PrometheusQueryTree & result, String & error_message, size_t & error_pos);

    /// Converts a quoted string literal to its unquoted version: "abc" -> abc
    /// Accepts an input string in quotes or double quotes or backticks, and also handles escape sequences
    /// according to the PromQL rules (see https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals).
    static bool parseStringLiteral(std::string_view input, String & res_string, String & error_message, size_t & error_pos);

    /// Parses a scalar literal which can be either anm integer number or a floating-point number (e.g. 237e6), or Inf, or Nan,
    /// or a hexadecimal number (e.g. 0xA7CD).
    /// Also underscores (_) can be used in between decimal or hexadecimal digits and they don't mean anything.
    static bool parseScalar(std::string_view input, ScalarType & res_scalar, String & error_message, size_t & error_pos);

    /// Parses an offset which can be written after the "offset" keyword (e.g. "offset 5m").
    /// The function supports both the number format and the duration format ("offset -20.5" and "offset 0x10" are also valid).
    static bool parseOffset(std::string_view input, UInt32 scale, OffsetType & res_offset, String & error_message, size_t & error_pos);

    /// Parses a timestamp which can be written after '@' character (e.g. "@ 1609746000").
    /// The function supports both the number format and the duration format ("@ 55y" is valid).
    static bool parseTimestamp(std::string_view input, UInt32 scale, TimestampType & res_timestamp, String & error_message, size_t & error_pos);

    /// Parses a time range which is used in range selectors, for example "[1h30m]".
    /// The function supports both the number format and the duration format.
    static bool parseTimeRange(std::string_view input, UInt32 scale, OffsetType & res_range, String & error_message, size_t & error_pos);

    /// Parses a time range with an optional resolution which are used in subqueries, for example "[1h:5m]" or "[1h:]".
    static bool parseSubqueryRange(std::string_view input, UInt32 scale, OffsetType & res_range, std::optional<OffsetType> & res_resolution, String & error_message, size_t & error_pos);

    /// Returns true if a string contains time units, i.e. characters 'y', 'w', 'd', 'h', 'm', 's', 'ms'.
    static bool containsTimeUnits(std::string_view input);
};

}
