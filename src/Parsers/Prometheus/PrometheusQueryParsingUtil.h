#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB
{
/// Helper utilities for parsing prometheus queries. These utilities are used by PrometheusQueryTree.
struct PrometheusQueryParsingUtil
{
public:
    /// Parses a prometheus query.
    /// Scale `timestamp_scale` is used to parse decimals representing timestamps and offsets.
    static bool parseQuery(std::string_view input, PrometheusQueryTree & result, String & error_message, size_t & error_pos);

    /// Converts a quoted string literal to its unquoted version: "abc" -> abc
    /// Accepts an input string in quotes or double quotes or backticks, and also handles escape sequences
    /// according to the PromQL rules (see https://prometheus.io/docs/prometheus/latest/querying/basics/#string-literals).
    static bool parseStringLiteral(std::string_view input, String & result, String & error_message, size_t & error_pos);

    using ScalarType = PrometheusQueryTree::ScalarType;
    using IntervalType = PrometheusQueryTree::IntervalType;

    /// Contains either a scalar or an interval.
    struct ScalarOrInterval
    {
        ScalarOrInterval() = default;
        explicit ScalarOrInterval(ScalarType scalar_) : scalar(scalar_) {}
        explicit ScalarOrInterval(IntervalType interval_) : interval(interval_) {}
        std::optional<ScalarType> scalar;
        std::optional<IntervalType> interval;
        bool empty() const { return !scalar && !interval; }
        void negate();
    };

    /// Parses a scalar or literal which can be either an integer number or a floating-point number (e.g. 237e6), or Inf, or Nan,
    /// or a hexadecimal number (e.g. 0xA7CD), or an interval with time units, for example 1m30s.
    /// Also underscores (_) can be used in between decimal or hexadecimal digits and they don't mean anything.
    static bool parseScalarOrInterval(std::string_view input, ScalarOrInterval & result, String & error_message, size_t & error_pos);

    /// Finds a time range in a range selector, for example for "[1h30m]" the function returns "1h30m".
    static bool findTimeRange(std::string_view input, std::string_view & res_range, String & error_message, size_t & error_pos);

    /// Finds a range and optionally a resolution in a subquery, for example for "[1h:5m]" the function returns "1h" and "5m".
    static bool findSubqueryRangeAndResolution(std::string_view input, std::string_view & res_range, std::string_view & res_resolution,
                                               String & error_message, size_t & error_pos);
};

}
