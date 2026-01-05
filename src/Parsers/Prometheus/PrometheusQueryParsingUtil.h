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
    using DurationType = PrometheusQueryTree::DurationType;

    /// Contains either a scalar or a duration.
    struct ScalarOrDuration
    {
        ScalarOrDuration() = default;
        explicit ScalarOrDuration(ScalarType scalar_) : scalar(scalar_) {}
        explicit ScalarOrDuration(DurationType duration_) : duration(duration_) {}
        std::optional<ScalarType> scalar;
        std::optional<DurationType> duration;
        bool empty() const { return !scalar && !duration; }
        void negate();
    };

    /// Parses a scalar or literal which can be either an integer number or a floating-point number (e.g. 237e6), or Inf, or Nan,
    /// or a hexadecimal number (e.g. 0xA7CD), or a duration with time units, for example 1m30s.
    /// Also underscores (_) can be used in between decimal or hexadecimal digits and they don't mean anything.
    static bool parseScalarOrDuration(std::string_view input, ScalarOrDuration & result, String & error_message, size_t & error_pos);

    /// Finds a time range in a range selector, for example for "[1h30m]" the function returns "1h30m".
    static bool findTimeRange(std::string_view input, std::string_view & res_range, String & error_message, size_t & error_pos);

    /// Finds a range and optionally a resolution in a subquery, for example for "[1h:5m]" the function returns "1h" and "5m".
    static bool findSubqueryRangeAndResolution(std::string_view input, std::string_view & res_range, std::string_view & res_resolution,
                                               String & error_message, size_t & error_pos);
};

}
