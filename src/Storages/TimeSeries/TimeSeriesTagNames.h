#pragma once


namespace DB
{

/// Label names with special meaning.
struct TimeSeriesTagNames
{
    static constexpr const char * MetricName = "__name__";

    /// Internal marker that a PromQL operator or function has dropped the metric name of a series.
    /// The tag `__name__` itself stays in the group until the final result is built, so that functions like
    /// label_replace can still read it and series with different names stay distinct in intermediate results.
    /// The name is reserved for the PromQL converter and rejected on insert, in `tags_to_columns`, and in PromQL label names.
    static constexpr const char * DroppedMetricNameMarker = "__name__.dropped";
    static constexpr const char * DroppedMetricNameMarkerValue = "1";
};

}
