#pragma once


namespace DB
{

struct TimeSeriesColumnNames
{
    /// The "samples" table contains time series:
    static constexpr const char * ID = "id";
    static constexpr const char * Timestamp = "timestamp";
    static constexpr const char * Value = "value";

    /// The "tags" table contains identifiers for each combination of a metric name with corresponding tags (labels):

    /// The default expression specified for the "id" column contains an expression for calculating an identifier of a time series by a metric name and tags.
    //static constexpr const char * kID = "id";
    static constexpr const char * MetricName = "metric_name";

    /// Contains all tags, including `__name__` and tags with columns from the `tags_to_columns` setting (older tables
    /// store only non-column tags without the metric name, so reading must handle both cases).
    static constexpr const char * Tags = "tags";

    /// Contains the time range of a time series.
    static constexpr const char * MinTime = "min_time";
    static constexpr const char * MaxTime = "max_time";

    /// The optional "histograms" table contains native histogram samples; `id` and `timestamp` are shared
    /// with the "samples" table layout, the remaining columns mirror the Prometheus native histogram model.
    static constexpr const char * Flags = "flags";
    static constexpr const char * Schema = "schema";
    static constexpr const char * ZeroThreshold = "zero_threshold";
    static constexpr const char * Count = "count";
    static constexpr const char * Sum = "sum";
    static constexpr const char * ZeroCount = "zero_count";
    static constexpr const char * PositiveSpans = "positive_spans";
    static constexpr const char * PositiveValues = "positive_values";
    static constexpr const char * NegativeSpans = "negative_spans";
    static constexpr const char * NegativeValues = "negative_values";
    static constexpr const char * CustomValues = "custom_values";

    /// The outer column of a TimeSeries table with a "histograms" target: an array of histogram samples per row.
    static constexpr const char * Histograms = "histograms";

    /// The "metrics" table contains general information (metadata) about metrics:
    static constexpr const char * MetricFamily = "metric_family";
    static constexpr const char * Type = "type";
    static constexpr const char * Unit = "unit";
    static constexpr const char * Help = "help";

    /// Columns returned by the table function prometheusQuery().
    /// The function can also output columns `tags`, `value`, and `timestamp`.
    static constexpr const char * TimeSeries = "time_series";

    /// The column with native histogram samples returned by prometheusQuery() together with `value`/`time_series`
    /// when the query result carries native histograms.
    static constexpr const char * Histogram = "histogram";
    static constexpr const char * HistogramSeries = "histogram_series";

    /// Internal columns used by steps of prometheus query evaluation.
    /// The function prometheusQuery() doesn't output them.
    static constexpr const char * Group = "group";
    static constexpr const char * NewGroup = "new_group";
    static constexpr const char * OriginalGroup = "original_group";
    static constexpr const char * JoinGroup = "join_group";
    static constexpr const char * JoinPresence = "join_presence";
    static constexpr const char * Values = "values";
    static constexpr const char * HistogramValues = "histogram_values";
    static constexpr const char * SampleKinds = "sample_kinds";
    /// Aliases of the series reconstructed from the corresponding grid columns by `timeSeriesFromGrid`
    /// when `last_over_time` resamples a combined grid (e.g. from a subquery) onto the outer grid.
    static constexpr const char * HistogramTimeSeries = "histogram_time_series";
    static constexpr const char * SampleKindsTimeSeries = "sample_kinds_time_series";
    static constexpr const char * IsHistogram = "is_histogram";
    static constexpr const char * SelectedGroups = "selected_groups";
    static constexpr const char * StepsMask = "steps_mask";

    /// Old names kept for compatibility:

    /// The old name of the "metric_family" column, still used in the "metrics" target table.
    static constexpr const char * MetricFamilyName = "metric_family_name";

    /// Older tables fill it ephemerally with all tags except `__name__`, so their identifiers for the same time series can differ.
    static constexpr const char * AllTags = "all_tags";
};

}
