#pragma once


namespace DB
{

struct TimeSeriesColumnNames
{
    /// The "samples" table contains time series:
    static constexpr const char * ID = "id";
    static constexpr const char * Timestamp = "timestamp";
    static constexpr const char * Value = "value";

    /// Whether this sample is a Prometheus "stale marker" (see PrometheusRemoteWriteProtocol.cpp's
    /// `isPrometheusStaleMarker`). Such rows are kept (instead of being dropped at ingest) so that a bare
    /// PromQL instant selector can still see exactly where a series went stale and stop its lookback there,
    /// while every numeric `_over_time` function simply excludes them (see fromSelector.cpp).
    static constexpr const char * IsStaleMarker = "is_stale_marker";

    /// Internal column used only while evaluating a bare instant selector (see fromSelector.cpp): whether
    /// the most recent row (real sample or marker) within the selector's window is a stale marker. Deliberately
    /// named differently from `IsStaleMarker` above (which is a per-row flag from the "samples" table) - the two
    /// are read together in the same SELECT, and reusing one name for both would make `IsStaleMarker` resolve to
    /// this column's own alias instead of the source column.
    static constexpr const char * WinningRowIsStaleMarker = "winning_row_is_stale_marker";

    /// The "tags" table contains identifiers for each combination of a metric name with corresponding tags (labels):

    /// The default expression specified for the "id" column contains an expression for calculating an identifier of a time series by a metric name and tags.
    //static constexpr const char * kID = "id";
    static constexpr const char * MetricName = "metric_name";

    /// Contains tags which have no corresponding columns specified in the "tags_to_columns" setting.
    static constexpr const char * Tags = "tags";

    /// Contains all tags, including those ones which have corresponding columns specified in the "tags_to_columns" setting.
    /// This is a generated column, it's not stored anywhere, it's generated on the fly.
    static constexpr const char * AllTags = "all_tags";

    /// Contains the time range of a time series.
    static constexpr const char * MinTime = "min_time";
    static constexpr const char * MaxTime = "max_time";

    /// The "metrics" table contains general information (metadata) about metrics:
    static constexpr const char * MetricFamily = "metric_family";
    static constexpr const char * Type = "type";
    static constexpr const char * Unit = "unit";
    static constexpr const char * Help = "help";

    /// Columns returned by the table function prometheusQuery().
    /// The function can also output columns `tags`, `value`, and `timestamp`.
    static constexpr const char * TimeSeries = "time_series";

    /// Internal columns used by steps of prometheus query evaluation.
    /// The function prometheusQuery() doesn't output them.
    static constexpr const char * Group = "group";
    static constexpr const char * NewGroup = "new_group";
    static constexpr const char * Groups = "groups";
    static constexpr const char * Indices = "indices";
    static constexpr const char * MaskedValues = "masked_values";
    static constexpr const char * OriginalGroup = "original_group";
    static constexpr const char * JoinGroup = "join_group";
    static constexpr const char * JoinCount = "join_count";
    static constexpr const char * Values = "values";
    static constexpr const char * SamplingKeys = "sampling_keys";

    /// Obsolete names
    static constexpr const char * MetricFamilyName = "metric_family_name";
};

}
