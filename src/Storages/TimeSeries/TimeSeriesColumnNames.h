#pragma once


namespace DB
{

struct TimeSeriesColumnNames
{
    /// The "data" table contains time series:
    static constexpr const char * ID = "id";
    static constexpr const char * Timestamp = "timestamp";
    static constexpr const char * Value = "value";

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
    static constexpr const char * MetricFamilyName = "metric_family_name";
    static constexpr const char * Type = "type";
    static constexpr const char * Unit = "unit";
    static constexpr const char * Help = "help";
};

}
