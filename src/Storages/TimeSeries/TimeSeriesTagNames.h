#pragma once


namespace DB
{

/// Label names with special meaning.
struct TimeSeriesTagNames
{
    static constexpr const char * MetricName = "__name__";
};

}
