#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>


namespace DB::PrometheusQueryToSQL
{
    using ResultType = PrometheusQueryResultType;
    using PQT = PrometheusQueryTree;
    using Node = PQT::Node;
    using NodeType = PQT::NodeType;
    using ColumnNames = TimeSeriesColumnNames;
    
    constexpr const char * kMetricName = TimeSeriesTagNames::MetricName;
}


namespace DB
{
    struct PrometheusQueryEvaluationSettings;
}
