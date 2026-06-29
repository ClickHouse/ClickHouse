#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>


namespace DB::PrometheusQueryToSQL
{
    using PQT = PrometheusQueryTree;
    using Node = PQT::Node;
    using NodeType = PQT::NodeType;
    using ScalarType = PQT::ScalarType;
    using TimestampType = PQT::TimestampType;
    using DurationType = PQT::DurationType;

    using ResultType = PrometheusQueryResultType;

    using ColumnNames = TimeSeriesColumnNames;
    constexpr const char * kMetricName = TimeSeriesTagNames::MetricName;
}


namespace DB
{
    struct PrometheusQueryEvaluationSettings;
}
