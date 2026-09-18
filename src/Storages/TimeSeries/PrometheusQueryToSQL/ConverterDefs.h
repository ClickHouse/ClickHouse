#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>


namespace DB::PrometheusQueryToSQL
{
    using Node = PrometheusQueryTree::Node;
    using NodeType = PrometheusQueryTree::NodeType;
    using ScalarType = PrometheusQueryTree::ScalarType;
    using TimestampType = PrometheusQueryTree::TimestampType;
    using DurationType = PrometheusQueryTree::DurationType;

    using ResultType = PrometheusQueryResultType;

    using ColumnNames = TimeSeriesColumnNames;
    constexpr const char * kMetricName = TimeSeriesTagNames::MetricName;

    /// Replaces one complete PromQL subtree with a query-scoped native fragment.
    /// The fragment is exposed to the SQL analyzer as an ephemeral external table.
    struct NativeFragmentDescription
    {
        const Node * node = nullptr;
        String table_name;
    };
}


namespace DB
{
    struct PrometheusQueryEvaluationSettings;
}
