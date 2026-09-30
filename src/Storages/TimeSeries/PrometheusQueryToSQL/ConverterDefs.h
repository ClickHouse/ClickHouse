#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>

#include <vector>


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
    /// Each fragment is exposed to the SQL analyzer as an ephemeral external table.
    struct NativeFragmentDescription
    {
        const Node * node = nullptr;
        String table_name;
        bool metric_name_dropped = false;
    };

    inline constexpr size_t MAX_NATIVE_FRAGMENTS = 2;
    using NativeFragmentDescriptions = std::vector<NativeFragmentDescription>;
}


namespace DB
{
    struct PrometheusQueryEvaluationSettings;
}
