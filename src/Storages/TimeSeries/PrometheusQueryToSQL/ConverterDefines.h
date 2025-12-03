#pragma once

#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB::PrometheusQueryToSQL
{
    using ResultType = PrometheusQueryResultType;
    using PQT = PrometheusQueryTree;
    using Node = PQT::Node;
    using NodeType = PQT::NodeType;
    using ColumnNames = TimeSeriesColumnNames;
}
