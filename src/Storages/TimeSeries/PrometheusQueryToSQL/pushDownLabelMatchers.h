#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{

/// Copies label matchers across binary operators for the labels they match by, so `b / on(job) a{job="x"}` reads only `b{job="x"}`.
std::shared_ptr<const PrometheusQueryTree> pushDownLabelMatchers(std::shared_ptr<const PrometheusQueryTree> promql_tree);

}
