#pragma once

#include <base/types.h>


namespace DB
{

class PrometheusQueryTree;

/// Converts a parsed PromQL query to the JSON representation of its AST which the Prometheus HTTP API
/// endpoint "/api/v1/parse_query" returns (the same format as the `translateAST` function in Prometheus).
/// Throws an exception if the query calls a function or an aggregation operator with a wrong number
/// of arguments or with arguments of wrong types, mirroring the checks the Prometheus parser does.
String prometheusQueryTreeToJSON(const PrometheusQueryTree & promql_query);

}
