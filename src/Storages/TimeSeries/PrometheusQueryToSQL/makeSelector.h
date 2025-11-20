#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

struct ConverterContext;

/// Makes a SQL query to read from an instant selector, for example
/// http_requests{job="prometheus"}
SQLQueryPiece makeSelector(const PQT::InstantSelector * instant_selector_node, ConverterContext & context);

/// Makes a SQL query to read from a range selector, for example
/// http_requests{job="prometheus"}[20m]
SQLQueryPiece makeSelector(const PQT::RangeSelector * range_selector_node, ConverterContext & context);

}
