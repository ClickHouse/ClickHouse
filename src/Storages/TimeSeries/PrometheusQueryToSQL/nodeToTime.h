#pragma once

#include <Core/Field.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{

/// Extracts a timestamp from a node of type either PQT::ScalarLiteral or PQT::ScalarLiteral.
DecimalField<DateTime64> nodeToTime(const Node * scalar_or_duration_node, UInt32 default_scale);

/// Extracts a interval from a node of type either PQT::ScalarLiteral or PQT::ScalarLiteral.
DecimalField<Decimal64> nodeToDuration(const Node * scalar_or_duration_node, UInt32 default_scale);

}
