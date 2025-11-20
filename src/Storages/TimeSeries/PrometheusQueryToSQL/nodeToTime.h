#pragma once

#include <Core/Field.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>


namespace DB::PrometheusQueryToSQL
{

/// Extracts a timestamp from a node of type either PrometheusQueryTree::ScalarLiteral or PrometheusQueryTree::ScalarLiteral.
DecimalField<DateTime64> nodeToTime(const PrometheusQueryTree::Node * scalar_or_interval_node, UInt32 default_scale);

/// Extracts a interval from a node of type either PrometheusQueryTree::ScalarLiteral or PrometheusQueryTree::ScalarLiteral.
DecimalField<Decimal64> nodeToDuration(const PrometheusQueryTree::Node * scalar_or_interval_node, UInt32 default_scale);

}
