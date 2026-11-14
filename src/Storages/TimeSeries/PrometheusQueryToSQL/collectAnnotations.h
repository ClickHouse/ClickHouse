#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

using ConvertNodeFunction = SQLQueryPiece (*)(const Node * node, ConverterContext & context);

/// Adds the annotations of a function or an aggregation operator to `context.annotations` if it's set.
/// `convert_node` converts an argument again to build the probe of an annotation which depends on the data.
void collectAnnotations(
    const Node * node, const std::vector<SQLQueryPiece> & arguments, ConverterContext & context, ConvertNodeFunction convert_node);

/// Repeats the annotations added since `first_annotation` by `left_argument` for an equal `right_argument` which is not converted.
void repeatAnnotationsForEqualArgument(
    const Node * left_argument, const Node * right_argument, size_t first_annotation, ConverterContext & context);

}
