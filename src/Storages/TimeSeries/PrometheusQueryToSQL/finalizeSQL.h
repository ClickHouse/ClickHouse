#pragma once

#include <Parsers/IAST_fwd.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Finalizes an AST built to execute a prometheus query.
/// The function makes a SQL query returning either:
/// 1. Two columns named "timestamp", "value" (where the "value" column contain a floating-point number), one row
///    (if the result of the prometheus query is SCALAR).
/// 2. Two columns named "timestamp", "value" (where the "value" column contain a string), one row
///    (if the result of the prometheus query is STRING).
/// 3. Three columns named "tags", "timestamp", "value" (if the result of the prometheus query is INSTANT VECTOR).
/// 4. Two columns named "tags", "time_series" (if the result of the prometheus query is RANGE VECTOR).
ASTPtr finalizeSQL(SQLQueryPiece && result, ConverterContext & context);

}
