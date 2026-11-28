#pragma once

#include <Storages/TimeSeries/PrometheusQueryToSQL/SQLQueryPiece.h>


namespace DB::PrometheusQueryToSQL
{

/// Checks of the arguments of a PromQL function or operator.
/// `node` is the function or the operator called with `arguments`; error messages name it.
/// All checks throw `CANNOT_EXECUTE_PROMQL_QUERY`.

/// Checks that the number of arguments is `expected_count`.
void checkNumberOfArguments(const PrometheusQueryTree::Node * node, const std::vector<SQLQueryPiece> & arguments, size_t expected_count);

/// Checks that the number of arguments is from `min_count` to `max_count`, both included.
void checkNumberOfArgumentsBetween(
    const PrometheusQueryTree::Node * node, const std::vector<SQLQueryPiece> & arguments, size_t min_count, size_t max_count);

/// Checks that the number of arguments is `min_count` or more.
void checkNumberOfArgumentsAtLeast(const PrometheusQueryTree::Node * node, const std::vector<SQLQueryPiece> & arguments, size_t min_count);

/// Checks that the argument at `index`, counted from 0, has the expected type.
void checkArgumentType(
    const PrometheusQueryTree::Node * node,
    const std::vector<SQLQueryPiece> & arguments,
    size_t index,
    ResultType expected_type,
    const ConverterContext & context);

}
