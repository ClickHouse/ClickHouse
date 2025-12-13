#pragma once

#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{
struct ConverterContext;

/// How data is stored in a QueryPiece.
enum class StoreMethod
{
    /// No data.
    /// Can be used with types ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    EMPTY,

    /// A const scalar value stored in `QueryPiece::scalar_value`.
    /// CONST_SCALAR is produced by a float literal in a prometheus query.
    /// Can be used with types ResultType::SCALAR, ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    CONST_SCALAR,

    /// A const string value stored in `QueryPiece::string_value`.
    /// CONST_STRING is produced by a string literal in a prometheus query.
    /// Can be used only with type ResultType::STRING.
    CONST_STRING,

    /// Data are stored in one row and one column named `values` (array of floating-point values).
    /// The values are aligned to the time grid.
    /// SCALAR_GRID is produced by functions returning a scalar, for example scalar().
    /// Can be used with types ResultType::SCALAR, ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    SCALAR_GRID,

    /// Data are stored in two columns `group` (UInt64), `values` (array of nullable floating-point values).
    /// Values of each row are aligned to the time grid. Each value of `group` can appear only once in the output.
    /// GRID is produced by functions like last_over_time() or rate() in a prometheus query.
    /// Can be used with types ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    VECTOR_GRID,

    /// Data are stored in three columns `group` (UInt64), `timestamp` (timestamp_type), 'value` (floating point).
    /// RAW_DATA is produced by selectors in a prometheus query.
    /// Can be used only with type ResultType::RANGE_VECTOR.
    RAW_DATA,
};


/// Represents a part of a promql query prepared to execute as an SQL query.
struct SQLQueryPiece
{
    SQLQueryPiece(const Node * node_, ResultType type_, StoreMethod store_method_)
        : node(node_), type(type_), store_method(store_method_) {}

    const Node * node;
    ResultType type;
    StoreMethod store_method;

    /// Operators and functions drop the metric name, i.e. the tag named '__name__.
    bool metric_name_dropped = false;

    /// Fields `start_time`, `end_time`, `step` are set only if `store_method` is one of [CONST_SCALAR, CONST_STRING, GRID].
    /// If `store_method` is RAW_DATA then these fields are not set.
    /// If `store_method` is CONST_STRING then `start_time` is always equal to `end_time`.
    DecimalField<DateTime64> start_time;
    DecimalField<DateTime64> end_time;
    DecimalField<Decimal64> step;

    /// Field `scalar_value` is set only if `store_method` is CONST_SCALAR.
    Float64 scalar_value;

    /// Field `string_value` is set only if `store_method` is CONST_STRING.
    String string_value;

    /// Field `select_query` is set to a SELECT query only if `store_method` is one of [GRID, RAW_DATA].
    /// If `store_method` is GRID then the SELECT query returns two columns "group" (UInt64), "values" (Array(Nullable(floating_point_type))).
    /// If `store_method` is RAW_DATA then the SELECT query returns three columns "group" (UInt64), "timestamp" (timestamp_type), "value" (floating_point_type).
    /// If `store_method` is CONST_SCALAR or CONST_STRING then field `select_query` is not set.
    ASTPtr select_query;
};

std::string_view getPromQLQuery(const SQLQueryPiece & query_piece, const ConverterContext & context);

}
