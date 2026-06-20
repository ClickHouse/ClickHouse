#pragma once

#include <Core/Field.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterDefs.h>


namespace DB::PrometheusQueryToSQL
{
struct ConverterContext;

/// Represents how data is stored in a SQLQueryPiece.
enum class StoreMethod
{
    /// No data.
    /// Can be used with types ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    EMPTY,

    /// A const scalar value stored in `SQLQueryPiece::scalar_value`.
    /// CONST_SCALAR is produced by a float literal in a prometheus query.
    /// Can be used with types ResultType::SCALAR, ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    CONST_SCALAR,

    /// A const string value stored in `SQLQueryPiece::string_value`.
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
    /// VECTOR_GRID is produced by functions like last_over_time() or rate() in a prometheus query.
    /// Can be used with types ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    VECTOR_GRID,

    /// Data are stored in three columns `group` (UInt64), `timestamp` (timestamp_data_type), 'value` (scalar_data_type).
    /// RAW_DATA is produced by selectors in a prometheus query.
    /// Can be used only with type ResultType::RANGE_VECTOR.
    RAW_DATA,
};


/// Represents a part of a prometheus query prepared to execute as an SQL query.
/// To execute a prometheus query we build such SQLQueryPieces for the nodes
/// of the corresponding PrometheusQueryTree, we get an SQLQueryPiece for the root node,
/// and then we convert it to SQL by calling the function finalizeSQL().
struct SQLQueryPiece
{
    SQLQueryPiece(const Node * node_, ResultType type_, StoreMethod store_method_)
        : node(node_), type(type_), store_method(store_method_) {}

    const Node * node = nullptr;
    ResultType type = ResultType::RANGE_VECTOR;
    StoreMethod store_method = StoreMethod::EMPTY;

    /// Operators and functions drop the metric name, i.e. the tag named '__name__.
    bool metric_name_dropped = false;

    /// `start_time`, `end_time`, `step` are used only if `store_method` is one of
    /// [CONST_SCALAR, CONST_STRING, SCALAR_GRID, VECTOR_GRID].
    /// If `store_method` is CONST_STRING then `start_time` is always equal to `end_time`.
    /// If `store_method` is RAW_DATA then these fields are not used.
    TimestampType start_time = {};
    TimestampType end_time = {};
    DurationType step = {};

    /// `scalar_value` is used only if `store_method` is CONST_SCALAR.
    ScalarType scalar_value = {};

    /// `string_value` is used only if `store_method` is CONST_STRING.
    String string_value;

    /// `select_query` is used only if `store_method` is one of [SCALAR_GRID, VECTOR_GRID, RAW_DATA].
    /// If `store_method` is SCALAR_GRID then the SELECT query outputs one column `values` (Array(Nullable(scalar_data_type))).
    /// If `store_method` is VECTOR_GRID then the SELECT query outputs two columns `group` (UInt64), `values` (Array(Nullable(scalar_data_type))).
    /// If `store_method` is RAW_DATA then the SELECT query outputs three columns `group` (UInt64), `timestamp` (timestamp_data_type), `value` (scalar_data_type).
    /// If `store_method` is CONST_SCALAR or CONST_STRING then the SELECT query is not used.
    ASTPtr select_query;
};

String getPromQLQuery(const SQLQueryPiece & query_piece, const ConverterContext & context);

}
