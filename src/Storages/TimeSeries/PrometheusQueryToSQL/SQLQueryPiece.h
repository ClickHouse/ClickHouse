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
    /// Can be used with any types.
    EMPTY,

    /// A const scalar value stored in `SQLQueryPiece::scalar_value`.
    /// CONST_SCALAR is produced by a float literal in a prometheus query.
    /// Can be used with types ResultType::SCALAR, ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    CONST_SCALAR,

    /// A const string value stored in `SQLQueryPiece::string_value`.
    /// CONST_STRING is produced by a string literal in a prometheus query.
    /// Can be used only with type ResultType::STRING.
    CONST_STRING,

    /// A single scalar is stored in one row and one column named `value` (Float64).
    /// Can be used with types ResultType::SCALAR, ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    SINGLE_SCALAR,

    /// Data are stored in one row and one column named `values` (Array(Float64)).
    /// The values are aligned to the time grid.
    /// SCALAR_GRID is produced by functions returning a scalar, for example scalar().
    /// Can be used with types ResultType::SCALAR, ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    SCALAR_GRID,

    /// Data are stored in two columns:
    /// - `group` (UInt64),
    /// - `values` (Array(Nullable(Float64))).
    /// Values of each row are aligned to the time grid. Each value of `group` can appear only once in the output.
    /// VECTOR_GRID is produced by functions like last_over_time() or rate() in a prometheus query.
    /// Can be used with types ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    VECTOR_GRID,

    /// Data are stored in three columns:
    /// - `group` (UInt64),
    /// - `timestamp` (the type of the timestamps in the TimeSeries table,
    ///   or `ConverterContext::result_timestamp_type` after applying an offset),
    /// - `value` (Float64 or Float32, the type of the values in the table).
    /// The columns keep the types they have in the table because raw data can be big: the aggregate functions accept
    /// any of these types, and the result is converted to `ConverterContext::result_timestamp_type` and Float64 later.
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
    ResultType type = ResultType::SCALAR;
    StoreMethod store_method = StoreMethod::EMPTY;

    /// Whether the tag named '__name__' is known to be absent from the `group` column (and no dropped-name marker either).
    /// Operators and functions which drop the metric name usually don't remove '__name__' right away, they only mark
    /// the series with `kDroppedMetricNameMarker` (see dropMetricName) and leave this flag `false`;
    /// the tag is removed from the final result by finalizeSQL.
    bool metric_name_dropped = false;

    /// `start_time`, `end_time`, `step` are used only if `store_method` is one of
    /// [CONST_SCALAR, CONST_STRING, SCALAR_GRID, VECTOR_GRID].
    /// They use the scale `ConverterContext::result_timestamp_scale`.
    /// If `store_method` is CONST_STRING then `start_time` is always equal to `end_time`.
    /// If `store_method` is RAW_DATA then these fields are not used.
    TimestampType start_time = {};
    TimestampType end_time = {};
    DurationType step = {};

    /// `scalar_value` is used only if `store_method` is CONST_SCALAR.
    ScalarType scalar_value = {};

    /// `string_value` is used only if `store_method` is CONST_STRING.
    String string_value;

    /// `select_query` is used only if `store_method` is one of [SINGLE_SCALAR, SCALAR_GRID, VECTOR_GRID, RAW_DATA].
    /// If `store_method` is SINGLE_SCALAR then the SELECT query outputs one column `value` (Float64) with a single row.
    /// If `store_method` is SCALAR_GRID then the SELECT query outputs one column `values` (Array(Float64)) with a single row.
    /// If `store_method` is VECTOR_GRID then the SELECT query outputs two columns `group` (UInt64), `values` (Array(Nullable(Float64))).
    /// If `store_method` is RAW_DATA then the SELECT query outputs three columns `group` (UInt64), `timestamp`, `value`
    /// (see the comment for StoreMethod::RAW_DATA for their types).
    /// If `store_method` is CONST_SCALAR or CONST_STRING then the SELECT query is not used.
    ASTPtr select_query;
};

String getPromQLText(const SQLQueryPiece & query_piece, const ConverterContext & context);

/// Called when the store method can't be handled because it's incompatible with the type of `query_piece`.
[[noreturn]] void throwUnexpectedStoreMethod(const SQLQueryPiece & query_piece, const ConverterContext & context);

}
