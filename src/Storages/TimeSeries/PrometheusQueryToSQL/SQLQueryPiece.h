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

    /// A const scalar value stored in `SQLQueryPiece::scalar_value`; produced by a float literal.
    /// Can be used with types ResultType::SCALAR, ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    CONST_SCALAR,

    /// A const string value stored in `SQLQueryPiece::string_value`; produced by a string literal.
    /// Can be used only with type ResultType::STRING.
    CONST_STRING,

    /// A single scalar is stored in one row and one column named `value` (floating-point).
    /// Can be used with types ResultType::SCALAR, ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    SINGLE_SCALAR,

    /// One row with a `values` array (floating-point) aligned to the time grid; produced by scalar-returning functions like scalar.
    /// Can be used with types ResultType::SCALAR, ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    SCALAR_GRID,

    /// Columns `group` (UInt64), `values` (array of nullable floating-point values) aligned to the time grid; each `group` appears once.
    /// Produced by functions like last_over_time or rate. Can be used with types ResultType::INSTANT_VECTOR, ResultType::RANGE_VECTOR.
    VECTOR_GRID,

    /// Columns `group` (UInt64), `timestamp` (timestamp_data_type), `value` (scalar_data_type); produced by selectors.
    /// Can be used only with type ResultType::RANGE_VECTOR.
    RAW_DATA,

    /// 15 columns: `group`, `timestamp`, `value`, 11 histogram payload columns (getTimeSeriesHistogramPayloadColumns), `is_histogram`.
    /// A row is a float sample (default payload) or a histogram sample (dummy `value`). Can be used only with ResultType::RANGE_VECTOR.
    HISTOGRAM_RAW_DATA,

    /// Columns `group` (UInt64), `values`, `histogram_values`, `sample_kinds`: three equal-length arrays aligned to one time grid.
    /// `sample_kinds` = per-step winning kind (NULL, 0=float, 1=histogram; ties keep the histogram) masking the two arms.
    HISTOGRAM_GRID,
};


/// Represents a part of a prometheus query prepared to execute as an SQL query: we build SQLQueryPieces
/// for the nodes of the PrometheusQueryTree and convert the root piece to SQL by calling finalizeSQL.
struct SQLQueryPiece
{
    SQLQueryPiece(const Node * node_, ResultType type_, StoreMethod store_method_)
        : node(node_), type(type_), store_method(store_method_) {}

    const Node * node = nullptr;
    ResultType type = ResultType::SCALAR;
    StoreMethod store_method = StoreMethod::EMPTY;

    /// Operators and functions drop the metric name, i.e. the tag named '__name__.
    bool metric_name_dropped = false;

    /// `start_time`, `end_time`, `step` are used only for [CONST_SCALAR, CONST_STRING, SCALAR_GRID, VECTOR_GRID, HISTOGRAM_GRID]
    /// (for CONST_STRING `start_time` equals `end_time`); unused for RAW_DATA and HISTOGRAM_RAW_DATA.
    TimestampType start_time = {};
    TimestampType end_time = {};
    DurationType step = {};

    /// `scalar_value` is used only if `store_method` is CONST_SCALAR.
    ScalarType scalar_value = {};

    /// `string_value` is used only if `store_method` is CONST_STRING.
    String string_value;

    /// `select_query` is used only for [SINGLE_SCALAR, SCALAR_GRID, VECTOR_GRID, RAW_DATA, HISTOGRAM_RAW_DATA, HISTOGRAM_GRID].
    /// It outputs the columns documented per store method in StoreMethod; unused for CONST_SCALAR and CONST_STRING.
    ASTPtr select_query;
};

String getPromQLText(const SQLQueryPiece & query_piece, const ConverterContext & context);

/// Called when the store method can't be handled because it's incompatible with the type of `query_piece`.
[[noreturn]] void throwUnexpectedStoreMethod(const SQLQueryPiece & query_piece, const ConverterContext & context);

}
