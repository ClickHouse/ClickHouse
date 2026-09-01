#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionTime.h>

#include <Core/DecimalFunctions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

SQLQueryPiece makeTimeQueryPiece(const PrometheusQueryTree::Node * node, ConverterContext & context)
{
    auto node_range = context.node_range_getter.get(node);
    if (node_range.empty())
        return SQLQueryPiece{node, ResultType::SCALAR, StoreMethod::EMPTY};

    if (node_range.start_time == node_range.end_time)
    {
        /// Single evaluation time, so we use StoreMethod::CONST_SCALAR.
        SQLQueryPiece res{node, ResultType::SCALAR, StoreMethod::CONST_SCALAR};
        res.start_time = node_range.start_time;
        res.end_time = node_range.end_time;
        res.step = node_range.step;
        res.scalar_value = DecimalUtils::convertTo<Float64>(node_range.start_time, context.timestamp_scale);
        return res;
    }
    else
    {
        /// Range of evaluation times (e.g. "time()[10m:1m]"), so we use StoreMethod::SCALAR_GRID.
        SQLQueryPiece res{node, ResultType::SCALAR, StoreMethod::SCALAR_GRID};
        res.start_time = node_range.start_time;
        res.end_time = node_range.end_time;
        res.step = node_range.step;

        SelectQueryBuilder builder;

        builder.select_list.push_back(makeASTFunction(
            "CAST",
            makeASTFunction(
                "timeSeriesRange",
                timeSeriesTimestampToAST(node_range.start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(node_range.end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(node_range.step, context.timestamp_data_type)),
            make_intrusive<ASTLiteral>(fmt::format("Array({})", context.scalar_data_type->getName()))));

        builder.select_list.back()->setAlias(ColumnNames::Values);
        res.select_query = builder.getSelectQuery();

        return res;
    }
}


SQLQueryPiece makeTimeQueryPieceNative(const PrometheusQueryTree::Node * node, ConverterContext & context)
{
    auto node_range = context.node_range_getter.get(node);
    if (node_range.empty())
        return SQLQueryPiece{node, ResultType::SCALAR, StoreMethod::EMPTY};

    if (node_range.start_time == node_range.end_time)
    {
        /// Single evaluation time. Unlike makeTimeQueryPiece()'s CONST_SCALAR branch, return it via a one-row
        /// subquery (StoreMethod::SINGLE_SCALAR) keeping the value typed as `context.timestamp_data_type`,
        /// instead of converting it to a Float64 scalar_value (which a caller would then have to cast to
        /// `context.scalar_data_type`, losing precision if that's Float32).
        SQLQueryPiece res{node, ResultType::SCALAR, StoreMethod::SINGLE_SCALAR};
        res.start_time = node_range.start_time;
        res.end_time = node_range.end_time;
        res.step = node_range.step;

        SelectQueryBuilder builder;
        builder.select_list.push_back(timeSeriesTimestampToAST(node_range.start_time, context.timestamp_data_type));
        builder.select_list.back()->setAlias(ColumnNames::Value);
        res.select_query = builder.getSelectQuery();

        return res;
    }
    else
    {
        /// Range of evaluation times. Unlike makeTimeQueryPiece()'s SCALAR_GRID branch, don't cast the result of
        /// timeSeriesRange() down to Array(scalar_data_type); keep its native return type Array(timestamp_data_type)
        /// so no precision is lost before a caller extracts a calendar component from each timestamp.
        SQLQueryPiece res{node, ResultType::SCALAR, StoreMethod::SCALAR_GRID};
        res.start_time = node_range.start_time;
        res.end_time = node_range.end_time;
        res.step = node_range.step;

        SelectQueryBuilder builder;
        builder.select_list.push_back(makeASTFunction(
            "timeSeriesRange",
            timeSeriesTimestampToAST(node_range.start_time, context.timestamp_data_type),
            timeSeriesTimestampToAST(node_range.end_time, context.timestamp_data_type),
            timeSeriesDurationToAST(node_range.step, context.timestamp_data_type)));

        builder.select_list.back()->setAlias(ColumnNames::Values);
        res.select_query = builder.getSelectQuery();

        return res;
    }
}


SQLQueryPiece fromFunctionTime(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionTime(function_name));

    if (!arguments.empty())
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects no arguments, but was called with {} arguments",
                        function_name, arguments.size());
    }

    return makeTimeQueryPiece(function_node, context);
}

}
