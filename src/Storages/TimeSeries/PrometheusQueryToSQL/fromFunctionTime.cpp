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

SQLQueryPiece fromFunctionTime(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionTime(function_name));

    if (!arguments.empty())
    {
        throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                        "Function '{}' expects no arguments, but was called with {} arguments",
                        function_name, arguments.size());
    }

    auto node_range = context.node_range_getter.get(function_node);
    if (node_range.empty())
        return SQLQueryPiece{function_node, ResultType::SCALAR, StoreMethod::EMPTY};

    if (node_range.start_time == node_range.end_time)
    {
        /// Single evaluation time, so we use StoreMethod::CONST_SCALAR.
        SQLQueryPiece res{function_node, ResultType::SCALAR, StoreMethod::CONST_SCALAR};
        res.start_time = node_range.start_time;
        res.end_time = node_range.end_time;
        res.step = node_range.step;
        res.scalar_value = DecimalUtils::convertTo<Float64>(node_range.start_time, context.timestamp_scale);
        return res;
    }
    else
    {
        /// Range of evaluation times (e.g. "time()[10m:1m]"), so we use StoreMethod::SCALAR_GRID.
        SQLQueryPiece res{function_node, ResultType::SCALAR, StoreMethod::SCALAR_GRID};
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

}
