#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionTimestamp.h>

#include <Common/Exception.h>
#include <Core/DecimalFunctions.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    void checkArgumentTypes(const PQT::Function * function_node, const std::vector<SQLQueryPiece> & arguments, const ConverterContext & context)
    {
        const auto & function_name = function_node->function_name;

        if (arguments.size() != 1)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects {} arguments, but was called with {} arguments",
                            function_name, 1, arguments.size());
        }

        const auto & argument = arguments[0];
        if (argument.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Function '{}' expects an argument of type {}, but expression {} has type {}",
                            function_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(argument, context), argument.type);
        }
    }

    ASTPtr makeTimeGridAST(const SQLQueryPiece & argument, const ConverterContext & context)
    {
        return makeASTFunction(
            "CAST",
            makeASTFunction(
                "timeSeriesRange",
                timeSeriesTimestampToAST(argument.start_time, context.timestamp_data_type),
                timeSeriesTimestampToAST(argument.end_time, context.timestamp_data_type),
                timeSeriesDurationToAST(argument.step, context.timestamp_data_type)),
            make_intrusive<ASTLiteral>(fmt::format("Array({})", context.scalar_data_type->getName())));
    }

    SQLQueryPiece makeScalarLikeTimestampResult(
        const PQT::Function * function_node,
        const SQLQueryPiece & argument,
        ConverterContext & context)
    {
        SQLQueryPiece res{function_node, function_node->result_type, argument.store_method};
        res.start_time = argument.start_time;
        res.end_time = argument.end_time;
        res.step = argument.step;
        res.metric_name_dropped = argument.metric_name_dropped;

        if (argument.start_time == argument.end_time)
        {
            res.store_method = StoreMethod::CONST_SCALAR;
            res.scalar_value = DecimalUtils::convertTo<Float64>(argument.start_time, context.timestamp_scale);
            return dropMetricName(std::move(res), context);
        }

        SelectQueryBuilder builder;
        builder.select_list.push_back(makeTimeGridAST(argument, context));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        res.store_method = StoreMethod::SCALAR_GRID;
        res.select_query = builder.getSelectQuery();
        return dropMetricName(std::move(res), context);
    }
}


bool isFunctionTimestamp(std::string_view function_name)
{
    return function_name == "timestamp";
}


SQLQueryPiece applyFunctionTimestamp(const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(function_node, arguments, context);

    auto & argument = arguments[0];
    SQLQueryPiece res{function_node, function_node->result_type, argument.store_method};
    res.start_time = argument.start_time;
    res.end_time = argument.end_time;
    res.step = argument.step;
    res.metric_name_dropped = argument.metric_name_dropped;

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        {
            res.store_method = StoreMethod::EMPTY;
            return res;
        }

        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        {
            /// Scalar-like instant vectors have a sample at every evaluation step.
            return makeScalarLikeTimestampResult(function_node, argument, context);
        }

        case StoreMethod::SCALAR_GRID:
        {
            SelectQueryBuilder builder;
            builder.select_list.push_back(makeTimeGridAST(argument, context));
            builder.select_list.back()->setAlias(ColumnNames::Values);

            res.store_method = StoreMethod::SCALAR_GRID;
            res.select_query = builder.getSelectQuery();
            return dropMetricName(std::move(res), context);
        }

        case StoreMethod::VECTOR_GRID:
        {
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});

            SelectQueryBuilder builder;
            builder.from_table = context.subqueries.back().name;
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            /// timestamp(v) returns the evaluation timestamp only where v has a sample.
            /// Keep NULL positions so the normal vector-grid finalizer preserves PromQL sparsity.
            builder.select_list.push_back(makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("value"), make_intrusive<ASTIdentifier>("timestamp")),
                    makeASTFunction(
                        "if",
                        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("value")),
                        make_intrusive<ASTIdentifier>("timestamp"),
                        make_intrusive<ASTLiteral>(Field{} /* NULL */))),
                make_intrusive<ASTIdentifier>(ColumnNames::Values),
                makeTimeGridAST(argument, context)));
            builder.select_list.back()->setAlias(ColumnNames::Values);

            res.store_method = StoreMethod::VECTOR_GRID;
            res.select_query = builder.getSelectQuery();
            return dropMetricName(std::move(res), context);
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            throwUnexpectedStoreMethod(argument, context);
        }
    }

    UNREACHABLE();
}

}
