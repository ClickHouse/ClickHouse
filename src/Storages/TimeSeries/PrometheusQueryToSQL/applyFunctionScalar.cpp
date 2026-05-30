#include <Storages/TimeSeries/PrometheusQueryToSQL/applyFunctionScalar.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/Prometheus/stepsInTimeSeriesRange.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
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
}


SQLQueryPiece applyFunctionScalar(
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    chassert(isFunctionScalar(function_name));

    checkArgumentTypes(function_node, arguments, context);
    auto & argument = arguments[0];

    auto res = argument;
    res.node = function_node;
    res.type = ResultType::SCALAR;

    switch (argument.store_method)
    {
        case StoreMethod::EMPTY:
        case StoreMethod::CONST_SCALAR:
        case StoreMethod::SINGLE_SCALAR:
        case StoreMethod::SCALAR_GRID:
        {
            /// These store methods are already compatible with scalars, so we do nothing here.
            return res;
        }

        case StoreMethod::VECTOR_GRID:
        {
            /// If the argument contains time series we have to do some aggregation.
            SelectQueryBuilder builder;

            if (argument.start_time == argument.end_time)
            {
                /// SELECT if(count(values[1]) = 1, assumeNotNull(any(values[1])), NaN) AS value
                /// FROM <vector_grid>
                builder.select_list.push_back(makeASTFunction(
                    "if",
                    makeASTFunction(
                        "equals",
                        makeASTFunction(
                            "count",
                            makeASTFunction(
                                "arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u))),
                        make_intrusive<ASTLiteral>(1)),
                    makeASTFunction(
                        "assumeNotNull",
                        makeASTFunction(
                            "any",
                            makeASTFunction(
                                "arrayElement", make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTLiteral>(1u)))),
                    timeSeriesScalarToAST(std::numeric_limits<Float64>::quiet_NaN(), context.scalar_data_type)));

                builder.select_list.back()->setAlias(ColumnNames::Value);
                res.store_method = StoreMethod::SINGLE_SCALAR;
            }
            else
            {
                /// SELECT arrayMap(x, y -> if(x = 1, assumeNotNull(y), NaN),
                ///                 if(empty(counts), arrayResize(CAST([], 'Array(UInt64)'), <count_of_time_steps>, CAST(0, 'UInt64')), counts),
                ///                 if(empty(any_values), arrayResize(CAST([], 'Array(Nullable(scalar_data_type))'), <count_of_time_steps>, CAST(NaN, 'Nullable(scalar_data_type)')), any_values)) AS values
                /// FROM <vector_grid>
                auto counts = makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values));
                auto any_values = makeASTFunction("anyForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values));
                auto count_of_time_steps = make_intrusive<ASTLiteral>(stepsInTimeSeriesRange(argument.start_time, argument.end_time, argument.step));
                auto nan = timeSeriesScalarToAST(std::numeric_limits<Float64>::quiet_NaN(), context.scalar_data_type);

                auto empty_counts = makeASTFunction("CAST", make_intrusive<ASTLiteral>(Array{}), make_intrusive<ASTLiteral>("Array(UInt64)"));
                auto empty_values = makeASTFunction(
                    "CAST",
                    make_intrusive<ASTLiteral>(Array{}),
                    make_intrusive<ASTLiteral>(fmt::format("Array(Nullable({}))", context.scalar_data_type->getName())));
                auto nullable_nan = makeASTFunction(
                    "CAST",
                    nan->clone(),
                    make_intrusive<ASTLiteral>(fmt::format("Nullable({})", context.scalar_data_type->getName())));

                auto counts_or_zero_counts = makeASTFunction(
                    "if",
                    makeASTFunction("empty", counts->clone()),
                    makeASTFunction(
                        "arrayResize", std::move(empty_counts), count_of_time_steps->clone(), makeASTFunction("CAST", make_intrusive<ASTLiteral>(0), make_intrusive<ASTLiteral>("UInt64"))),
                    std::move(counts));

                auto any_values_or_nans = makeASTFunction(
                    "if",
                    makeASTFunction("empty", any_values->clone()),
                    makeASTFunction("arrayResize", std::move(empty_values), count_of_time_steps->clone(), std::move(nullable_nan)),
                    std::move(any_values));

                builder.select_list.push_back(makeASTFunction(
                    "arrayMap",
                    makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                        makeASTFunction(
                            "if",
                            makeASTFunction("equals", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(1)),
                            makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>("y")),
                            std::move(nan))),
                    std::move(counts_or_zero_counts),
                    std::move(any_values_or_nans)));

                builder.select_list.back()->setAlias(ColumnNames::Values);
                res.store_method = StoreMethod::SCALAR_GRID;
            }

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;
            res.select_query = builder.getSelectQuery();

            return res;
        }

        case StoreMethod::CONST_STRING:
        case StoreMethod::RAW_DATA:
        {
            /// Can't get in here because these store methods are incompatible with the allowed argument types
            /// (see checkArgumentTypes()).
            throwUnexpectedStoreMethod(argument, context);
        }
    }

    UNREACHABLE();
}

}
