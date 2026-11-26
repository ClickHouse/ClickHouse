#include <Storages/TimeSeries/PrometheusQueryToSQL/applyDateTimeFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/checkArguments.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/fromFunctionTime.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>

#include <limits>

namespace DB::PrometheusQueryToSQL
{

namespace
{
    using TransformASTFunc = ASTPtr (*)(ASTPtr t);

    struct ImplInfo
    {
        TransformASTFunc transform_ast;
    };

    const ImplInfo * getImplInfo(std::string_view function_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"day_of_week",
             {
                 /// Returned values should be from 0 to 6, where 0 means Sunday.
                 [](ASTPtr t) -> ASTPtr
                 { return makeASTFunction("toDayOfWeek", std::move(t), /* mode = */ make_intrusive<ASTLiteral>(2u)); },
             }},

            {"day_of_month",
             {
                 /// Returned values should be from 1 to 31.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toDayOfMonth", std::move(t)); },
             }},

            {"days_in_month",
             {
                 /// Returned values should be from 28 to 31.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toDaysInMonth", std::move(t)); },
             }},

            {"day_of_year",
             {
                 /// Returned values should be from 1 to 365 for non-leap years, and 1 to 366 in leap years.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toDayOfYear", std::move(t)); },
             }},

            {"minute",
             {
                 /// Returned values should be from 0 to 59.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toMinute", std::move(t)); },
             }},

            {"hour",
             {
                 /// Returned values should be from 0 to 23.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toHour", std::move(t)); },
             }},

            {"month",
             {
                 /// Returned values should be from 1 to 12, where 1 means January.
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toMonth", std::move(t)); },
             }},

            {"year",
             {
                 [](ASTPtr t) -> ASTPtr { return makeASTFunction("toYear", std::move(t)); },
             }},
        };

        auto it = impl_map.find(function_name);
        if (it == impl_map.end())
            return nullptr;

        return &it->second;
    }
}


bool isDateTimeFunction(std::string_view function_name)
{
    return getImplInfo(function_name) != nullptr;
}


SQLQueryPiece applyDateTimeFunction(
    const PrometheusQueryTree::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    checkNumberOfArgumentsBetween(function_node, arguments, 0, 1);
    if (arguments.empty())
    {
        /// A date/time function called without arguments acts as if it was called with `vector(time())`.
        auto time_argument = fromEvaluationTime(function_node, context);
        time_argument.type = ResultType::INSTANT_VECTOR;
        arguments.push_back(std::move(time_argument));
    }
    else
    {
        checkArgumentType(function_node, arguments, 0, ResultType::INSTANT_VECTOR, context);
    }

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        /// multiIf(isNull(x), x, isFinite(x), f(toDateTime64(ifNotFinite(x, 0), 0, 'UTC'))::Float64, nan)
        /// NULLs (i.e. time steps without a value) are kept as is, and like in Prometheus a NaN or infinite value gives NaN.
        /// `toDateTime64` throws on a non-finite argument, so `ifNotFinite` keeps it safe even without short-circuit evaluation.
        chassert(args.size() == 1);
        ASTPtr x = std::move(args[0]);
        ASTPtr date_part = timeSeriesScalarASTCast(
            (impl_info->transform_ast)(
                makeASTFunction("toDateTime64",
                    makeASTFunction("ifNotFinite", x->clone(), timeSeriesScalarToAST(0)),
                    make_intrusive<ASTLiteral>(0u),
                    make_intrusive<ASTLiteral>("UTC"))));
        return makeASTFunction(
            "multiIf",
            makeASTFunction("isNull", x->clone()),
            x->clone(),
            makeASTFunction("isFinite", x->clone()),
            std::move(date_part),
            timeSeriesScalarToAST(std::numeric_limits<Float64>::quiet_NaN()));
    };

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
