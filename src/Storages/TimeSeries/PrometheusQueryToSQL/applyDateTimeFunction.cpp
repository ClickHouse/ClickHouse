#include <Storages/TimeSeries/PrometheusQueryToSQL/applyDateTimeFunction.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
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
    /// Checks if the types of the specified arguments are valid for a date/time function.
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
    const PQT::Function * function_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & function_name = function_node->function_name;
    const auto * impl_info = getImplInfo(function_name);
    chassert(impl_info);

    checkArgumentTypes(function_node, arguments, context);

    auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
    {
        /// f(toDateTime64(x, 0, 'UTC'))::scalar_data_type
        chassert(args.size() == 1);
        ASTPtr x = std::move(args[0]);
        return timeSeriesScalarASTCast(
            (impl_info->transform_ast)(
                makeASTFunction("toDateTime64", std::move(x), make_intrusive<ASTLiteral>(0u), make_intrusive<ASTLiteral>("UTC"))),
            context.scalar_data_type);
    };

    auto res = applySimpleFunction(function_node, context, apply_function_to_ast, std::move(arguments));
    return dropMetricName(std::move(res), context);
}

}
