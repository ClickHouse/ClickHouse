#include <Storages/TimeSeries/PrometheusQueryToSQL/applyAggregationOperatorQuantile.h>

#include <Core/ServerSettings.h>
#include <Interpreters/Context.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForAggregationOperator.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>

#include <cmath>
#include <limits>


namespace DB::ServerSetting
{
    extern const ServerSettingsUInt64 aggregate_function_group_array_max_element_size;
}

namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for the `quantile` aggregation operator.
    void checkArgumentTypes(
        const PrometheusQueryTree::AggregationOperator * operator_node,
        const std::vector<SQLQueryPiece> & arguments,
        const ConverterContext & context)
    {
        const auto & operator_name = operator_node->operator_name;

        if (arguments.size() != 2)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects 2 arguments, but was called with {} arguments",
                            operator_name, arguments.size());
        }

        const auto & phi_arg = arguments[0];

        if (phi_arg.type != ResultType::SCALAR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects first argument of type {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR,
                            getPromQLText(phi_arg, context), phi_arg.type);
        }

        const auto & vector_arg = arguments[1];

        if (vector_arg.type != ResultType::INSTANT_VECTOR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects second argument of type {}, but expression {} has type {}",
                            operator_name, ResultType::INSTANT_VECTOR,
                            getPromQLText(vector_arg, context), vector_arg.type);
        }
    }

    /// Converts the quantile parameter phi to an AST expression usable in SQL.
    ASTPtr getPhi(SQLQueryPiece && phi_arg, ConverterContext & context)
    {
        switch (phi_arg.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                return timeSeriesScalarToAST(phi_arg.scalar_value);
            }
            case StoreMethod::SINGLE_SCALAR:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(phi_arg.select_query), SQLSubqueryType::SCALAR});
                auto subquery_id = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
                /// Wrap with assumeNotNull() because scalar subqueries make their result nullable,
                /// but StoreMethod::SINGLE_SCALAR always means one row.
                return makeASTFunction("assumeNotNull", std::move(subquery_id));
            }
            case StoreMethod::SCALAR_GRID:
            {
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "Aggregation operator 'quantile' with a non-constant scalar parameter is not supported");
            }
            default:
            {
                throwUnexpectedStoreMethod(phi_arg, context);
            }
        }
    }

    /// Calculates the quantile at each step like Prometheus: NaN sorts before every other value,
    /// and between two samples the result is `lower * (1 - weight) + upper * weight`.
    ASTPtr makeQuantileForEach(const ASTPtr & phi, SelectQueryBuilder & builder, ASTPtr phi_in_range = nullptr)
    {
        auto id = [](const String & name) -> ASTPtr { return make_intrusive<ASTIdentifier>(name); };

        /// The limit of series per group and step is the size limit of groupArray.
        const auto & server_settings = Context::getGlobalContextInstance()->getServerSettings();
        UInt64 max_series = server_settings[ServerSetting::aggregate_function_group_array_max_element_size];
        UInt64 max_kept = (max_series == std::numeric_limits<UInt64>::max()) ? max_series : max_series + 1;

        /// groupArrayForEach(max_series + 1)(table.values): one value more than the limit shows a step over it.
        /// The column is qualified so that HAVING does not bind `values` to the result alias.
        auto step_values = [&]
        {
            return addParametersToAggregateFunction(
                makeASTFunction("groupArrayForEach", make_intrusive<ASTIdentifier>(Strings{builder.from_table, ColumnNames::Values})),
                make_intrusive<ASTLiteral>(max_kept));
        };

        /// Throws if a group has too many series at one step:
        /// HAVING throwIf(arrayExists(s -> length(s) > max_series, step_values), 'message') = 0
        String message = fmt::format(
            "PromQL aggregation operator 'quantile' got more than {} series in one group at one step, "
            "see the server setting aggregate_function_group_array_max_element_size", max_series);
        ASTPtr too_many_series = makeASTFunction("arrayExists",
            makeASTLambda({"s"},
                makeASTFunction("greater", makeASTFunction("length", id("s")), make_intrusive<ASTLiteral>(max_series))),
            step_values());
        /// A runtime phi outside [0, 1] gives a constant like a literal one, so the limit does not apply to it.
        if (phi_in_range)
            too_many_series = makeASTFunction("and", std::move(phi_in_range), std::move(too_many_series));
        builder.having = makeASTFunction("equals",
            makeASTFunction("throwIf", std::move(too_many_series), make_intrusive<ASTLiteral>(message)),
            make_intrusive<ASTLiteral>(0u));

        /// arrayMap(s -> arrayRotateRight(arraySort(s), arrayCount(x -> isNaN(x), s)), step_values)
        ASTPtr sorted_values = makeASTFunction("arrayMap",
            makeASTLambda({"s"},
                makeASTFunction("arrayRotateRight",
                    makeASTFunction("arraySort", id("s")),
                    makeASTFunction("arrayCount", makeASTLambda({"x"}, makeASTFunction("isNaN", id("x"))), id("s")))),
            step_values());

        /// rank = phi * (length(t) - 1), lower = floor(rank), weight = rank - lower
        auto rank = [&]
        {
            return makeASTFunction("multiply",
                phi->clone(),
                makeASTFunction("minus", makeASTFunction("length", id("t")), make_intrusive<ASTLiteral>(1u)));
        };
        auto lower = [&] { return makeASTFunction("floor", rank()); };
        auto weight = [&] { return makeASTFunction("minus", rank(), lower()); };
        auto element = [&](UInt64 offset)
        {
            ASTPtr index = makeASTFunction("plus", makeASTFunction("toUInt64", lower()), make_intrusive<ASTLiteral>(offset));
            return makeASTFunction("arrayElement", id("t"), makeASTFunction("least", std::move(index), makeASTFunction("length", id("t"))));
        };

        /// arrayMap(t -> if(empty(t), NULL, t[lower + 1] * (1 - weight) + t[least(lower + 2, length(t))] * weight), sorted_values)
        ASTPtr value = makeASTFunction("plus",
            makeASTFunction("multiply", element(1), makeASTFunction("minus", make_intrusive<ASTLiteral>(1.), weight())),
            makeASTFunction("multiply", element(2), weight()));

        return makeASTFunction("arrayMap",
            makeASTLambda({"t"},
                makeASTFunction("if", makeASTFunction("empty", id("t")), make_intrusive<ASTLiteral>(Field{} /* NULL */), std::move(value))),
            std::move(sorted_values));
    }
}


SQLQueryPiece applyAggregationOperatorQuantile(
    const PrometheusQueryTree::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    checkArgumentTypes(operator_node, arguments, context);

    auto & phi_arg = arguments[0];
    auto & vector_arg = arguments[1];

    /// If either argument is empty then the result is also empty.
    if (phi_arg.store_method == StoreMethod::EMPTY || vector_arg.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};

    vector_arg = toVectorGrid(std::move(vector_arg), context);

    /// PromQL evaluates `quantile` with an out-of-range or NaN parameter to a constant
    /// (with a warning) instead of failing the query:
    ///   phi < 0  -> -Inf for every group
    ///   phi > 1  -> +Inf for every group
    ///   phi NaN  -> NaN for every group
    /// quantileExactInclusive() would throw an exception for such phi, so if a constant phi
    /// is out of range we emit a constant-valued array aligned to the time grid instead
    /// (the same way `histogram_quantile` handles an out-of-range phi). A runtime scalar phi
    /// (e.g. `scalar(vector(-0.5))`, StoreMethod::SINGLE_SCALAR) gets the same treatment at
    /// runtime, see below.
    bool const_phi_out_of_range = (phi_arg.store_method == StoreMethod::CONST_SCALAR)
        && (std::isnan(phi_arg.scalar_value) || (phi_arg.scalar_value < 0) || (phi_arg.scalar_value > 1));

    auto res = vector_arg;
    res.node = operator_node;

    /// Step 1: aggregate over series, using `new_group` as an intermediate alias to avoid
    /// ambiguity with the input `group` column when the alias and the source column share the same name.
    ASTPtr aggregation_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(vector_arg.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        ASTPtr new_group = transformGroupASTForAggregationOperator(
            operator_node, make_intrusive<ASTIdentifier>(ColumnNames::Group), /*drop_metric_name=*/true, res.metric_name_dropped);

        builder.select_list.push_back(std::move(new_group));
        builder.select_list.back()->setAlias(ColumnNames::NewGroup);

        ASTPtr quantile_expr;
        if (const_phi_out_of_range)
        {
            Float64 out_of_range_value = std::numeric_limits<Float64>::quiet_NaN();
            if (std::isnan(phi_arg.scalar_value))
                out_of_range_value = std::numeric_limits<Float64>::quiet_NaN();
            else if (phi_arg.scalar_value < 0)
                out_of_range_value = -std::numeric_limits<Float64>::infinity();
            else
                out_of_range_value = std::numeric_limits<Float64>::infinity();

            /// arrayMap(x -> if(isNotNull(x), <constant>, NULL), anyForEach(values))
            /// anyForEach produces one array per group aligned to the time grid, with NULL at
            /// positions where no input series had data. arrayMap then replaces every non-NULL
            /// position with the constant and keeps NULLs as-is.
            quantile_expr = makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                    makeASTFunction("if",
                        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")),
                        make_intrusive<ASTLiteral>(out_of_range_value),
                        make_intrusive<ASTLiteral>(Field{} /* NULL */))),
                makeASTFunction("anyForEach", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        }
        else if (phi_arg.store_method == StoreMethod::SINGLE_SCALAR)
        {
            /// The value of a runtime scalar phi (e.g. `scalar(vector(-0.5))`) is not known here,
            /// so the out-of-range check has to happen at runtime: the phi parameter passed to
            /// the aggregate function is clamped to [0, 1] to keep the aggregate function from
            /// throwing (a scalar subquery is still a constant at analysis time, so it can be
            /// used as a parameter), and then every non-NULL element of the result is replaced
            /// with the Prometheus constant if phi turns out to be out of range or NaN:
            ///
            /// arrayMap(x -> if(isNotNull(x),
            ///                  multiIf(isNaN(phi), nan, phi < 0, -inf, phi > 1, inf, x),
            ///                  NULL),
            ///          makeQuantileForEach(least(greatest(phi, 0.), 1.)))
            ///
            /// least() and greatest() clamp a NaN phi to a valid value too, which one doesn't
            /// matter because the result is replaced with NaN anyway.
            ASTPtr phi = getPhi(std::move(phi_arg), context);

            ASTPtr clamped_phi = makeASTFunction("least",
                makeASTFunction("greatest", phi->clone(), make_intrusive<ASTLiteral>(0.)),
                make_intrusive<ASTLiteral>(1.));

            /// phi >= 0 AND phi <= 1, which is false for a NaN phi.
            ASTPtr phi_in_range = makeASTFunction("and",
                makeASTFunction("greaterOrEquals", phi->clone(), make_intrusive<ASTLiteral>(0.)),
                makeASTFunction("lessOrEquals", phi->clone(), make_intrusive<ASTLiteral>(1.)));

            ASTPtr substituted_element = makeASTFunction("multiIf",
                makeASTFunction("isNaN", phi->clone()),
                make_intrusive<ASTLiteral>(std::numeric_limits<Float64>::quiet_NaN()),
                makeASTFunction("less", phi->clone(), make_intrusive<ASTLiteral>(0.)),
                make_intrusive<ASTLiteral>(-std::numeric_limits<Float64>::infinity()),
                makeASTFunction("greater", phi->clone(), make_intrusive<ASTLiteral>(1.)),
                make_intrusive<ASTLiteral>(std::numeric_limits<Float64>::infinity()),
                make_intrusive<ASTIdentifier>("x"));

            quantile_expr = makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                    makeASTFunction("if",
                        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")),
                        std::move(substituted_element),
                        make_intrusive<ASTLiteral>(Field{} /* NULL */))),
                makeQuantileForEach(clamped_phi, builder, std::move(phi_in_range)));
        }
        else
        {
            quantile_expr = makeQuantileForEach(getPhi(std::move(phi_arg), context), builder);
        }
        builder.select_list.push_back(std::move(quantile_expr));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        if (operator_node->by || operator_node->without)
            builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));

        aggregation_query = builder.getSelectQuery();
    }

    /// Step 2: rename `new_group` back to `group` and drop empty grids (a WHERE, so `values` cannot bind to the input column
    /// under prefer_column_name_to_alias).
    {
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(aggregation_query), SQLSubqueryType::TABLE});

        SelectQueryBuilder builder;
        builder.from_table = context.subqueries.back().name;
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::NewGroup));
        builder.select_list.back()->setAlias(ColumnNames::Group);
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
        builder.where = makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(ColumnNames::Values));

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
