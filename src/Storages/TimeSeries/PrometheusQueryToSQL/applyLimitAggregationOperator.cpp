#include <Storages/TimeSeries/PrometheusQueryToSQL/applyLimitAggregationOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForAggregationOperator.h>
#include <cmath>
#include <limits>
#include <unordered_map>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for a limit aggregation operator.
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

        const auto & k_arg = arguments[0];

        if (k_arg.type != ResultType::SCALAR)
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Aggregation operator '{}' expects first argument of type {}, but expression {} has type {}",
                            operator_name, ResultType::SCALAR,
                            getPromQLText(k_arg, context), k_arg.type);
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

    /// Converts the `k` argument from ScalarType to UInt64:
    ///  - negative values (including -Inf) are clamped to 0
    ///  - NaN, +Inf, and values exceeding the UInt64 range cause an exception
    UInt64 convertScalarToK(ScalarType scalar)
    {
        if (std::isnan(scalar))
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Argument k of aggregation operator must not be NaN");
        if (scalar > static_cast<ScalarType>(std::numeric_limits<UInt64>::max()))
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY, "Argument k of aggregation operator is too large: {}", scalar);
        return (scalar < 0) ? 0 : static_cast<UInt64>(scalar);
    }

    /// Converts the `k` argument from ScalarType to UInt64.
    /// This is SQL version of the function convertScalarToK() taking a ScalarType.
    ASTPtr convertScalarToK(ASTPtr scalar)
    {
        /// accurateCast(floor(if(x < 0, 0, x)), 'UInt64')
        /// Pre-clamping negatives is important so that negative values (including -Inf) do not trigger an exception.
        /// For NaN and +Inf, `x < 0` is false, so the value is passed through to `accurateCast`
        /// which throws on values that don't fit into UInt64.
        auto clamped = makeASTFunction("if",
            makeASTFunction("less", scalar, make_intrusive<ASTLiteral>(0.0)),
            make_intrusive<ASTLiteral>(0.0),
            scalar->clone());
        auto floored = makeASTFunction("floor", std::move(clamped));
        return makeASTFunction("accurateCast", std::move(floored), make_intrusive<ASTLiteral>("UInt64"));
    }

    /// Converts the k parameter to an AST usable as the `k` argument of timeSeries*Masks: a UInt64 scalar expression or a scalar subquery returning Array(UInt64) aligned to the time grid.
    ASTPtr getK(SQLQueryPiece && k_arg, ConverterContext & context)
    {
        switch (k_arg.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                return make_intrusive<ASTLiteral>(convertScalarToK(k_arg.scalar_value));
            }
            case StoreMethod::SINGLE_SCALAR:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(k_arg.select_query), SQLSubqueryType::SCALAR});
                auto subquery_id = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
                /// Wrap with assumeNotNull() because scalar subqueries make their result nullable,
                /// but StoreMethod::SINGLE_SCALAR always means one row.
                auto assumed = makeASTFunction("assumeNotNull", std::move(subquery_id));
                return convertScalarToK(std::move(assumed));
            }
            case StoreMethod::SCALAR_GRID:
            {
                /// SELECT arrayMap(x -> convertScalarToK(x), values) AS values FROM <scalar_grid>
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(k_arg.select_query), SQLSubqueryType::TABLE});
                String inner_subquery_name = context.subqueries.back().name;

                SelectQueryBuilder builder;
                builder.from_table = inner_subquery_name;
                builder.select_list.push_back(makeASTFunction("arrayMap",
                    makeASTLambda({"x"}, convertScalarToK(make_intrusive<ASTIdentifier>("x"))),
                    make_intrusive<ASTIdentifier>(ColumnNames::Values)));
                builder.select_list.back()->setAlias(ColumnNames::Values);

                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), builder.getSelectQuery(), SQLSubqueryType::SCALAR});
                return make_intrusive<ASTIdentifier>(context.subqueries.back().name);
            }
            default:
            {
                throwUnexpectedStoreMethod(k_arg, context);
            }
        }
    }

    struct ImplInfo
    {
        /// The aggregate function selecting which series to keep at each time step (see Step 1 below).
        const char * aggregate_function_name;

        /// Whether `timeSeriesGroupToSamplingKey(group)` should be passed to the aggregate function to provide deterministic "pseudo-random" sampling for `limitk`.
        bool use_sampling_keys = false;
    };

    const ImplInfo * getImplInfo(std::string_view operator_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"topk", {"timeSeriesTopKMasks", /* use_sampling_keys = */ false}},
            {"bottomk", {"timeSeriesBottomKMasks", /* use_sampling_keys = */ false}},
            {"limitk", {"timeSeriesLimitKMasks", /* use_sampling_keys = */ true}},
        };

        auto it = impl_map.find(operator_name);
        if (it == impl_map.end())
            return nullptr;
        return &it->second;
    }
}


bool isLimitAggregationOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


SQLQueryPiece applyLimitAggregationOperator(
    const PrometheusQueryTree::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
{
    const auto & operator_name = operator_node->operator_name;

    const ImplInfo * impl_info = getImplInfo(operator_name);
    chassert(impl_info);

    checkArgumentTypes(operator_node, arguments, context);

    auto & k_arg = arguments[0];
    auto & vector_arg = arguments[1];

    /// If either argument is empty then the result is also empty.
    if (k_arg.store_method == StoreMethod::EMPTY || vector_arg.store_method == StoreMethod::EMPTY)
        return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};

    vector_arg = toVectorGrid(std::move(vector_arg), context);

    ASTPtr k = getK(std::move(k_arg), context);

    auto res = vector_arg;
    res.node = operator_node;

    /// The vector grid becomes a named subquery because Steps 1 and 3 both read it: recomputing the grid twice keeps every step of this plan streaming, never collecting all series into one row.
    context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(vector_arg.select_query), SQLSubqueryType::TABLE});
    String vector_grid = context.subqueries.back().name;

    /// Step 1: choose up to k series to keep at each time step within each aggregation group.
    ///
    ///   SELECT timeSeries<TopK|BottomK|LimitK>Masks(<k>, group[, timeSeriesGroupToSamplingKey(group)], values) AS selected_groups
    ///   FROM <vector_grid>
    ///   [GROUP BY <by_tags_expr>]
    ///
    /// `selected_groups` is Array(Tuple(key UInt64, steps_mask Array(UInt8))); the sampling key (added for limitk) ranks series by a hash of their tags regardless of row read order.
    ASTPtr step1_query;
    {
        SelectQueryBuilder builder;
        builder.from_table = vector_grid;

        ASTPtr aggregate_function;
        if (impl_info->use_sampling_keys)
            aggregate_function = makeASTFunction(impl_info->aggregate_function_name,
                std::move(k),
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                makeASTFunction("timeSeriesGroupToSamplingKey", make_intrusive<ASTIdentifier>(ColumnNames::Group)),
                make_intrusive<ASTIdentifier>(ColumnNames::Values));
        else
            aggregate_function = makeASTFunction(impl_info->aggregate_function_name,
                std::move(k),
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                make_intrusive<ASTIdentifier>(ColumnNames::Values));

        builder.select_list.push_back(std::move(aggregate_function));
        builder.select_list.back()->setAlias(ColumnNames::SelectedGroups);

        if (operator_node->by || operator_node->without)
        {
            bool metric_name_dropped_from_group = vector_arg.metric_name_dropped;
            ASTPtr by_tags_expr = transformGroupASTForAggregationOperator(
                operator_node, make_intrusive<ASTIdentifier>(ColumnNames::Group), /*drop_metric_name=*/true, metric_name_dropped_from_group);
            builder.group_by.push_back(std::move(by_tags_expr));
        }

        step1_query = builder.getSelectQuery();
    }

    /// Step 2: unfold `selected_groups` into one row per kept series.
    ///
    ///   SELECT (arrayJoin(selected_groups) AS p).1 AS join_group,
    ///          p.2 AS steps_mask
    ///   FROM step1
    ///
    /// A series belongs to exactly one aggregation group, so each kept series produces exactly one row here.
    ASTPtr step2_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        auto array_join_expr = makeASTFunction("arrayJoin", make_intrusive<ASTIdentifier>(ColumnNames::SelectedGroups));
        array_join_expr->setAlias("p");

        builder.select_list.push_back(makeASTFunction("tupleElement", array_join_expr, make_intrusive<ASTLiteral>(1u)));
        builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

        builder.select_list.push_back(makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("p"), make_intrusive<ASTLiteral>(2u)));
        builder.select_list.back()->setAlias(ColumnNames::StepsMask);

        step2_query = builder.getSelectQuery();
    }

    /// Step 3: keep only the chosen series and mask their values at non-chosen time steps with NULLs.
    ///
    ///   SELECT group,
    ///          arrayMap((x, m) -> if(m, x, NULL), values, steps_mask) AS values
    ///   FROM <vector_grid>
    ///   ANY INNER JOIN step2 ON group = join_group
    ///
    /// `steps_mask` is passed to `arrayMap` as a second array rather than captured by the lambda, because captured columns are replicated per array element.
    {
        SelectQueryBuilder builder;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

        builder.select_list.push_back(makeASTFunction("arrayMap",
            makeASTLambda({"x", "m"},
                makeASTFunction("if",
                    make_intrusive<ASTIdentifier>("m"),
                    make_intrusive<ASTIdentifier>("x"),
                    make_intrusive<ASTLiteral>(Field{} /* NULL */))),
            make_intrusive<ASTIdentifier>(ColumnNames::Values),
            make_intrusive<ASTIdentifier>(ColumnNames::StepsMask)));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        /// Reading the grid a second time here is correct because every vector construct is deterministic within one query (per-query group ids, deterministic tie-breaking).
        builder.from_table = vector_grid;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step2_query), SQLSubqueryType::TABLE});
        builder.join_table = context.subqueries.back().name;
        builder.join_kind = JoinKind::Inner;
        /// `join_group` values are unique (each series is selected by exactly one aggregation group), so ANY INNER JOIN cannot duplicate series rows.
        builder.join_strictness = JoinStrictness::Any;
        builder.join_on = makeASTFunction("equals",
            make_intrusive<ASTIdentifier>(ColumnNames::Group),
            make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
