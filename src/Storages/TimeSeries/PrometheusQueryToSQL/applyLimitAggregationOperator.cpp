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
    extern const int NOT_IMPLEMENTED;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Checks if the types of the specified arguments are valid for a limit aggregation operator.
    void checkArgumentTypes(
        const PQT::AggregationOperator * operator_node,
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
        /// toUInt64(if(x < 0, 0, x))
        /// Pre-clamping negatives is important so that -Inf does not trigger an exception.
        /// For NaN, `NaN < 0` is false, so the value is passed through to `toUInt64` which throws.
        auto clamped = makeASTFunction("if",
            makeASTFunction("less", scalar, make_intrusive<ASTLiteral>(0.0)),
            make_intrusive<ASTLiteral>(0.0),
            scalar->clone());
        return makeASTFunction("toUInt64", std::move(clamped));
    }

    /// Result of `getK`. If `is_array` is false, `ast` is a UInt64 scalar expression that evaluates
    /// to the same k value at every time step. If `is_array` is true, `ast` is an identifier of a
    /// scalar subquery returning an Array(UInt64) aligned to the time grid.
    struct KArgument
    {
        ASTPtr ast;
        bool is_array;
    };

    /// Converts the k parameter to an AST expression usable in SQL.
    KArgument getK(SQLQueryPiece && k_arg, std::string_view operator_name, ConverterContext & context)
    {
        switch (k_arg.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                return {make_intrusive<ASTLiteral>(convertScalarToK(k_arg.scalar_value)), /* is_array = */ false};
            }
            case StoreMethod::SINGLE_SCALAR:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(k_arg.select_query), SQLSubqueryType::SCALAR});
                auto subquery_id = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
                /// Wrap with assumeNotNull() because scalar subqueries make their result nullable,
                /// but StoreMethod::SINGLE_SCALAR always means one row.
                auto assumed = makeASTFunction("assumeNotNull", std::move(subquery_id));
                return {convertScalarToK(std::move(assumed)), /* is_array = */ false};
            }
            case StoreMethod::SCALAR_GRID:
            {
#if 0
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
                return {make_intrusive<ASTIdentifier>(context.subqueries.back().name), /* is_array = */ true};
#else
                /// TODO: arrayPartialSort(k, arr) doesn't work when k is different in different rows.
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                                "Aggregation operator '{}' with a non-constant scalar parameter is not supported",
                                operator_name);
#endif
            }
            default:
            {
                throwUnexpectedStoreMethod(k_arg, context);
            }
        }
    }

    /// Returns the sort key lambda for `arrayPartialSort` over an index array:
    /// - ascending  : `i -> assumeNotNull(arr[i])`   (used by `bottomk` with arr=v and by `limitk` with arr=sampling_keys)
    /// - descending : `i -> -assumeNotNull(arr[i])`  (used by `topk` with arr=v)
    ASTPtr buildSortKeyLambda(ASTPtr && arr, bool descending)
    {
        chassert(arr);
        auto arr_at_i = makeASTFunction("arrayElement", std::move(arr), make_intrusive<ASTIdentifier>("i"));
        ASTPtr value_key = makeASTFunction("assumeNotNull", std::move(arr_at_i));
        if (descending)
            value_key = makeASTFunction("negate", std::move(value_key));
        return makeASTLambda({"i"}, std::move(value_key));
    }

    /// Returns a function that builds the sort key lambda for a given operator.
    /// `values` is an array of values of different time series at a specific time,
    /// `sampling_keys` is a matching array of sampling keys calculated for each time series.
    /// `sampling_keys` is used only for `limitk`, it's nullptr for `topk` and `bottomk`.
    using BuildSortKeyLambdaFunc = ASTPtr (*)(ASTPtr values, ASTPtr sampling_keys);

    struct ImplInfo
    {
        BuildSortKeyLambdaFunc build_sort_key_lambda;
        /// Whether `timeSeriesGroupToSamplingKey(group)` should be used as `sampling_keys`
        /// to provide deterministic "pseudo-random" sampling for `limitk`.
        bool needs_sampling_keys;
    };

    const ImplInfo * getImplInfo(std::string_view operator_name)
    {
        static const std::unordered_map<std::string_view, ImplInfo> impl_map = {
            {"topk",
             {[](ASTPtr values, ASTPtr /*sampling_keys*/) -> ASTPtr
              { return buildSortKeyLambda(std::move(values), /*descending=*/ true); },
              /*needs_sampling_keys=*/ false}},

            {"bottomk",
             {[](ASTPtr values, ASTPtr /*sampling_keys*/) -> ASTPtr
              { return buildSortKeyLambda(std::move(values), /*descending=*/ false); },
              /*needs_sampling_keys=*/ false}},

            {"limitk",
             {[](ASTPtr /*values*/, ASTPtr sampling_keys) -> ASTPtr
              { return buildSortKeyLambda(std::move(sampling_keys), /*descending=*/ false); },
              /*needs_sampling_keys=*/ true}},
        };

        auto it = impl_map.find(operator_name);
        if (it == impl_map.end())
            return nullptr;
        return &it->second;
    }

    /// Builds an expression returning a sorted list of indices:
    ///
    ///   arraySort(arraySlice(
    ///       arrayPartialSort(sort_key_lambda, k,
    ///           arrayFilter((i, x) -> x IS NOT NULL, arrayEnumerate(values), values)),
    ///       1, k))
    ///
    /// The inner `arrayFilter` drops NULL values; `arrayPartialSort` picks k by `sort_key_lambda`;
    /// `arraySlice` takes exactly k (the partial sort may return more); the outer `arraySort`
    /// re-sorts the k indices numerically so Step 3 can use `indexOfAssumeSorted`.
    ///
    /// TODO: Consider adding new functions `arrayTopK`/`arrayBottomK` to ClickHouse
    /// to simplify this expression.
    ASTPtr buildKIndices(ASTPtr && k, ASTPtr && values, ASTPtr && sort_key_lambda)
    {
        /// arrayFilter((i, x) -> x IS NOT NULL, arrayEnumerate(values), values)
        auto non_null_indices = makeASTFunction("arrayFilter",
            makeASTLambda({"i", "x"}, makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
            makeASTFunction("arrayEnumerate", values->clone()),
            std::move(values));

        return makeASTFunction("arraySort",
            makeASTFunction("arraySlice",
                makeASTFunction("arrayPartialSort",
                    std::move(sort_key_lambda),
                    k->clone(),
                    std::move(non_null_indices)),
                make_intrusive<ASTLiteral>(1u),
                std::move(k)));
    }
}


bool isLimitAggregationOperator(std::string_view operator_name)
{
    return getImplInfo(operator_name) != nullptr;
}


SQLQueryPiece applyLimitAggregationOperator(
    const PQT::AggregationOperator * operator_node, std::vector<SQLQueryPiece> && arguments, ConverterContext & context)
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

    KArgument prepared_k = getK(std::move(k_arg), operator_name, context);
    ASTPtr k = std::move(prepared_k.ast);
    bool k_is_array = prepared_k.is_array;

    auto res = vector_arg;
    res.node = operator_node;

    /// Step 1: collect all series within each aggregation group.
    ///
    ///   SELECT groupArray(group) AS groups,
    ///          arrayTranspose(groupArray(values)) AS values
    ///          [, groupArray(timeSeriesGroupToSamplingKey(group)) AS sampling_keys]  -- for limitk
    ///   FROM vector_grid
    ///   GROUP BY <by_tags_expr>
    ///
    /// `groups` is an array of original series group IDs (length N).
    /// `values` is a TxN matrix: for each time step t, an array of N series values.
    /// `sampling_keys` (added for limitk) is an N-length array aligned with `groups`/`values`,
    /// used by Step 2 to pick k series deterministically regardless of row read order.
    ASTPtr step1_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(vector_arg.select_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(ColumnNames::Group)));
        builder.select_list.back()->setAlias(ColumnNames::Groups);

        builder.select_list.push_back(makeASTFunction("arrayTranspose",
            makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(ColumnNames::Values))));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        if (impl_info->needs_sampling_keys)
        {
            builder.select_list.push_back(makeASTFunction("groupArray",
                makeASTFunction("timeSeriesGroupToSamplingKey", make_intrusive<ASTIdentifier>(ColumnNames::Group))));
            builder.select_list.back()->setAlias(ColumnNames::SamplingKeys);
        }

        if (operator_node->by || operator_node->without)
        {
            bool metric_name_dropped_from_group = vector_arg.metric_name_dropped;
            ASTPtr by_tags_expr = transformGroupASTForAggregationOperator(
                operator_node, make_intrusive<ASTIdentifier>(ColumnNames::Group), /*drop_metric_name=*/true, metric_name_dropped_from_group);
            builder.group_by.push_back(std::move(by_tags_expr));
        }

        step1_query = builder.getSelectQuery();
    }

    /// Step 2: for each time step, compute the selected indices.
    ///
    /// When k is a single scalar:
    ///   SELECT groups, values, arrayMap(v -> <indices_expr>, values) AS indices
    ///   FROM step1
    ///
    /// When k is a scalar grid:
    ///   SELECT groups, values,
    ///       arrayMap((v, j) -> <indices_expr>, values, arrayEnumerate(values)) AS indices
    ///   FROM step1
    ASTPtr step2_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step1_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Groups));
        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));

        ASTPtr sampling_keys;
        if (impl_info->needs_sampling_keys)
            sampling_keys = make_intrusive<ASTIdentifier>(ColumnNames::SamplingKeys);

        auto sort_key_lambda = impl_info->build_sort_key_lambda(
            make_intrusive<ASTIdentifier>("v"),
            std::move(sampling_keys));

        if (k_is_array)
        {
            ASTPtr k_per_step = makeASTFunction("arrayElement", std::move(k), make_intrusive<ASTIdentifier>("j"));
            ASTPtr indices_expr = buildKIndices(std::move(k_per_step), make_intrusive<ASTIdentifier>("v"), std::move(sort_key_lambda));
            builder.select_list.push_back(makeASTFunction("arrayMap",
                makeASTLambda({"v", "j"}, std::move(indices_expr)),
                make_intrusive<ASTIdentifier>(ColumnNames::Values),
                makeASTFunction("arrayEnumerate", make_intrusive<ASTIdentifier>(ColumnNames::Values))));
        }
        else
        {
            ASTPtr indices_expr = buildKIndices(std::move(k), make_intrusive<ASTIdentifier>("v"), std::move(sort_key_lambda));
            builder.select_list.push_back(makeASTFunction("arrayMap",
                makeASTLambda({"v"}, std::move(indices_expr)),
                make_intrusive<ASTIdentifier>(ColumnNames::Values)));
        }

        builder.select_list.back()->setAlias(ColumnNames::Indices);

        step2_query = builder.getSelectQuery();
    }

    /// Step 3: apply the per-time-step mask using `indices`, producing `masked_values` (TxN).
    ///
    ///   SELECT groups,
    ///       arrayMap((v, idx) -> arrayMap((x, i) -> if(indexOfAssumeSorted(idx, i) > 0, x, NULL),
    ///           v, arrayEnumerate(v)),
    ///           values, indices) AS masked_values
    ///   FROM step2
    ASTPtr step3_query;
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step2_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Groups));

        auto inner_lambda = makeASTLambda({"x", "i"},
            makeASTFunction("if",
                makeASTFunction("greater",
                    makeASTFunction("indexOfAssumeSorted",
                        make_intrusive<ASTIdentifier>("idx"),
                        make_intrusive<ASTIdentifier>("i")),
                    make_intrusive<ASTLiteral>(0u)),
                make_intrusive<ASTIdentifier>("x"),
                make_intrusive<ASTLiteral>(Field{} /* NULL */)));

        auto mask_body = makeASTFunction("arrayMap",
            std::move(inner_lambda),
            make_intrusive<ASTIdentifier>("v"),
            makeASTFunction("arrayEnumerate", make_intrusive<ASTIdentifier>("v")));

        builder.select_list.push_back(makeASTFunction("arrayMap",
            makeASTLambda({"v", "idx"}, std::move(mask_body)),
            make_intrusive<ASTIdentifier>(ColumnNames::Values),
            make_intrusive<ASTIdentifier>(ColumnNames::Indices)));
        builder.select_list.back()->setAlias(ColumnNames::MaskedValues);

        step3_query = builder.getSelectQuery();
    }

    /// Step 4: transpose `masked_values` from TxN to NxT, then unzip `groups` and `masked_values`
    /// into individual series rows, discarding series that have no selected values at any time step.
    ///
    /// After step 3, `masked_values` is an array of T inner arrays, each of length N.
    /// `arrayTranspose` converts it to NxT so that `arrayZip` can pair each series group ID
    /// with its T-length values array.
    ///
    ///   SELECT (arrayJoin(arrayZip(groups, arrayTranspose(masked_values))) AS p).1 AS group,
    ///          p.2 AS values
    ///   FROM step3
    ///   WHERE arrayExists(x -> isNotNull(x), p.2)
    {
        SelectQueryBuilder builder;

        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(step3_query), SQLSubqueryType::TABLE});
        builder.from_table = context.subqueries.back().name;

        auto array_join_expr = makeASTFunction("arrayJoin",
            makeASTFunction("arrayZip",
                make_intrusive<ASTIdentifier>(ColumnNames::Groups),
                makeASTFunction("arrayTranspose", make_intrusive<ASTIdentifier>(ColumnNames::MaskedValues))));
        array_join_expr->setAlias("p");

        builder.select_list.push_back(makeASTFunction("tupleElement", array_join_expr, make_intrusive<ASTLiteral>(1u)));
        builder.select_list.back()->setAlias(ColumnNames::Group);

        builder.select_list.push_back(makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("p"), make_intrusive<ASTLiteral>(2u)));
        builder.select_list.back()->setAlias(ColumnNames::Values);

        builder.where = makeASTFunction("arrayExists",
            makeASTLambda({"x"}, makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
            makeASTFunction("tupleElement", make_intrusive<ASTIdentifier>("p"), make_intrusive<ASTLiteral>(2u)));

        res.select_query = builder.getSelectQuery();
    }

    return res;
}

}
