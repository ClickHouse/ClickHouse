#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleBinaryOperator.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/makeNoDuplicateSeriesPerStepCheck.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForBinaryOperator.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <algorithm>
#include <fmt/core.h>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
    extern const int LOGICAL_ERROR;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// The sample kind of a float sample in a `sample_kinds` array (see StoreMethod::HISTOGRAM_GRID).
    ASTPtr floatKind()
    {
        return make_intrusive<ASTLiteral>(Float64{0});
    }

    /// The sample kind of a histogram sample in a `sample_kinds` array.
    ASTPtr histogramKind()
    {
        return make_intrusive<ASTLiteral>(Float64{1});
    }

    /// The `sample_kinds` arm of a HISTOGRAM_GRID-producing binary operator, derived from the two
    /// result arms (exactly one of them is non-NULL at a kept time step; both NULL at a dropped one).
    ASTPtr buildResultSampleKinds()
    {
        return makeASTFunction(
            "arrayMap",
            makeASTLambda({"v", "h"}, makeASTFunction(
                "if",
                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("v")),
                floatKind(),
                makeASTFunction(
                    "if",
                    makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("h")),
                    histogramKind(),
                    make_intrusive<ASTLiteral>(Field{})))),
            make_intrusive<ASTIdentifier>(ColumnNames::Values),
            make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues));
    }

    /// Applies a simple binary operator to a scalar and a combined float+histogram grid
    /// (StoreMethod::HISTOGRAM_GRID); an outer query derives `sample_kinds` from the two arms.
    SQLQueryPiece applyOperatorToHistogramGridAndScalar(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && scalar_argument,
        SQLQueryPiece && vector_argument,
        bool scalar_is_left,
        ConverterContext & context,
        const std::function<ASTPtr(ASTPtr, ASTPtr)> & apply_function_to_ast,
        const SimpleBinaryOperatorHistogramArm & histogram_arm,
        bool drop_metric_name)
    {
        chassert(vector_argument.store_method == StoreMethod::HISTOGRAM_GRID);

        /// The per-step scalar value: one expression for CONST_SCALAR/SINGLE_SCALAR, or the `s`
        /// iterator of the arrayMap for SCALAR_GRID.
        ASTPtr scalar_value;
        ASTPtr scalar_grid_array;
        switch (scalar_argument.store_method)
        {
            case StoreMethod::CONST_SCALAR:
            {
                scalar_value = timeSeriesScalarToAST(scalar_argument.scalar_value);
                break;
            }
            case StoreMethod::SINGLE_SCALAR:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR});
                /// Here assumeNotNull() is used because the scalar subquery converts its result to nullable.
                scalar_value = makeASTFunction("assumeNotNull", make_intrusive<ASTIdentifier>(context.subqueries.back().name));
                break;
            }
            case StoreMethod::SCALAR_GRID:
            {
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(scalar_argument.select_query), SQLSubqueryType::SCALAR});
                scalar_grid_array = make_intrusive<ASTIdentifier>(context.subqueries.back().name);
                scalar_value = make_intrusive<ASTIdentifier>("s");
                break;
            }
            default:
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR,
                                "applyOperatorToHistogramGridAndScalar: Can't handle scalar argument {} because of its store method {}",
                                getPromQLText(scalar_argument, context), scalar_argument.store_method);
            }
        }

        ASTs float_lambda_args = {make_intrusive<ASTIdentifier>("v"), make_intrusive<ASTIdentifier>("k")};
        ASTs histogram_lambda_args = {make_intrusive<ASTIdentifier>("h"), make_intrusive<ASTIdentifier>("k")};
        if (scalar_grid_array)
        {
            float_lambda_args.push_back(make_intrusive<ASTIdentifier>("s"));
            histogram_lambda_args.push_back(make_intrusive<ASTIdentifier>("s"));
        }

        ASTPtr left_value = scalar_is_left ? scalar_value : static_cast<ASTPtr>(make_intrusive<ASTIdentifier>("v"));
        ASTPtr right_value = scalar_is_left ? static_cast<ASTPtr>(make_intrusive<ASTIdentifier>("v")) : scalar_value;

        SimpleBinaryOperatorHistogramArm::Input arm_input;
        arm_input.left_value = left_value;
        arm_input.right_value = right_value;
        if (scalar_is_left)
        {
            arm_input.left_histogram = make_intrusive<ASTLiteral>(Field{});
            arm_input.left_kind = floatKind();
            arm_input.right_histogram = make_intrusive<ASTIdentifier>("h");
            arm_input.right_kind = make_intrusive<ASTIdentifier>("k");
        }
        else
        {
            arm_input.left_histogram = make_intrusive<ASTIdentifier>("h");
            arm_input.left_kind = make_intrusive<ASTIdentifier>("k");
            arm_input.right_histogram = make_intrusive<ASTLiteral>(Field{});
            arm_input.right_kind = floatKind();
        }
        arm_input.left_is_scalar = scalar_is_left;
        arm_input.right_is_scalar = !scalar_is_left;

        ASTPtr inner_query;
        {
            SelectQueryBuilder builder;

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

            /// The float arm: the scalar combined with the grid's float samples (kind 0).
            ASTs float_sources = {
                make_intrusive<ASTIdentifier>(ColumnNames::Values),
                make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds)};
            if (scalar_grid_array)
                float_sources.push_back(scalar_grid_array);

            auto float_lambda = makeASTFunction("tuple");
            float_lambda->arguments->children = std::move(float_lambda_args);
            auto float_array_map = makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    std::move(float_lambda),
                    makeASTFunction(
                        "if",
                        makeASTFunction("equals", make_intrusive<ASTIdentifier>("k"), floatKind()),
                        apply_function_to_ast(std::move(left_value), std::move(right_value)),
                        make_intrusive<ASTLiteral>(Field{}))));
            float_array_map->arguments->children.insert(float_array_map->arguments->children.end(), float_sources.begin(), float_sources.end());
            builder.select_list.push_back(std::move(float_array_map));
            builder.select_list.back()->setAlias(ColumnNames::Values);

            /// The histogram arm (or NULL at time steps where the operation is not allowed).
            ASTs histogram_sources = {
                make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues),
                make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds)};
            if (scalar_grid_array)
                histogram_sources.push_back(scalar_grid_array);

            auto histogram_lambda = makeASTFunction("tuple");
            histogram_lambda->arguments->children = std::move(histogram_lambda_args);
            auto histogram_array_map = makeASTFunction(
                "arrayMap",
                makeASTFunction("lambda", std::move(histogram_lambda), histogram_arm.build_histogram_values_arm(arm_input)));
            histogram_array_map->arguments->children.insert(
                histogram_array_map->arguments->children.end(), histogram_sources.begin(), histogram_sources.end());
            builder.select_list.push_back(std::move(histogram_array_map));
            builder.select_list.back()->setAlias(ColumnNames::HistogramValues);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(vector_argument.select_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            inner_query = builder.getSelectQuery();
        }

        /// The outer query derives `sample_kinds` from the two arms.
        {
            SelectQueryBuilder builder;

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues));
            builder.select_list.push_back(buildResultSampleKinds());
            builder.select_list.back()->setAlias(ColumnNames::SampleKinds);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(inner_query), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            SQLQueryPiece res{operator_node, operator_node->result_type, StoreMethod::HISTOGRAM_GRID};
            res.select_query = builder.getSelectQuery();
            res.start_time = vector_argument.start_time;
            res.end_time = vector_argument.end_time;
            res.step = vector_argument.step;
            res.metric_name_dropped = vector_argument.metric_name_dropped;

            if (drop_metric_name)
                res = dropMetricName(std::move(res), context);

            return res;
        }
    }

    void checkVectorMatching(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        const SQLQueryPiece & left_argument,
        const SQLQueryPiece & right_argument)
    {
        if (!operator_node->labels.empty()
            && ((left_argument.type != ResultType::INSTANT_VECTOR) || (right_argument.type != ResultType::INSTANT_VECTOR)))
        {
            throw Exception(ErrorCodes::CANNOT_EXECUTE_PROMQL_QUERY,
                            "Binary operator '{}' with vector matching expects two arguments of type {}, got {} and {}",
                            operator_node->operator_name, ResultType::INSTANT_VECTOR, left_argument.type, right_argument.type);
        }
    }

    /// Applies a simple binary operator to operands if at least one of them is scalar.
    /// Other operand can be either scalar or instant vector.
    SQLQueryPiece applyOperatorToScalarsOrVectorAndScalar(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context,
        std::function<ASTPtr(ASTPtr, ASTPtr)> apply_operator_to_ast,
        bool drop_metric_name)
    {
        auto apply_function_to_ast = [&](ASTs args) -> ASTPtr
        {
            chassert(args.size() == 2);
            return apply_operator_to_ast(args[0], args[1]);
        };

        auto res = applySimpleFunction(operator_node, context, apply_function_to_ast, {std::move(left_argument), std::move(right_argument)});

        if (drop_metric_name)
            res = dropMetricName(std::move(res), context);

        return res;
    }

    /// Applies a simple operator if both operands are instant vectors.
    SQLQueryPiece applyOperatorToVectors(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context,
        std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast,
        bool drop_metric_name,
        bool allow_grouping_modifier_copy_metric_name,
        const SimpleBinaryOperatorHistogramArm * histogram_arm = nullptr)
    {
        /// If one of the arguments is empty then the result is also empty.
        if ((left_argument.store_method == StoreMethod::EMPTY) || (right_argument.store_method == StoreMethod::EMPTY))
        {
            return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};
        }

        /// The histogram mode: at least one side is a combined float+histogram grid (see SimpleBinaryOperatorHistogramArm).
        const bool with_histograms = histogram_arm
            && ((left_argument.store_method == StoreMethod::HISTOGRAM_GRID) || (right_argument.store_method == StoreMethod::HISTOGRAM_GRID));

        String sides[2];

        if (left_argument.store_method != StoreMethod::HISTOGRAM_GRID)
            left_argument = toVectorGrid(std::move(left_argument), context);
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
        sides[0] = context.subqueries.back().name;
        String & left = sides[0];

        if (right_argument.store_method != StoreMethod::HISTOGRAM_GRID)
            right_argument = toVectorGrid(std::move(right_argument), context);
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
        sides[1] = context.subqueries.back().name;
        String & right = sides[1];

        bool group_left = operator_node->group_left;
        bool group_right = operator_node->group_right;
        const auto & extra_labels = operator_node->extra_labels;

        /// Tags copied from the side "one" to the result group by `group_left(tags)` or `group_right(tags)`.
        std::vector<std::string_view> tags_to_copy = {extra_labels.begin(), extra_labels.end()};
        std::sort(tags_to_copy.begin(), tags_to_copy.end());
        tags_to_copy.erase(std::unique(tags_to_copy.begin(), tags_to_copy.end()), tags_to_copy.end());
        if (!allow_grouping_modifier_copy_metric_name)
        {
            auto it = std::lower_bound(tags_to_copy.begin(), tags_to_copy.end(), kMetricName);
            if (it != tags_to_copy.end() && *it == kMetricName)
                tags_to_copy.erase(it);
        }

        /// Apply the `on(tags)` or `ignoring(tags)` matching rules to the `group` column.
        bool metric_name_dropped_from_join_group_on_side[2] = {left_argument.metric_name_dropped, right_argument.metric_name_dropped};
        /// The join_group is always computed with `drop_metric_name=true` because the two sides typically
        /// have different metric names (e.g., `foo` and `bar`), so keeping `__name__` in the join key
        /// would prevent any matches. The exception is `on(__name__, ...)`, handled inside transformGroupASTForBinaryOperator.
        ASTPtr join_group_on_side[2]
            = {transformGroupASTForBinaryOperator(
                   operator_node,
                   make_intrusive<ASTIdentifier>(ColumnNames::Group),
                   /* drop_metric_name = */ true,
                   metric_name_dropped_from_join_group_on_side[0]),
               transformGroupASTForBinaryOperator(
                   operator_node,
                   make_intrusive<ASTIdentifier>(ColumnNames::Group),
                   /* drop_metric_name = */ true,
                   metric_name_dropped_from_join_group_on_side[1])};
        bool metric_name_dropped_from_join_group = metric_name_dropped_from_join_group_on_side[0] || metric_name_dropped_from_join_group_on_side[1];

        /// If `join_group` is the same as `group` then we already know it's unique.
        bool is_join_group_unique_on_side[2] = {
            tryGetIdentifierName(join_group_on_side[0].get()) == ColumnNames::Group,
            tryGetIdentifierName(join_group_on_side[1].get()) == ColumnNames::Group};

        /// `original_group` is used at step 5:
        /// - on the side "many" always,
        /// - on the side "one" if extra labels are copied from it via `group_left` / `group_right`,
        /// - on the left side of a one-to-one match if the metric name should be kept in the result but was dropped from `join_group`
        ///   (e.g. `a > b`: `join_group` has no `__name__` while the result must keep `__name__` of `a`).
        bool original_group_is_used_on_side[2]
            = {group_left || (group_right && !extra_labels.empty())
                   || (!group_right && !drop_metric_name && !left_argument.metric_name_dropped && metric_name_dropped_from_join_group),
               group_right || (group_left && !extra_labels.empty())};

        /// Steps 1-2:
        /// new_left / new_right:
        /// SELECT [group AS original_group | timeSeriesRemoveAllTagsExcept(group, extra_labels) AS original_group,]
        ///        timeSeriesRemoveAllTagsExcept(group, on_tags) AS join_group,
        ///        values | anyForEach(values) AS values
        /// FROM left / right
        /// [WHERE arrayExists(x -> isNotNull(x), values)]
        /// [GROUP BY join_group HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), join_group) = 0]
        /// [GROUP BY original_group, join_group
        ///  HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), join_group) = 0]

        for (size_t side_index = 0; side_index != 2; ++side_index)
        {
            String & side = sides[side_index];
            bool is_left_side = (side_index == 0);
            SelectQueryBuilder builder;

            /// If neither group_left nor group_right is specified then it's one-to-one match, and both sides are "one".
            /// If there is group_left then it's many-to-one match, and the left side is "many".
            /// If there is group_right then it's one-to-many match, and the right side is "many".
            bool is_side_many = is_left_side ? group_left : group_right;
            bool is_side_one = !is_side_many;

            ASTPtr join_group = join_group_on_side[side_index];
            bool & is_join_group_unique = is_join_group_unique_on_side[side_index];
            bool original_group_is_used = original_group_is_used_on_side[side_index];

            /// Series on the side "one" must be unique by `join_group` at each step.
            bool check_unique_by_join_group = is_side_one && !is_join_group_unique;

            /// If `original_group` isn't used then series on the side "one" are merged right here:
            /// `values` becomes `anyForEach(values)` and the HAVING clause throws an exception
            /// if two of them have values at the same step.
            /// However we can't do that if the `original_group` is used because
            /// the `original_group` can be different for the same `join_group`.
            ///
            /// The histogram mode keeps the check per series: series on the side "one" are merged by `join_group` with any().
            bool merge_by_join_group = check_unique_by_join_group && (with_histograms || !original_group_is_used);

            /// If `original_group` is used only to copy extra labels to the result group then series with the same `join_group`
            /// and the same extra labels are merged here, and `original_group` is reduced to the extra labels only
            /// because that's all that step 5 needs from it. Series with different extra labels stay separate.
            bool merge_by_join_group_and_extra_labels
                = !with_histograms && check_unique_by_join_group && original_group_is_used && !extra_labels.empty();

            if (original_group_is_used)
            {
                ASTPtr original_group = make_intrusive<ASTIdentifier>(ColumnNames::Group);
                if (merge_by_join_group_and_extra_labels)
                {
                    /// timeSeriesRemoveAllTagsExcept(group, extra_labels) AS original_group
                    original_group = makeASTFunction(
                        "timeSeriesRemoveAllTagsExcept",
                        std::move(original_group),
                        make_intrusive<ASTLiteral>(Array{tags_to_copy.begin(), tags_to_copy.end()}));
                }
                else if (merge_by_join_group)
                {
                    original_group = makeASTFunction("any", std::move(original_group));
                }
                original_group->setAlias(ColumnNames::OriginalGroup);
                builder.select_list.push_back(std::move(original_group));
            }

            builder.select_list.push_back(join_group);
            builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

            ASTPtr values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            if (with_histograms && merge_by_join_group)
            {
                values = makeASTFunction("any", std::move(values));
                values->setAlias(ColumnNames::Values);
            }
            else if (merge_by_join_group || merge_by_join_group_and_extra_labels)
            {
                values = makeNoDuplicateSeriesPerStepValues(std::move(values));
                values->setAlias(ColumnNames::Values);
            }
            builder.select_list.push_back(std::move(values));

            if (with_histograms)
            {
                const bool side_has_histograms = (is_left_side ? left_argument : right_argument).store_method == StoreMethod::HISTOGRAM_GRID;

                ASTPtr histogram_values;
                ASTPtr sample_kinds;
                if (side_has_histograms)
                {
                    histogram_values = make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues);
                    sample_kinds = make_intrusive<ASTIdentifier>(ColumnNames::SampleKinds);
                }
                else
                {
                    /// A plain float side: an all-NULL histogram arm, and kind 0 at float-sample steps.
                    histogram_values = makeASTFunction(
                        "arrayResize",
                        makeASTFunction(
                            "CAST",
                            make_intrusive<ASTLiteral>(Array{}),
                            make_intrusive<ASTLiteral>(fmt::format("Array(Nullable({}))", getTimeSeriesHistogramPayloadTupleType()->getName()))),
                        makeASTFunction("length", make_intrusive<ASTIdentifier>(ColumnNames::Values)));
                    sample_kinds = makeASTFunction(
                        "arrayMap",
                        makeASTLambda({"x"}, makeASTFunction(
                            "if",
                            makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x")),
                            floatKind(),
                            make_intrusive<ASTLiteral>(Field{}))),
                        make_intrusive<ASTIdentifier>(ColumnNames::Values));
                }

                if (merge_by_join_group)
                {
                    histogram_values = makeASTFunction("any", std::move(histogram_values));
                    sample_kinds = makeASTFunction("any", std::move(sample_kinds));
                }
                /// Both arms of a plain float side are expressions, so they need the alias even without `any()`.
                if (merge_by_join_group || !side_has_histograms)
                {
                    histogram_values->setAlias(ColumnNames::HistogramValues);
                    sample_kinds->setAlias(ColumnNames::SampleKinds);
                }
                builder.select_list.push_back(std::move(histogram_values));
                builder.select_list.push_back(std::move(sample_kinds));
            }

            builder.from_table = side;

            if (with_histograms && merge_by_join_group)
            {
                /// GROUP BY join_group HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, join_group) = 0
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup)),
                    make_intrusive<ASTLiteral>(0u));
                is_join_group_unique = true;
            }
            else if (merge_by_join_group)
            {
                /// GROUP BY join_group HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), join_group) = 0
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                builder.having = makeNoDuplicateSeriesPerStepCheck(
                    make_intrusive<ASTIdentifier>(Strings{side, ColumnNames::Values}),
                    make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                is_join_group_unique = true;
            }
            else if (merge_by_join_group_and_extra_labels)
            {
                /// GROUP BY original_group, join_group
                /// HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), join_group) = 0
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::OriginalGroup));
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                builder.having = makeNoDuplicateSeriesPerStepCheck(
                    make_intrusive<ASTIdentifier>(Strings{side, ColumnNames::Values}),
                    make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
            }

            auto subquery_type = SQLSubqueryType::TABLE;

            /// If the `original_group` is used then series with the same `join_group` can still be there after this step,
            /// they're checked at steps 3-4. Series without any value aren't series at all,
            /// so they're removed here to reduce the rows for that check and for the join.
            if (!with_histograms && check_unique_by_join_group && original_group_is_used)
            {
                /// WHERE arrayExists(x -> isNotNull(x), values)
                builder.where = makeASTFunction(
                    "arrayExists",
                    makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                        makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
                    make_intrusive<ASTIdentifier>(Strings{side, ColumnNames::Values}));

                /// The result of this step is read twice at steps 3-4, so it must be evaluated once.
                subquery_type = SQLSubqueryType::MATERIALIZED_TABLE;
            }

            ASTPtr ast = builder.getSelectQuery();
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(ast), subquery_type});

            side = context.subqueries.back().name;
        }

        /// Steps 3-4 (only for a side with series kept separate at steps 1-2):
        /// new_left / new_right:
        /// SELECT original_group, join_group, values
        /// FROM left / right
        /// WHERE join_group IN (SELECT join_group FROM left / right GROUP BY join_group
        ///                      HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), join_group) = 0)
        ///
        /// The subquery throws an exception if two series with the same `join_group` have values at the same step,
        /// otherwise it returns all the values of `join_group`, so the condition keeps all the rows.
        /// The check is a subquery in the WHERE clause because a subquery in the WITH clause is evaluated only if it's referenced.
        for (size_t side_index = 0; side_index != 2; ++side_index)
        {
            bool is_left_side = (side_index == 0);
            bool is_side_many = is_left_side ? group_left : group_right;
            bool is_side_one = !is_side_many;
            bool is_join_group_unique = is_join_group_unique_on_side[side_index];
            bool check_unique_by_join_group = is_side_one && !is_join_group_unique;

            if (check_unique_by_join_group)
            {
                String & side = sides[side_index];

                SelectQueryBuilder check_builder;
                check_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                check_builder.from_table = side;
                check_builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                check_builder.having = makeNoDuplicateSeriesPerStepCheck(
                    make_intrusive<ASTIdentifier>(ColumnNames::Values), make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

                SelectQueryBuilder builder;
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::OriginalGroup));
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
                builder.from_table = side;
                builder.where = makeASTFunction(
                    "in", make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup), make_intrusive<ASTSubquery>(check_builder.getSelectQuery()));

                ASTPtr ast = builder.getSelectQuery();
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(ast), SQLSubqueryType::TABLE});

                side = context.subqueries.back().name;
            }
        }

        /// Step 5:
        /// if without grouping:
        /// SELECT timeSeriesRemoveTag(join_group, '__name__') AS group,
        ///        arrayMap(x, y -> f(x, y), left.values, right.values) AS values
        /// FROM left INNER ANY JOIN right
        /// ON left.join_group = right.join_group
        /// [GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), group) = 0]
        ///
        /// if with group_left/group_right:
        /// SELECT timeSeriesCopyTags(timeSeriesRemoveTag(side_many.original_group, '__name__'), side_one.original_group, extra_labels) AS group,
        ///        arrayMap(x, y -> f(x, y), left.values, right.values) AS values
        /// FROM left LEFT/RIGHT SEMI JOIN right
        /// ON left.join_group = right.join_group
        /// [GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), group) = 0]
        ///
        /// If series were kept separate at steps 1-2 then that side can have multiple rows with the same `join_group`,
        /// so INNER ALL JOIN is used to get all the pairs, and the grouping by the result `group` is applied.
        /// If the grouping is applied then `values` becomes `anyForEach(arrayMap(...))`: series with the same result `group`
        /// are merged step by step and an exception is thrown only if two of them have values at the same step.
        ///
        ASTPtr result_ast;
        bool metric_name_dropped_from_result = false;
        {
            SelectQueryBuilder builder;

            ASTPtr new_group;

            /// Two rows of the join can get the same result `group`, then they must be merged step by step.
            /// It's set below where the result group is built.
            bool check_no_duplicate_groups = false;

            if (!group_left && !group_right)
            {
                /// Neither group_left nor group_right is specified.

                bool original_group_is_used_on_left = original_group_is_used_on_side[0];
                chassert(!original_group_is_used_on_side[1]);

                if (original_group_is_used_on_left)
                {
                    /// We can't use `join_group` as the result group in case when
                    /// the metric name `__name__` should be preserved in the result but it has already been dropped from `join_group`.
                    /// Example 1. `foo == ignoring(size) bar`
                    /// - here the result should have only `size` removed, but `join_group` has both `size` and `__name__` removed,
                    /// so we have to recompute it from the `original_group` by removing only `size`.
                    /// Example 2. `foo == bar`
                    /// - here the result should have all the tags of `foo`, but `join_group` has `__name__` removed,
                    /// so we take the original group from the left argument.
                    metric_name_dropped_from_result = left_argument.metric_name_dropped;
                    new_group = transformGroupASTForBinaryOperator(
                        operator_node,
                        make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::OriginalGroup}),
                        drop_metric_name,
                        metric_name_dropped_from_result);
                }
                else
                {
                    /// Usually we can use `join_group` directly as the result group.
                    new_group = make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup);
                    metric_name_dropped_from_result = metric_name_dropped_from_join_group;
                }

                /// If we use `join_group` in result then it's possible that it has the metric name `__name__`,
                /// but the result shouldn't have it.
                if (drop_metric_name && !metric_name_dropped_from_result)
                {
                    /// For example `a + on(__name__) b`
                    /// - here `join_group` has the __name__ tag, but the result shouldn't have it.
                    new_group = makeASTFunction("timeSeriesRemoveTag", new_group, make_intrusive<ASTLiteral>(kMetricName));
                    metric_name_dropped_from_result = true;
                    check_no_duplicate_groups = true;
                }
            }
            else
            {
                chassert(group_left != group_right);

                /// Either group_left or group_right is specified.
                /// There are two sides: "one" and "many".
                String side_many;
                String side_one;
                bool metric_name_dropped_from_side_many = false;
                bool metric_name_dropped_from_side_one = false;

                if (group_left)
                {
                    side_many = left;
                    side_one = right;
                    metric_name_dropped_from_side_many = left_argument.metric_name_dropped;
                    metric_name_dropped_from_side_one = right_argument.metric_name_dropped;
                }
                else
                {
                    chassert(group_right);
                    side_many = right;
                    side_one = left;
                    metric_name_dropped_from_side_many = right_argument.metric_name_dropped;
                    metric_name_dropped_from_side_one = left_argument.metric_name_dropped;
                }

                /// Drop the metric name from the side "many".
                new_group = make_intrusive<ASTIdentifier>(Strings{side_many, ColumnNames::OriginalGroup});

                metric_name_dropped_from_result = metric_name_dropped_from_side_many;

                if (drop_metric_name && !metric_name_dropped_from_result)
                {
                    new_group = makeASTFunction("timeSeriesRemoveTag", new_group, make_intrusive<ASTLiteral>(kMetricName));
                    metric_name_dropped_from_result = true;
                    check_no_duplicate_groups = true;
                }

                /// Add extra labels from the side "one".
                if (!extra_labels.empty())
                {
                    if (allow_grouping_modifier_copy_metric_name
                        && std::binary_search(tags_to_copy.begin(), tags_to_copy.end(), kMetricName) && !metric_name_dropped_from_side_one)
                        metric_name_dropped_from_result = false;

                    new_group = makeASTFunction(
                        "timeSeriesCopyTags",
                        new_group,
                        make_intrusive<ASTIdentifier>(Strings{side_one, ColumnNames::OriginalGroup}),
                        make_intrusive<ASTLiteral>(Array{tags_to_copy.begin(), tags_to_copy.end()}));

                    check_no_duplicate_groups = true;
                }
            }

            /// A side "one" can have multiple rows with the same `join_group` if its series were kept separate at steps 1-2
            /// because step 5 uses its `original_group` (e.g. `a > b`, see above). Such rows can give multiple rows of the join
            /// with the same result `group`, so they must be merged step by step.
            bool side_one_has_multiple_rows_per_join_group
                = (!group_left && !is_join_group_unique_on_side[0]) || (!group_right && !is_join_group_unique_on_side[1]);

            if (side_one_has_multiple_rows_per_join_group)
                check_no_duplicate_groups = true;

            /// Calculate properties of JOIN.
            /// If `join_group` is unique on both sides, then each row matches at most one row.
            JoinKind join_kind = JoinKind::Inner;
            JoinStrictness join_strictness = JoinStrictness::Any;

            /// A side can have multiple rows with the same `join_group` if it's the side "many"
            /// or if its series were kept separate at steps 1-2 because of using `original_group`.
            bool left_has_multiple_rows_per_join_group = group_left || !is_join_group_unique_on_side[0];
            bool right_has_multiple_rows_per_join_group = group_right || !is_join_group_unique_on_side[1];

            if (left_has_multiple_rows_per_join_group && right_has_multiple_rows_per_join_group)
            {
                /// Both sides can have multiple rows with the same `join_group`, so we need all the pairs.
                join_kind = JoinKind::Inner;
                join_strictness = JoinStrictness::All;
            }
            else if (left_has_multiple_rows_per_join_group || right_has_multiple_rows_per_join_group)
            {
                /// Only one side can have multiple rows with the same `join_group`, each row of it matches at most one row of the other side.
                join_kind = left_has_multiple_rows_per_join_group ? JoinKind::Left : JoinKind::Right;
                join_strictness = JoinStrictness::Semi;
            }

            builder.select_list.push_back(std::move(new_group));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            ASTPtr values;
            ASTPtr histogram_values;
            if (!with_histograms)
            {
                values = makeASTFunction(
                    "arrayMap",
                    makeASTFunction(
                        "lambda",
                        makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                        apply_function_to_ast(make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y"))),
                    make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                    make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Values}));
            }
            else
            {
                /// The float arm: both sides resolved to a float sample (kind 0) at this time step.
                values = makeASTFunction(
                    "arrayMap",
                    makeASTLambda({"x", "k", "y", "m"}, makeASTFunction(
                        "if",
                        makeASTFunction(
                            "and",
                            makeASTFunction("equals", make_intrusive<ASTIdentifier>("k"), floatKind()),
                            makeASTFunction("equals", make_intrusive<ASTIdentifier>("m"), floatKind())),
                        apply_function_to_ast(make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                        make_intrusive<ASTLiteral>(Field{}))),
                    make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                    make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::SampleKinds}),
                    make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Values}),
                    make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::SampleKinds}));

                /// The histogram arm (NULL at time steps where the operation is not allowed for the kind combination).
                SimpleBinaryOperatorHistogramArm::Input arm_input;
                arm_input.left_value = make_intrusive<ASTIdentifier>("x");
                arm_input.left_histogram = make_intrusive<ASTIdentifier>("h");
                arm_input.left_kind = make_intrusive<ASTIdentifier>("k");
                arm_input.right_value = make_intrusive<ASTIdentifier>("y");
                arm_input.right_histogram = make_intrusive<ASTIdentifier>("g");
                arm_input.right_kind = make_intrusive<ASTIdentifier>("m");

                histogram_values = makeASTFunction(
                    "arrayMap",
                    makeASTLambda({"h", "k", "g", "m", "x", "y"}, histogram_arm->build_histogram_values_arm(arm_input)),
                    make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::HistogramValues}),
                    make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::SampleKinds}),
                    make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::HistogramValues}),
                    make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::SampleKinds}),
                    make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                    make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Values}));
            }

            ASTPtr values_for_check;
            if (check_no_duplicate_groups && with_histograms)
            {
                values = makeASTFunction("any", std::move(values));
                histogram_values = makeASTFunction("any", std::move(histogram_values));
            }
            else if (check_no_duplicate_groups)
            {
                values_for_check = values->clone();
                values = makeNoDuplicateSeriesPerStepValues(std::move(values));
            }

            builder.select_list.push_back(std::move(values));
            builder.select_list.back()->setAlias(ColumnNames::Values);

            if (histogram_values)
            {
                builder.select_list.push_back(std::move(histogram_values));
                builder.select_list.back()->setAlias(ColumnNames::HistogramValues);
            }

            builder.from_table = left;

            builder.join_kind = join_kind;
            builder.join_strictness = join_strictness;
            builder.join_table = right;

            builder.join_on = makeASTFunction(
                "equals",
                make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::JoinGroup}),
                make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::JoinGroup}));

            if (check_no_duplicate_groups && with_histograms)
            {
                /// GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, group) = 0
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
                builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::Group)),
                    make_intrusive<ASTLiteral>(0u));
            }
            else if (check_no_duplicate_groups)
            {
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
                builder.having = makeNoDuplicateSeriesPerStepCheck(std::move(values_for_check), make_intrusive<ASTIdentifier>(ColumnNames::Group));
            }

            result_ast = builder.getSelectQuery();
        }

        if (with_histograms)
        {
            /// The outer query derives `sample_kinds` from the two arms.
            SelectQueryBuilder builder;

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::HistogramValues));
            builder.select_list.push_back(buildResultSampleKinds());
            builder.select_list.back()->setAlias(ColumnNames::SampleKinds);

            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(result_ast), SQLSubqueryType::TABLE});
            builder.from_table = context.subqueries.back().name;

            result_ast = builder.getSelectQuery();
        }

        SQLQueryPiece res{operator_node, operator_node->result_type, with_histograms ? StoreMethod::HISTOGRAM_GRID : StoreMethod::VECTOR_GRID};

        res.select_query = std::move(result_ast);
        res.start_time = left_argument.start_time;
        res.end_time = left_argument.end_time;
        res.step = left_argument.step;
        res.metric_name_dropped = metric_name_dropped_from_result;

        return res;
    }
}


SQLQueryPiece applySimpleBinaryOperator(
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast,
    bool drop_metric_name,
    bool allow_grouping_modifier_copy_metric_name,
    const SimpleBinaryOperatorHistogramArm * histogram_arm)
{
    checkVectorMatching(operator_node, left_argument, right_argument);

    if ((left_argument.type == ResultType::SCALAR) || (right_argument.type == ResultType::SCALAR))
    {
        /// At least one operand is scalar.
        if (histogram_arm
            && ((left_argument.store_method == StoreMethod::HISTOGRAM_GRID) || (right_argument.store_method == StoreMethod::HISTOGRAM_GRID))
            && (left_argument.store_method != StoreMethod::EMPTY) && (right_argument.store_method != StoreMethod::EMPTY))
        {
            /// A scalar combined with a combined float+histogram grid.
            const bool scalar_is_left = (left_argument.type == ResultType::SCALAR);
            /// The scalar goes to the first argument and the vector to the second; spell the two cases out
            /// so each argument is moved in exactly one place (the correlated ternaries moved both twice).
            if (scalar_is_left)
                return applyOperatorToHistogramGridAndScalar(
                    operator_node,
                    std::move(left_argument),
                    std::move(right_argument),
                    true,
                    context,
                    apply_function_to_ast,
                    *histogram_arm,
                    drop_metric_name);
            return applyOperatorToHistogramGridAndScalar(
                operator_node,
                std::move(right_argument),
                std::move(left_argument),
                false,
                context,
                apply_function_to_ast,
                *histogram_arm,
                drop_metric_name);
        }

        return applyOperatorToScalarsOrVectorAndScalar(
            operator_node, std::move(left_argument), std::move(right_argument), context, apply_function_to_ast, drop_metric_name);
    }

    /// Both operands are instant vectors.
    chassert((left_argument.type == ResultType::INSTANT_VECTOR) && (right_argument.type == ResultType::INSTANT_VECTOR));

    return applyOperatorToVectors(
        operator_node,
        std::move(left_argument),
        std::move(right_argument),
        context,
        apply_function_to_ast,
        drop_metric_name,
        allow_grouping_modifier_copy_metric_name,
        histogram_arm);
}

}
