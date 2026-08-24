#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleBinaryOperator.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForBinaryOperator.h>
#include <Storages/TimeSeries/TimeSeriesNativeHistograms.h>
#include <Storages/TimeSeries/timeSeriesTypesToAST.h>
#include <algorithm>
#include <fmt/core.h>


namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
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
                scalar_value = timeSeriesScalarToAST(scalar_argument.scalar_value, context.scalar_data_type);
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

        /// Steps 1-2: each side selects `group` AS `original_group`, the join key AS `join_group`, and `values`
        /// (with a `timeSeriesThrowDuplicateSeriesIf` duplicate check on side "one").
        bool metric_name_dropped_from_join_group = false;

        for (auto & side : sides)
        {
            SelectQueryBuilder builder;

            bool metric_name_dropped_from_group = (side == left) ? left_argument.metric_name_dropped : right_argument.metric_name_dropped;
            bool metric_name_dropped_from_join_group_on_side = metric_name_dropped_from_group;

            /// `join_group` is always computed with `drop_metric_name = true` because the sides usually have different
            /// metric names; the exception is `on(__name__, ...)`, handled inside `transformGroupASTForBinaryOperator`.
            ASTPtr join_group = transformGroupASTForBinaryOperator(
                operator_node,
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                /* drop_metric_name = */ true,
                metric_name_dropped_from_join_group_on_side);

            /// If the metric name has dropped from the `join_group` either on left or on right then it's dropped.
            metric_name_dropped_from_join_group |= metric_name_dropped_from_join_group_on_side;

            /// Cardinality: one-to-one without modifiers, many-to-one with `group_left`, one-to-many with `group_right`.
            bool group_on_side = (side == left) ? group_left : group_right;

            /// If `join_group` is the same as `group` then we already know it's unique.
            bool check_side_one = !group_on_side && (tryGetIdentifierName(join_group.get()) != ColumnNames::Group);

            /// We add column `original_group` because we may need it at step 3.
            ASTPtr original_group = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            if (check_side_one)
                original_group = makeASTFunction("any", std::move(original_group));
            original_group->setAlias(ColumnNames::OriginalGroup);
            builder.select_list.push_back(std::move(original_group));

            builder.select_list.push_back(join_group);
            builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

            ASTPtr values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            if (check_side_one)
            {
                values = makeASTFunction("any", std::move(values));
                values->setAlias(ColumnNames::Values);
            }
            builder.select_list.push_back(std::move(values));

            if (with_histograms)
            {
                const bool side_has_histograms = ((side == left) ? left_argument : right_argument).store_method == StoreMethod::HISTOGRAM_GRID;

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

                if (check_side_one)
                {
                    histogram_values = makeASTFunction("any", std::move(histogram_values));
                    histogram_values->setAlias(ColumnNames::HistogramValues);
                    sample_kinds = makeASTFunction("any", std::move(sample_kinds));
                    sample_kinds->setAlias(ColumnNames::SampleKinds);
                }
                builder.select_list.push_back(std::move(histogram_values));
                builder.select_list.push_back(std::move(sample_kinds));
            }
            builder.from_table = side;

            if (check_side_one)
            {
                /// We throw an exception if there are multiple matches on the side "one".
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

                builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup)),
                    make_intrusive<ASTLiteral>(0u));
            }

            ASTPtr ast = builder.getSelectQuery();
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(ast), SQLSubqueryType::TABLE});

            side = context.subqueries.back().name;
        }

        /// Step 3: join the sides on `join_group` (INNER ANY without grouping; LEFT/RIGHT SEMI with `group_left`/`group_right`,
        /// copying `side_one` tags via `timeSeriesCopyTags`) and combine values with `arrayMap(x, y -> f(x, y), ...)`.
        ASTPtr result_ast;
        bool metric_name_dropped_from_result = false;
        {
            SelectQueryBuilder builder;

            JoinKind join_kind = JoinKind::Inner;
            JoinStrictness join_strictness = JoinStrictness::Any;

            ASTPtr new_group;
            bool check_no_duplicate_groups = false;

            if (!group_left && !group_right)
            {
                /// Neither group_left nor group_right is specified.

                /// Usually we can use `join_group` directly as the result group, but not always —
                /// see below for cases where we must recompute it from `original_group`.
                bool can_use_join_group_in_result = true;

                /// We can't use `join_group` as the result group in case when
                /// the metric name `__name__` should be preserved in the result but it has already been dropped from `join_group`.
                if (!drop_metric_name && !left_argument.metric_name_dropped && metric_name_dropped_from_join_group)
                {
                    /// E.g. `foo == ignoring(size) bar` must drop only `size`, and `foo == bar` must keep all of `foo`'s tags,
                    /// but `join_group` also has `__name__` removed, so recompute the group from `original_group`.
                    can_use_join_group_in_result = false;
                }

                if (can_use_join_group_in_result)
                {
                    new_group = make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup);
                    metric_name_dropped_from_result = metric_name_dropped_from_join_group;
                }
                else
                {
                    metric_name_dropped_from_result = left_argument.metric_name_dropped;
                    new_group = transformGroupASTForBinaryOperator(
                        operator_node,
                        make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::OriginalGroup}),
                        drop_metric_name,
                        metric_name_dropped_from_result);
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

                /// One-to-one matches: `join_group` is unique on both sides, so INNER ANY JOIN is good here.
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

                    /// We look for many-to-one matches.
                    join_kind = JoinKind::Left;
                }
                else
                {
                    chassert(group_right);
                    side_many = right;
                    side_one = left;
                    metric_name_dropped_from_side_many = right_argument.metric_name_dropped;
                    metric_name_dropped_from_side_one = left_argument.metric_name_dropped;

                    /// We look for one-to-many matches.
                    join_kind = JoinKind::Right;
                }

                join_strictness = JoinStrictness::Semi;

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
                    std::vector<std::string_view> tags_to_copy = {extra_labels.begin(), extra_labels.end()};
                    std::sort(tags_to_copy.begin(), tags_to_copy.end());
                    tags_to_copy.erase(std::unique(tags_to_copy.begin(), tags_to_copy.end()), tags_to_copy.end());

                    if (allow_grouping_modifier_copy_metric_name)
                    {
                        if (std::binary_search(tags_to_copy.begin(), tags_to_copy.end(), kMetricName) && !metric_name_dropped_from_side_one)
                            metric_name_dropped_from_result = false;
                    }
                    else
                    {
                        auto it = std::lower_bound(tags_to_copy.begin(), tags_to_copy.end(), kMetricName);
                        if (it != tags_to_copy.end() && *it == kMetricName)
                            tags_to_copy.erase(it);
                    }

                    new_group = makeASTFunction(
                        "timeSeriesCopyTags",
                        new_group,
                        make_intrusive<ASTIdentifier>(Strings{side_one, ColumnNames::OriginalGroup}),
                        make_intrusive<ASTLiteral>(Array{tags_to_copy.begin(), tags_to_copy.end()}));

                    check_no_duplicate_groups = true;
                }
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

            if (check_no_duplicate_groups)
            {
                values = makeASTFunction("any", std::move(values));
                if (histogram_values)
                    histogram_values = makeASTFunction("any", std::move(histogram_values));
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

            if (check_no_duplicate_groups)
            {
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

                builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::Group)),
                    make_intrusive<ASTLiteral>(0u));
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
