#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleBinaryOperator.h>

#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTWithOnIgnoring.h>
#include <algorithm>


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Applies a simple binary operator to operands if at least one of them is scalar.
    /// Other operand can be either scalar or instant vector.
    SQLQueryPiece applyOperatorToScalarsOrVectorAndScalar(
        const PQT::BinaryOperator * operator_node,
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
        const PQT::BinaryOperator * operator_node,
        SQLQueryPiece && left_argument,
        SQLQueryPiece && right_argument,
        ConverterContext & context,
        std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast,
        bool drop_metric_name,
        bool allow_grouping_modifier_copy_metric_name)
    {
        /// If one of the arguments is empty then the result is also empty.
        if ((left_argument.store_method == StoreMethod::EMPTY) || (right_argument.store_method == StoreMethod::EMPTY))
        {
            return SQLQueryPiece{operator_node, operator_node->result_type, StoreMethod::EMPTY};
        }

        String sides[2];

        left_argument = toVectorGrid(std::move(left_argument), context);
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
        sides[0] = context.subqueries.back().name;
        String & left = sides[0];

        right_argument = toVectorGrid(std::move(right_argument), context);
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
        sides[1] = context.subqueries.back().name;
        String & right = sides[1];

        bool group_left = operator_node->group_left;
        bool group_right = operator_node->group_right;
        const auto & extra_labels = operator_node->extra_labels;

        /// Step 1:
        /// new_left:
        /// SELECT group AS original_group,
        ///        timeSeriesRemoveAllTagsExcept(group, on_tags) AS join_group,
        ///        values
        /// FROM left
        /// [GROUP BY join_group HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, join_group) = 0]
        ///
        /// Step 2:
        /// new_right:
        /// SELECT group AS original_group,
        ///        timeSeriesRemoveAllTagsExcept(group, on_tags) AS join_group,
        ///        values
        /// FROM right
        /// [GROUP BY join_group HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, join_group) = 0]
        ///
        bool metric_name_dropped_from_join_group = false;

        for (auto & side : sides)
        {
            SelectQueryBuilder builder;

            bool metric_name_dropped_from_group = (side == left) ? left_argument.metric_name_dropped : right_argument.metric_name_dropped;
            bool metric_name_dropped_from_join_group_on_side = metric_name_dropped_from_group;

            /// The join_group is always computed with `drop_metric_name=true` because the two sides typically
            /// have different metric names (e.g., `foo` and `bar`), so keeping `__name__` in the join key
            /// would prevent any matches. The exception is `on(__name__, ...)`, handled inside transformGroupASTWithOnIgnoring.
            ASTPtr join_group = transformGroupASTWithOnIgnoring(
                operator_node,
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                /* drop_metric_name = */ true,
                metric_name_dropped_from_join_group_on_side);

            /// If the metric name has dropped from the `join_group` either on left or on right then it's dropped.
            metric_name_dropped_from_join_group |= metric_name_dropped_from_join_group_on_side;

            /// If neither group_left not group_right is specified then it's one-to-one match.
            /// If there is group_left then it's many-to-one match.
            /// If there is group_right then it's one-to-many match.
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

        /// Step 3:
        /// if without grouping:
        /// SELECT timeSeriesRemoveTag(join_group, '__name__') AS group,
        ///        arrayMap(x, y -> f(x, y), left.values, right.values) AS values
        /// FROM left INNER ANY JOIN right
        /// ON left.join_group = right.join_group
        /// [GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, group) = 0]
        ///
        /// if with group_left/group_right:
        /// SELECT timeSeriesCopyTags(timeSeriesRemoveTag(side_many.original_group, '__name__'), side_one.original_group, extra_labels) AS group,
        ///        arrayMap(x, y -> f(x, y), left.values, right.values) AS values
        /// FROM left LEFT/RIGHT SEMI JOIN right
        /// ON left.join_group = right.join_group
        /// [GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(count() > 1, group) = 0]
        ///
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
                    /// Example 1. `foo == ignoring(size) bar`
                    /// - here the result should have only `size` removed, but `join_group` has both `size` and `__name__` removed,
                    /// so we have to recompute it from the `original_group` by removing only `size`.
                    /// Example 2. `foo == bar`
                    /// - here the result should have all the tags of `foo`, but `join_group` has `__name__` removed,
                    /// so we take the original group from the left argument.
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
                    new_group = transformGroupASTWithOnIgnoring(
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

                /// We look for one-to-one matches.
                /// We've already made sure that values of `join_group` are unique on both sides,
                /// so INNER ANY JOIN is good here.
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

            ASTPtr values = makeASTFunction(
                "arrayMap",
                makeASTFunction(
                    "lambda",
                    makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                    apply_function_to_ast(make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y"))),
                make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::Values}),
                make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::Values}));

            if (check_no_duplicate_groups)
                values = makeASTFunction("any", std::move(values));

            builder.select_list.push_back(std::move(values));
            builder.select_list.back()->setAlias(ColumnNames::Values);

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

        SQLQueryPiece res{operator_node, operator_node->result_type, StoreMethod::VECTOR_GRID};

        res.select_query = std::move(result_ast);
        res.start_time = left_argument.start_time;
        res.end_time = left_argument.end_time;
        res.step = left_argument.step;
        res.metric_name_dropped = metric_name_dropped_from_result;

        return res;
    }
}


SQLQueryPiece applySimpleBinaryOperator(
    const PQT::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast,
    bool drop_metric_name,
    bool allow_grouping_modifier_copy_metric_name)
{
    if ((left_argument.type == ResultType::SCALAR) || (right_argument.type == ResultType::SCALAR))
    {
        /// At least one operand is scalar.
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
        allow_grouping_modifier_copy_metric_name);
}

}
