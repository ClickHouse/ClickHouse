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
#include <algorithm>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
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

    /// True when two samples share one grid position.
    ASTPtr makeGridOverlapPredicate(ASTPtr values)
    {
        return makeASTFunction(
            "arrayExists",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                makeASTFunction("greater", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTLiteral>(1u))),
            makeASTFunction("countForEach", std::move(values)));
    }

    /// HAVING clause: throw when `values` overlap, and keep the query otherwise.
    ASTPtr makeNoGridOverlapHaving(ASTPtr values, ASTPtr group)
    {
        return makeASTFunction(
            "equals",
            makeASTFunction(
                "timeSeriesThrowDuplicateSeriesIf",
                makeGridOverlapPredicate(std::move(values)),
                std::move(group)),
            make_intrusive<ASTLiteral>(0u));
    }

    /// Applies a simple operator if both operands are instant vectors.
    SQLQueryPiece applyOperatorToVectors(
        const PrometheusQueryTree::BinaryOperator * operator_node,
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

        /// Step 1 and step 2 keep every source series as its own row:
        /// SELECT group AS original_group,
        ///        timeSeriesRemoveAllTagsExcept(group, on_tags) AS join_group,
        ///        values
        /// FROM <side>
        ///
        /// The side that must be unique per timestamp is joined to a key-only aggregate.
        /// That aggregate throws when two values share a grid position. It does not choose labels:
        /// SELECT join_group
        /// FROM <side_rows>
        /// GROUP BY join_group
        /// HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(x -> x > 1, countForEach(values)), join_group) = 0
        ///
        bool metric_name_dropped_from_join_group = false;
        bool sides_keep_separate_rows = false;

        for (auto & side : sides)
        {
            SelectQueryBuilder builder;

            bool metric_name_dropped_from_group = (side == left) ? left_argument.metric_name_dropped : right_argument.metric_name_dropped;
            bool metric_name_dropped_from_join_group_on_side = metric_name_dropped_from_group;

            /// The join_group is always computed with `drop_metric_name=true` because the two sides typically
            /// have different metric names (e.g., `foo` and `bar`), so keeping `__name__` in the join key
            /// would prevent any matches. The exception is `on(__name__, ...)`, handled inside transformGroupASTForBinaryOperator.
            ASTPtr join_group = transformGroupASTForBinaryOperator(
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

            /// `original_group` is this series' own tags. Step 3 reads it per output row.
            ASTPtr original_group = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            original_group->setAlias(ColumnNames::OriginalGroup);
            builder.select_list.push_back(std::move(original_group));

            builder.select_list.push_back(join_group);
            builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

            builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
            builder.from_table = side;

            ASTPtr ast = builder.getSelectQuery();
            /// The overlap check and the join both read this grid.
            const auto subquery_type = check_side_one ? SQLSubqueryType::MATERIALIZED_TABLE : SQLSubqueryType::TABLE;
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(ast), subquery_type});

            side = context.subqueries.back().name;

            if (check_side_one)
            {
                /// Several series may share `join_group` when their samples do not overlap.
                /// The check selects only the join key, so it cannot pick one series' labels.
                sides_keep_separate_rows = true;

                SelectQueryBuilder keys_builder;
                keys_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                keys_builder.from_table = side;
                keys_builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                keys_builder.having = makeNoGridOverlapHaving(
                    make_intrusive<ASTIdentifier>(ColumnNames::Values),
                    make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

                ASTPtr keys_ast = keys_builder.getSelectQuery();
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(keys_ast), SQLSubqueryType::TABLE});
                const String keys_name = context.subqueries.back().name;

                SelectQueryBuilder kept_builder;
                kept_builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{side, ColumnNames::OriginalGroup}));
                kept_builder.select_list.back()->setAlias(ColumnNames::OriginalGroup);
                kept_builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{side, ColumnNames::JoinGroup}));
                kept_builder.select_list.back()->setAlias(ColumnNames::JoinGroup);
                kept_builder.select_list.push_back(make_intrusive<ASTIdentifier>(Strings{side, ColumnNames::Values}));
                kept_builder.select_list.back()->setAlias(ColumnNames::Values);
                kept_builder.from_table = side;
                kept_builder.join_kind = JoinKind::Inner;
                kept_builder.join_strictness = JoinStrictness::All;
                kept_builder.join_table = keys_name;
                kept_builder.join_on = makeASTFunction(
                    "equals",
                    make_intrusive<ASTIdentifier>(Strings{side, ColumnNames::JoinGroup}),
                    make_intrusive<ASTIdentifier>(Strings{keys_name, ColumnNames::JoinGroup}));

                ASTPtr kept_ast = kept_builder.getSelectQuery();
                context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(kept_ast), SQLSubqueryType::TABLE});
                side = context.subqueries.back().name;
            }
        }

        /// Step 3 emits one row per matching pair, then merges rows that share the result group.
        /// Each pair keeps the source series' `original_group`, so labels stay with their timestamp.
        ///
        /// if without grouping:
        /// SELECT timeSeriesRemoveTags(join_group, ['__name__', '']) AS group,
        ///        arrayMap(x, y -> f(x, y), left.values, right.values) AS values
        /// FROM left INNER ALL JOIN right
        /// ON left.join_group = right.join_group
        /// [GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(x -> x > 1, countForEach(mapped)), group) = 0]
        ///
        /// if with group_left/group_right:
        /// SELECT timeSeriesCopyTags(timeSeriesRemoveTags(side_many.original_group, ['__name__', '']), side_one.original_group, tags_to_copy) AS group,
        ///        arrayMap(x, y -> f(x, y), left.values, right.values) AS values
        /// FROM left INNER ALL JOIN right
        /// ON left.join_group = right.join_group
        /// [GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(x -> x > 1, countForEach(mapped)), group) = 0]
        ///
        /// The GROUP BY merges rows that become the same group. Samples that do not overlap are merged.
        /// Two values at the same grid position are duplicate series.
        ///
        ASTPtr result_ast;
        bool metric_name_dropped_from_result = false;
        {
            SelectQueryBuilder builder;

            JoinKind join_kind = JoinKind::Inner;
            /// ALL keeps one output row per matching series. ANY or SEMI would keep a single label set.
            JoinStrictness join_strictness = sides_keep_separate_rows ? JoinStrictness::All : JoinStrictness::Any;

            ASTPtr new_group;
            bool check_no_duplicate_groups = false;

            if (!group_left && !group_right)
            {
                /// Neither group_left nor group_right is specified.

                /// Usually we can use `join_group` directly as the result group, but not always —
                /// see below for cases where we must recompute it from `original_group`.
                bool can_use_join_group_in_result = true;

                /// We can't use `join_group` as the result group in case when
                /// the metric name `__name__` should be preserved in the result: `join_group` may have dropped `__name__` or the drop marker.
                if (!drop_metric_name && !left_argument.metric_name_dropped)
                {
                    /// Example 1. `foo == ignoring(size) bar`
                    /// - here the result should have only `size` removed, but `join_group` has both `size` and `__name__` removed,
                    /// so we have to recompute it from the `original_group` by removing only `size`.
                    /// Example 2. `foo == bar`
                    /// - here the result should have all the tags of `foo`, but `join_group` has `__name__` removed,
                    /// so we take the original group from the left argument.
                    /// Example 3. `(foo + 1) == on(__name__, shape) bar`
                    /// - here the result should keep `__name__` and the drop marker, but `join_group` has the marker removed,
                    /// so we recompute the group from the `original_group`.
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
                    new_group = makeASTFunction(
                        "timeSeriesRemoveTags", new_group, make_intrusive<ASTLiteral>(Array{kMetricName, kDroppedMetricNameMarker}));
                    metric_name_dropped_from_result = true;
                    check_no_duplicate_groups = true;
                }

                /// One-to-one matches. A shared grid position was already rejected on each side.
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

                    /// Many-to-one. An outer join would keep an unmatched many-side row and
                    /// fill the one side with an empty array.
                    if (!sides_keep_separate_rows)
                        join_kind = JoinKind::Left;
                }
                else
                {
                    chassert(group_right);
                    side_many = right;
                    side_one = left;
                    metric_name_dropped_from_side_many = right_argument.metric_name_dropped;
                    metric_name_dropped_from_side_one = left_argument.metric_name_dropped;

                    if (!sides_keep_separate_rows)
                        join_kind = JoinKind::Right;
                }

                if (!sides_keep_separate_rows)
                    join_strictness = JoinStrictness::Semi;

                /// Drop the metric name from the side "many".
                new_group = make_intrusive<ASTIdentifier>(Strings{side_many, ColumnNames::OriginalGroup});

                metric_name_dropped_from_result = metric_name_dropped_from_side_many;

                if (drop_metric_name && !metric_name_dropped_from_result)
                {
                    new_group = makeASTFunction(
                        "timeSeriesRemoveTags", new_group, make_intrusive<ASTLiteral>(Array{kMetricName, kDroppedMetricNameMarker}));
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
                        if (std::binary_search(tags_to_copy.begin(), tags_to_copy.end(), kMetricName))
                        {
                            /// The many side may still carry the drop marker. A one side that kept its
                            /// metric name clears that marker, so the copied name stays.
                            if (!metric_name_dropped_from_side_one)
                            {
                                metric_name_dropped_from_result = false;
                                new_group = makeASTFunction(
                                    "timeSeriesRemoveTag",
                                    std::move(new_group),
                                    make_intrusive<ASTLiteral>(kDroppedMetricNameMarker));
                            }
                        }
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

            /// Pairs that share a result group merge here. The source rows stayed separate until this point.
            if (sides_keep_separate_rows)
                check_no_duplicate_groups = true;

            if (check_no_duplicate_groups)
            {
                builder.having = makeNoGridOverlapHaving(values->clone(), make_intrusive<ASTIdentifier>(ColumnNames::Group));
                values = makeASTFunction("anyForEach", std::move(values));
            }

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
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));

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
    const PrometheusQueryTree::BinaryOperator * operator_node,
    SQLQueryPiece && left_argument,
    SQLQueryPiece && right_argument,
    ConverterContext & context,
    std::function<ASTPtr(ASTPtr, ASTPtr)> apply_function_to_ast,
    bool drop_metric_name,
    bool allow_grouping_modifier_copy_metric_name)
{
    checkVectorMatching(operator_node, left_argument, right_argument);

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
