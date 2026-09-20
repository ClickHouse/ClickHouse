#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleBinaryOperator.h>

#include <Common/Exception.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
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

        bool group_left = operator_node->group_left;
        bool group_right = operator_node->group_right;
        const auto & extra_labels = operator_node->extra_labels;

        bool metric_name_dropped_from_join_group = false;
        String left;
        String right;

        if (group_left)
        {
            /// Dynamic filter pushdown: evaluate left side first and push join_group into right side.
            left_argument = toVectorGrid(std::move(left_argument), context);
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
            String left_grid = context.subqueries.back().name;

            bool metric_name_dropped_from_left = left_argument.metric_name_dropped;
            ASTPtr left_join_group = transformGroupASTForBinaryOperator(
                operator_node,
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                /* drop_metric_name = */ true,
                metric_name_dropped_from_left);
            metric_name_dropped_from_join_group |= metric_name_dropped_from_left;

            SelectQueryBuilder left_builder;
            ASTPtr left_original_group = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            left_original_group->setAlias(ColumnNames::OriginalGroup);
            left_builder.select_list.push_back(std::move(left_original_group));
            left_builder.select_list.push_back(left_join_group);
            left_builder.select_list.back()->setAlias(ColumnNames::JoinGroup);
            left_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
            left_builder.from_table = left_grid;

            ASTPtr left_ast = left_builder.getSelectQuery();
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_ast), SQLSubqueryType::MATERIALIZED_TABLE});
            left = context.subqueries.back().name;

            bool metric_name_dropped_from_right = right_argument.metric_name_dropped;
            ASTPtr right_join_group = transformGroupASTForBinaryOperator(
                operator_node,
                make_intrusive<ASTIdentifier>(ColumnNames::Group),
                /* drop_metric_name = */ true,
                metric_name_dropped_from_right);
            metric_name_dropped_from_join_group |= metric_name_dropped_from_right;

            SelectQueryBuilder filter_builder;
            filter_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
            filter_builder.from_table = left;
            auto filter_subquery = make_intrusive<ASTSubquery>(filter_builder.getSelectQuery());
            ASTPtr filter_condition = makeASTFunction("in", right_join_group->clone(), std::move(filter_subquery));

            /// Push down join_group restriction into selector and range-aggregation stages of right side.
            if (right_argument.select_query)
            {
                if (auto * select_query = right_argument.select_query->as<ASTSelectQuery>())
                {
                    if (auto * tables = select_query->tables())
                    {
                        if (!tables->children.empty())
                        {
                            if (auto * elem = tables->children[0]->as<ASTTablesInSelectQueryElement>())
                            {
                                if (auto * table_expr = elem->table_expression ? elem->table_expression->as<ASTTableExpression>() : nullptr)
                                {
                                    if (auto * table_id = table_expr->database_and_table_name ? table_expr->database_and_table_name->as<ASTTableIdentifier>() : nullptr)
                                    {
                                        String from_table_name = table_id->shortName();
                                        for (auto & subq : context.subqueries)
                                        {
                                            if (subq.name == from_table_name && subq.ast)
                                            {
                                                if (auto * inner_select = subq.ast->as<ASTSelectQuery>())
                                                {
                                                    ASTPtr inner_where = inner_select->getExpression(ASTSelectQuery::Expression::WHERE);
                                                    if (inner_where)
                                                        inner_select->setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", inner_where, filter_condition->clone()));
                                                    else
                                                        inner_select->setExpression(ASTSelectQuery::Expression::WHERE, filter_condition->clone());
                                                }
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }

                    ASTPtr existing_where = select_query->getExpression(ASTSelectQuery::Expression::WHERE);
                    if (existing_where)
                        select_query->setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", existing_where, filter_condition->clone()));
                    else
                        select_query->setExpression(ASTSelectQuery::Expression::WHERE, filter_condition->clone());
                }
            }

            right_argument = toVectorGrid(std::move(right_argument), context);
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
            String right_grid = context.subqueries.back().name;

            SelectQueryBuilder right_builder;
            bool check_side_one = (tryGetIdentifierName(right_join_group.get()) != ColumnNames::Group);

            ASTPtr original_group = make_intrusive<ASTIdentifier>(ColumnNames::Group);
            if (check_side_one)
                original_group = makeASTFunction("any", std::move(original_group));
            original_group->setAlias(ColumnNames::OriginalGroup);
            right_builder.select_list.push_back(std::move(original_group));

            right_builder.select_list.push_back(right_join_group);
            right_builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

            ASTPtr values = make_intrusive<ASTIdentifier>(ColumnNames::Values);
            if (check_side_one)
            {
                values = makeASTFunction("any", std::move(values));
                values->setAlias(ColumnNames::Values);
            }
            right_builder.select_list.push_back(std::move(values));
            right_builder.from_table = right_grid;

            if (check_side_one)
            {
                right_builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                right_builder.having = makeASTFunction(
                    "equals",
                    makeASTFunction(
                        "timeSeriesThrowDuplicateSeriesIf",
                        makeASTFunction("greater", makeASTFunction("count"), make_intrusive<ASTLiteral>(1u)),
                        make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup)),
                    make_intrusive<ASTLiteral>(0u));
            }

            /// Filter right-side join_groups by left side in Step 2.
            right_builder.where = filter_condition;

            ASTPtr right_ast = right_builder.getSelectQuery();
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_ast), SQLSubqueryType::TABLE});
            right = context.subqueries.back().name;
        }
        else
        {
            String sides[2];

            left_argument = toVectorGrid(std::move(left_argument), context);
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(left_argument.select_query), SQLSubqueryType::TABLE});
            sides[0] = context.subqueries.back().name;

            right_argument = toVectorGrid(std::move(right_argument), context);
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(right_argument.select_query), SQLSubqueryType::TABLE});
            sides[1] = context.subqueries.back().name;

            for (size_t i = 0; i < 2; ++i)
            {
                SelectQueryBuilder builder;

                bool metric_name_dropped_from_group = (i == 0) ? left_argument.metric_name_dropped : right_argument.metric_name_dropped;
                bool metric_name_dropped_from_join_group_on_side = metric_name_dropped_from_group;

                ASTPtr join_group = transformGroupASTForBinaryOperator(
                    operator_node,
                    make_intrusive<ASTIdentifier>(ColumnNames::Group),
                    /* drop_metric_name = */ true,
                    metric_name_dropped_from_join_group_on_side);

                metric_name_dropped_from_join_group |= metric_name_dropped_from_join_group_on_side;

                bool group_on_side = (i == 0) ? group_left : group_right;
                bool check_side_one = !group_on_side && (tryGetIdentifierName(join_group.get()) != ColumnNames::Group);

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

                builder.from_table = sides[i];

                if (check_side_one)
                {
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

                if (i == 0)
                    left = context.subqueries.back().name;
                else
                    right = context.subqueries.back().name;
            }
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
