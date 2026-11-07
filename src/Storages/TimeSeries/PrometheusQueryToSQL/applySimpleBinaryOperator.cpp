#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleBinaryOperator.h>

#include <Common/Exception.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/ASTWindowDefinition.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/ConverterContext.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/SelectQueryBuilder.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/applySimpleFunction.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/dropMetricName.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/makeNoDuplicateSeriesPerStepCheck.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/toVectorGrid.h>
#include <Storages/TimeSeries/PrometheusQueryToSQL/transformGroupASTForBinaryOperator.h>
#include <algorithm>
#include <tuple>


namespace DB::ErrorCodes
{
    extern const int CANNOT_EXECUTE_PROMQL_QUERY;
}


namespace DB::PrometheusQueryToSQL
{

namespace
{
    /// Collects all ASTSelectQuery instances by unwrapping ASTSelectWithUnionQuery layers.
    void collectSelectQueries(const ASTPtr & ast, std::vector<ASTSelectQuery *> & result)
    {
        if (!ast)
            return;
        if (auto * select = ast->as<ASTSelectQuery>())
        {
            result.push_back(select);
        }
        else if (auto * select_with_union = ast->as<ASTSelectWithUnionQuery>())
        {
            if (select_with_union->list_of_selects)
            {
                for (const auto & child : select_with_union->list_of_selects->children)
                    collectSelectQueries(child, result);
            }
        }
    }

    /// Checks if a node preserves the time-series group without dropping or modifying labels.
    bool isGroupPreservingNode(const PrometheusQueryTree::Node * node)
    {
        if (!node)
            return false;

        using NodeType = PrometheusQueryTree::NodeType;
        if (node->node_type == NodeType::InstantSelector || node->node_type == NodeType::RangeSelector)
            return true;

        if (node->node_type == NodeType::Offset)
            return !node->children.empty() && isGroupPreservingNode(node->children[0]);

        if (node->node_type == NodeType::Subquery)
        {
            const auto * subq = static_cast<const PrometheusQueryTree::Subquery *>(node);
            return isGroupPreservingNode(subq->getExpression());
        }

        if (node->node_type == NodeType::Function)
        {
            const auto * func = static_cast<const PrometheusQueryTree::Function *>(node);
            if (func->function_name == "last_over_time")
                return !func->children.empty() && isGroupPreservingNode(func->children[0]);
        }

        return false;
    }

    /// arrayExists(x -> isNotNull(x), values)
    /// A row without any value is not a sample, e.g. a row of `requests > 1000` where every value is filtered out.
    ASTPtr makeHasValueCondition()
    {
        return makeASTFunction(
            "arrayExists",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x")),
                makeASTFunction("isNotNull", make_intrusive<ASTIdentifier>("x"))),
            make_intrusive<ASTIdentifier>(ColumnNames::Values));
    }

    /// Dynamic filter pushdown for group_left: adds `right_join_group IN (SELECT join_group FROM left)` to the selector
    /// and range-aggregation stages of the right side (`right_grid`), so the right side reads only the series which can match.
    void pushDownLeftJoinGroups(
        const String & right_grid, const ASTPtr & right_join_group, const String & left, ConverterContext & context)
    {
        /// SELECT join_group FROM left WHERE arrayExists(x -> isNotNull(x), values)
        SelectQueryBuilder filter_builder;
        filter_builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
        filter_builder.from_table = left;
        filter_builder.where = makeHasValueCondition();
        auto filter_subquery = make_intrusive<ASTSubquery>(filter_builder.getSelectQuery());
        ASTPtr filter_condition = makeASTFunction("in", right_join_group->clone(), std::move(filter_subquery));

        std::unordered_set<String> visited_subqueries{right_grid};
        std::vector<ASTSelectQuery *> group_queries;

        auto push_down_to_ast = [&](auto & self, const ASTPtr & query_ast) -> void
        {
            std::vector<ASTSelectQuery *> select_queries;
            collectSelectQueries(query_ast, select_queries);
            for (auto * select_query : select_queries)
            {
                bool has_group = false;
                if (auto select_list = select_query->select())
                {
                    for (const auto & col : select_list->children)
                    {
                        if (col->tryGetAlias() == ColumnNames::Group)
                        {
                            has_group = true;
                            break;
                        }
                        if (auto * id = col->as<ASTIdentifier>())
                        {
                            if (id->shortName() == ColumnNames::Group)
                            {
                                has_group = true;
                                break;
                            }
                        }
                    }
                }

                if (!has_group)
                    continue;

                group_queries.push_back(select_query);

                if (auto tables = select_query->tables())
                {
                    for (const auto & table_child : tables->children)
                    {
                        if (auto * elem = table_child->as<ASTTablesInSelectQueryElement>())
                        {
                            if (auto * table_expr = elem->table_expression ? elem->table_expression->as<ASTTableExpression>() : nullptr)
                            {
                                if (auto * table_id = table_expr->database_and_table_name ? table_expr->database_and_table_name->as<ASTTableIdentifier>() : nullptr)
                                {
                                    String from_table_name = table_id->shortName();
                                    if (visited_subqueries.insert(from_table_name).second)
                                    {
                                        for (auto & subq : context.subqueries)
                                        {
                                            if (subq.name == from_table_name && subq.ast)
                                            {
                                                self(self, subq.ast);
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        };

        for (auto & subq : context.subqueries)
        {
            if (subq.name == right_grid)
            {
                push_down_to_ast(push_down_to_ast, subq.ast);
                break;
            }
        }

        for (auto * select_query : group_queries)
        {
            ASTPtr existing_where = select_query->where();
            if (existing_where)
                select_query->setExpression(ASTSelectQuery::Expression::WHERE, makeASTFunction("and", existing_where, filter_condition->clone()));
            else
                select_query->setExpression(ASTSelectQuery::Expression::WHERE, filter_condition->clone());
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

    /// Properties of a side of a binary operator applied to two vectors, see `applyOperatorToVectors`.
    struct SideProperties
    {
        /// Name of the subquery with the rows of this side.
        String table_name;

        /// The metric name `__name__` is already dropped from `group` of the argument.
        bool metric_name_dropped_from_argument = false;

        /// `group` with the `on(tags)` or `ignoring(tags)` matching rules applied, the key of the JOIN at step 3.
        /// Basically it's either timeSeriesRemoveAllTagsExcept(group, <on_tags>) or timeSeriesRemoveTags(group, <ignoring_tags>).
        ASTPtr join_group;

        /// Normally the metric name `__name__` isn't in `join_group` - it's dropped,
        /// unless it's `on(__name__, ...)` which keeps it.
        bool metric_name_dropped_from_join_group = false;

        /// The rows of the argument are known to be unique by `join_group`,
        /// because `join_group` is the same as `group`.
        bool is_join_group_unique = false;

        /// The column `original_group` is selected at steps 1-2 and used at step 3.
        /// It represents the original group of this argument in case we need it to calculate the result.
        bool use_original_group = false;

        /// The column `extra_labels_group` is selected at steps 1-2 and used at step 3.
        /// It represents a group made of extra labels taken from this argument
        /// in case it's the side "one" of group_left/group_right operation with extra labels.
        bool use_extra_labels_group = false;

        /// The column `value_counts` is selected at steps 1-2 and used at step 3: how many series of this side with the same
        /// `join_group` have a value at each step. It lets step 3 throw an exception if two or more of them match the same
        /// series of the other side at a step where that series has a value too. Duplicates at other steps are allowed.
        bool use_value_counts = false;

        /// Series of the side "one" with the same `join_group` (and `extra_labels_group`) are merged at steps 1-2
        /// to reduce the rows of the JOIN.
        bool merge_by_join_group = false;
    };

    /// Stores an argument as a vector grid in a new subquery and calculates the properties of this side.
    SideProperties getSideProperties(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        bool drop_metric_name,
        bool left_side,
        SQLQueryPiece && argument,
        ConverterContext & context)
    {
        SideProperties side;

        argument = toVectorGrid(std::move(argument), context);
        context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(argument.select_query), SQLSubqueryType::TABLE});
        side.table_name = context.subqueries.back().name;
        side.metric_name_dropped_from_argument = argument.metric_name_dropped;

        bool group_left = operator_node->group_left;
        bool group_right = operator_node->group_right;
        bool one_to_one = !group_left && !group_right;

        /// If neither group_left nor group_right is specified then it's one-to-one match, and both sides are "one".
        /// If there is group_left then it's many-to-one match, and the left side is "many".
        /// If there is group_right then it's one-to-many match, and the right side is "many".
        bool is_side_many = left_side ? group_left : group_right;
        bool is_side_one = !is_side_many;

        /// Apply the `on(tags)` or `ignoring(tags)` matching rules to the `group` column.
        /// The join_group is always computed with `drop_metric_name=true` because the two sides typically
        /// have different metric names (e.g., `foo` and `bar`), so keeping `__name__` in the JOIN key
        /// would prevent any matches.
        /// The exception is `on(__name__, ...)`, handled inside transformGroupASTForBinaryOperator.
        side.metric_name_dropped_from_join_group = side.metric_name_dropped_from_argument;
        side.join_group = transformGroupASTForBinaryOperator(
            operator_node,
            make_intrusive<ASTIdentifier>(ColumnNames::Group),
            /* drop_metric_name = */ true,
            side.metric_name_dropped_from_join_group);

        /// If `join_group` is the same as `group` then we know it's unique.
        side.is_join_group_unique = (tryGetIdentifierName(side.join_group.get()) == ColumnNames::Group);

        /// `original_group` is `group` of the argument, it's selected at steps 1-2 and used at step 3 to make the result group:
        /// - on the side "many" always,
        /// - on the left side of a one-to-one match if the metric name should be kept in the result but was dropped from `join_group`
        ///   (e.g. `a > b`: `join_group` has no `__name__` while the result must keep `__name__` of `a`).
        ///   That's not the case with `on(...)` because then the result has only the listed labels, i.e. it's `join_group`.
        side.use_original_group = is_side_many
            || (one_to_one && left_side && !operator_node->on && !drop_metric_name && !side.metric_name_dropped_from_argument
                && side.metric_name_dropped_from_join_group);

        /// The extra labels are copied from the side "one" of a many-to-one match.
        side.use_extra_labels_group = is_side_one && !one_to_one && !operator_node->extra_labels.empty();

        /// A side "one" can have several series with the same `join_group`, they're checked at step 3.
        side.use_value_counts = is_side_one && !side.is_join_group_unique;

        /// A side is merged by `join_group` unless its `original_group` is needed for the result group
        /// (the left side of a comparison which keeps the metric name): then its series can get different result groups.
        side.merge_by_join_group = side.use_value_counts && !side.use_original_group;

        return side;
    }

    /// Tags copied from the side "one" to the result group by `group_left(tags)` or `group_right(tags)`.
    std::vector<std::string_view> getTagsToCopy(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        bool allow_grouping_modifier_copy_metric_name)
    {
        const auto & extra_labels = operator_node->extra_labels;
        std::vector<std::string_view> tags_to_copy = {extra_labels.begin(), extra_labels.end()};
        std::sort(tags_to_copy.begin(), tags_to_copy.end());
        tags_to_copy.erase(std::unique(tags_to_copy.begin(), tags_to_copy.end()), tags_to_copy.end());
        if (!allow_grouping_modifier_copy_metric_name)
        {
            auto it = std::lower_bound(tags_to_copy.begin(), tags_to_copy.end(), kMetricName);
            if (it != tags_to_copy.end() && *it == kMetricName)
                tags_to_copy.erase(it);
        }
        return tags_to_copy;
    }

    /// Makes `extra_labels_group` of the side "one" of a many-to-one match: `group` reduced to the extra labels copied to the result.
    ASTPtr getExtraLabelsGroup(const std::vector<std::string_view> & tags_to_copy)
    {
        /// timeSeriesRemoveAllTagsExcept(group, extra_labels)
        return makeASTFunction(
            "timeSeriesRemoveAllTagsExcept",
            make_intrusive<ASTIdentifier>(ColumnNames::Group),
            make_intrusive<ASTLiteral>(Array{tags_to_copy.begin(), tags_to_copy.end()}));
    }

    /// Makes the result group of a one-to-one match (neither `group_left` nor `group_right` is specified).
    /// Sets `check_no_duplicate_groups` if two rows of the JOIN can get the same result group.
    ASTPtr makeResultGroupForOneToOne(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        bool drop_metric_name,
        const String & left_table,
        bool left_table_has_original_group,
        bool metric_name_dropped_from_left_argument,
        bool metric_name_dropped_from_join_group,
        bool & metric_name_dropped_from_result,
        bool & check_no_duplicate_groups)
    {
        chassert(!operator_node->group_left && !operator_node->group_right);

        ASTPtr new_group;
        if (left_table_has_original_group)
        {
            /// We can't use `join_group` as the result group in case when
            /// the metric name `__name__` should be preserved in the result but it has already been dropped from `join_group`.
            /// Example 1. `foo == ignoring(size) bar`
            /// - here the result should have only `size` removed, but `join_group` has both `size` and `__name__` removed,
            /// so we have to recompute it from the `original_group` by removing only `size`.
            /// Example 2. `foo == bar`
            /// - here the result should have all the tags of `foo`, but `join_group` has `__name__` removed,
            /// so we take the original group from the left argument.
            metric_name_dropped_from_result = metric_name_dropped_from_left_argument;
            new_group = transformGroupASTForBinaryOperator(
                operator_node,
                make_intrusive<ASTIdentifier>(Strings{left_table, ColumnNames::OriginalGroup}),
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

        return new_group;
    }

    /// Makes the result group of a many-to-one match (either `group_left` or `group_right` is specified):
    /// `original_group` of the side "many" without the metric name, plus the extra labels copied from the side "one".
    /// Sets `check_no_duplicate_groups` if two rows of the JOIN can get the same result group.
    ASTPtr makeResultGroupForManyToOne(
        const PrometheusQueryTree::BinaryOperator * operator_node,
        const std::vector<std::string_view> & tags_to_copy,
        bool drop_metric_name,
        const String & left_table,
        bool metric_name_dropped_from_left_argument,
        const String & right_table,
        bool metric_name_dropped_from_right_argument,
        bool & metric_name_dropped_from_result,
        bool & check_no_duplicate_groups)
    {
        bool group_left = operator_node->group_left;
        chassert(group_left != operator_node->group_right);

        /// There are two sides: "one" and "many".
        const String & side_many = group_left ? left_table : right_table;
        const String & side_one = group_left ? right_table : left_table;
        bool metric_name_dropped_from_side_many
            = group_left ? metric_name_dropped_from_left_argument : metric_name_dropped_from_right_argument;
        bool metric_name_dropped_from_side_one
            = group_left ? metric_name_dropped_from_right_argument : metric_name_dropped_from_left_argument;

        /// Drop the metric name from the side "many".
        ASTPtr new_group = make_intrusive<ASTIdentifier>(Strings{side_many, ColumnNames::OriginalGroup});
        metric_name_dropped_from_result = metric_name_dropped_from_side_many;

        if (drop_metric_name && !metric_name_dropped_from_result)
        {
            new_group = makeASTFunction("timeSeriesRemoveTag", new_group, make_intrusive<ASTLiteral>(kMetricName));
            metric_name_dropped_from_result = true;
            check_no_duplicate_groups = true;
        }

        /// Add extra labels from the side "one".
        if (!operator_node->extra_labels.empty())
        {
            /// `tags_to_copy` has `__name__` only if copying the metric name is allowed.
            if (std::binary_search(tags_to_copy.begin(), tags_to_copy.end(), kMetricName) && !metric_name_dropped_from_side_one)
                metric_name_dropped_from_result = false;

            new_group = makeASTFunction(
                "timeSeriesCopyTags",
                new_group,
                make_intrusive<ASTIdentifier>(Strings{side_one, ColumnNames::ExtraLabelsGroup}),
                make_intrusive<ASTLiteral>(Array{tags_to_copy.begin(), tags_to_copy.end()}));

            check_no_duplicate_groups = true;
        }

        return new_group;
    }

    /// Chooses the kind and the strictness of the JOIN at step 3.
    /// A side has several rows with the same `join_group` if it isn't merged or if it's merged by `join_group` and `extra_labels_group`,
    /// then LEFT/RIGHT SEMI JOIN (one such side) or INNER ALL JOIN (both sides) is used to get all the pairs.
    std::pair<JoinKind, JoinStrictness> getJoinKindAndStrictness(
        bool left_has_multiple_rows_per_join_group,
        bool right_has_multiple_rows_per_join_group)
    {
        if (left_has_multiple_rows_per_join_group && right_has_multiple_rows_per_join_group)
        {
            /// Both sides can have multiple rows with the same `join_group`, so we need all the pairs.
            return {JoinKind::Inner, JoinStrictness::All};
        }

        if (left_has_multiple_rows_per_join_group || right_has_multiple_rows_per_join_group)
        {
            /// Only one side can have multiple rows with the same `join_group`, each row of it matches at most one row of the other side.
            return {left_has_multiple_rows_per_join_group ? JoinKind::Left : JoinKind::Right, JoinStrictness::Semi};
        }

        /// `join_group` is unique on both sides, so each row matches at most one row.
        return {JoinKind::Inner, JoinStrictness::Any};
    }

    /// arrayMap(x, y -> f(x, y), left.values, right.values)
    ASTPtr makeResultValues(
        const std::function<ASTPtr(ASTPtr, ASTPtr)> & apply_function_to_ast, const String & left_table, const String & right_table)
    {
        return makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y")),
                apply_function_to_ast(make_intrusive<ASTIdentifier>("x"), make_intrusive<ASTIdentifier>("y"))),
            make_intrusive<ASTIdentifier>(Strings{left_table, ColumnNames::Values}),
            make_intrusive<ASTIdentifier>(Strings{right_table, ColumnNames::Values}));
    }

    /// arrayMap(c -> toUInt8(least(c, 2)), <counts>)
    /// How many series of the side with the same `join_group` have a value at each step, capped at 2 because only 0, 1 and 2 matter
    /// for the check at step 3. The counts are
    /// - countForEach(<side>.values) if the series are merged by `join_group`
    ///   (the alias `values` of the merged values hides the source column, so it's referred to as `<side>.values`);
    /// - sumForEach(countForEach(<side>.values)) OVER (PARTITION BY join_group) if they're merged by `join_group` and
    ///   `extra_labels_group`, because series with the same `join_group` are duplicates whatever labels they copy to the result;
    /// - countForEach(<side>.values) OVER (PARTITION BY join_group) if they aren't merged
    ///   (the left side of a comparison which keeps the metric name).
    ASTPtr makeValueCounts(
        const String & table_name,
        bool use_extra_labels_group,
        bool merged_by_join_group)
    {
        auto counts = makeASTFunction("countForEach", make_intrusive<ASTIdentifier>(Strings{table_name, ColumnNames::Values}));
        if (merged_by_join_group && use_extra_labels_group)
            counts = makeASTFunction("sumForEach", std::move(counts));

        if (!merged_by_join_group || use_extra_labels_group)
        {
            /// <counts> OVER (PARTITION BY join_group)
            auto partition_by = make_intrusive<ASTExpressionList>();
            partition_by->children.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));

            auto window_definition = make_intrusive<ASTWindowDefinition>();
            window_definition->partition_by = partition_by;
            window_definition->children.push_back(std::move(partition_by));

            counts->setIsWindowFunction(true);
            counts->setKind(ASTFunction::Kind::WINDOW_FUNCTION);
            counts->window_definition = window_definition;
            counts->children.push_back(std::move(window_definition));
        }

        return makeASTFunction(
            "arrayMap",
            makeASTFunction(
                "lambda",
                makeASTFunction("tuple", make_intrusive<ASTIdentifier>("c")),
                makeASTFunction("toUInt8", makeASTFunction("least", make_intrusive<ASTIdentifier>("c"), make_intrusive<ASTLiteral>(2u)))),
            std::move(counts));
    }

    /// Makes the WHERE condition of the JOIN at step 3 which throws an exception if two matched rows have more than one pair
    /// of series with values at the same step. The check uses `value_counts` of a side which has them,
    /// and the values of a side which has one series per row.
    ///
    /// Duplicates on the side which must be unique (the right side of a one-to-one match,
    /// the side "one" of group_left / group_right) are reported only at steps
    /// where a matched series of the other side has a value.
    /// It's Prometheus behaviour with the bug https://github.com/prometheus/prometheus/issues/6743 fixed.
    ASTPtr makeNoDuplicatePerStepCheck(
        const String & left_table,
        bool left_table_has_value_counts,
        const String & right_table,
        bool right_table_has_value_counts)
    {
        if (!left_table_has_value_counts && !right_table_has_value_counts)
            return nullptr;

        /// The two sides have the same `join_group` after the JOIN, so either can be used for the error message.
        ASTPtr join_group = make_intrusive<ASTIdentifier>(Strings{left_table, ColumnNames::JoinGroup});

        if (left_table_has_value_counts && right_table_has_value_counts)
        {
            return makeNoDuplicateSeriesPerStepCheckMatchingCounts(
                make_intrusive<ASTIdentifier>(Strings{left_table, ColumnNames::ValueCounts}),
                make_intrusive<ASTIdentifier>(Strings{right_table, ColumnNames::ValueCounts}),
                std::move(join_group));
        }

        if (left_table_has_value_counts)
        {
            /// The right side has one series per row, so its values are used instead of the counts.
            return makeNoDuplicateSeriesPerStepCheckMatchingValuesAndCounts(
                make_intrusive<ASTIdentifier>(Strings{right_table, ColumnNames::Values}),
                make_intrusive<ASTIdentifier>(Strings{left_table, ColumnNames::ValueCounts}),
                std::move(join_group));
        }

        /// The same for the left side.
        return makeNoDuplicateSeriesPerStepCheckMatchingValuesAndCounts(
            make_intrusive<ASTIdentifier>(Strings{left_table, ColumnNames::Values}),
            make_intrusive<ASTIdentifier>(Strings{right_table, ColumnNames::ValueCounts}),
            std::move(join_group));
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

        SQLQueryPiece res{operator_node, operator_node->result_type, StoreMethod::VECTOR_GRID};
        res.start_time = left_argument.start_time;
        res.end_time = left_argument.end_time;
        res.step = left_argument.step;

        /// Only a right side which keeps the series of its selector can be filtered by the left join groups.
        bool right_is_group_preserving
            = (right_argument.store_method == StoreMethod::VECTOR_GRID) && isGroupPreservingNode(right_argument.node);

        /// For group_left the left join groups are pushed down into the right side.
        /// `on()` is skipped: its join group is always the zero group, so nothing could be filtered out.
        bool push_down_left_join_groups = operator_node->group_left && right_is_group_preserving
            && !(operator_node->on && operator_node->labels.empty());

        /// Both arguments are stored as vector grids in subqueries, the properties of the sides are calculated from them.
        SideProperties sides[2]
            = {getSideProperties(operator_node, drop_metric_name, /* left_side = */ true, std::move(left_argument), context),
               getSideProperties(operator_node, drop_metric_name, /* left_side = */ false, std::move(right_argument), context)};

        bool one_to_one = !operator_node->group_left && !operator_node->group_right;

        std::vector<std::string_view> tags_to_copy = getTagsToCopy(operator_node, allow_grouping_modifier_copy_metric_name);

        /// Steps 1-2:
        /// new_left / new_right:
        /// SELECT [group AS original_group,] [timeSeriesRemoveAllTagsExcept(group, extra_labels) AS extra_labels_group,]
        ///        timeSeriesRemoveAllTagsExcept(group, on_tags) AS join_group,
        ///        values | anyForEach(values) AS values
        ///        [, arrayMap(c -> toUInt8(least(c, 2)), <counts>) AS value_counts]
        /// FROM left / right
        /// [GROUP BY join_group [, extra_labels_group]]
        /// where <counts> is the number of series with a value at each step among the series with the same `join_group`.

        for (size_t side_index = 0; side_index != 2; ++side_index)
        {
            SideProperties & side = sides[side_index];
            SelectQueryBuilder builder;

            if ((side_index == 1) && push_down_left_join_groups)
                pushDownLeftJoinGroups(side.table_name, side.join_group, sides[0].table_name, context);

            if (side.use_original_group)
            {
                /// group AS original_group
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
                builder.select_list.back()->setAlias(ColumnNames::OriginalGroup);
            }

            if (side.use_extra_labels_group)
            {
                builder.select_list.push_back(getExtraLabelsGroup(tags_to_copy));
                builder.select_list.back()->setAlias(ColumnNames::ExtraLabelsGroup);
            }

            builder.select_list.push_back(side.join_group);
            builder.select_list.back()->setAlias(ColumnNames::JoinGroup);

            if (side.merge_by_join_group)
            {
                /// anyForEach(values) AS values
                ASTPtr values = makeNoDuplicateSeriesPerStepValues(make_intrusive<ASTIdentifier>(ColumnNames::Values));
                values->setAlias(ColumnNames::Values);
                builder.select_list.push_back(std::move(values));
            }
            else
            {
                builder.select_list.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Values));
            }

            if (side.use_value_counts)
            {
                builder.select_list.push_back(makeValueCounts(side.table_name, side.use_extra_labels_group, side.merge_by_join_group));
                builder.select_list.back()->setAlias(ColumnNames::ValueCounts);
            }

            builder.from_table = side.table_name;

            if (side.merge_by_join_group)
            {
                /// GROUP BY join_group [, extra_labels_group]
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::JoinGroup));
                if (side.use_extra_labels_group)
                    builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::ExtraLabelsGroup));
            }

            /// The left side is read again by the right side, so it must be evaluated once.
            auto subquery_type = ((side_index == 0) && push_down_left_join_groups) ? SQLSubqueryType::MATERIALIZED_TABLE : SQLSubqueryType::TABLE;

            ASTPtr ast = builder.getSelectQuery();
            context.subqueries.emplace_back(SQLSubquery{context.subqueries.size(), std::move(ast), subquery_type});

            side.table_name = context.subqueries.back().name;
        }

        /// Step 3:
        /// if without grouping:
        /// SELECT timeSeriesRemoveTag(join_group, '__name__') AS group,
        ///        arrayMap(x, y -> f(x, y), left.values, right.values) AS values
        /// FROM left INNER ANY | LEFT SEMI JOIN right
        /// ON left.join_group = right.join_group
        /// [WHERE timeSeriesThrowDuplicateSeriesIf(arrayExists((l, r) -> greatest(l, r) >= 2 AND least(l, r) >= 1,
        ///                                                    left.value_counts, right.value_counts), left.join_group) = 0]
        /// [GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), group) = 0]
        ///
        /// if with group_left/group_right:
        /// SELECT timeSeriesCopyTags(timeSeriesRemoveTag(side_many.original_group, '__name__'), side_one.extra_labels_group, extra_labels) AS group,
        ///        arrayMap(x, y -> f(x, y), left.values, right.values) AS values
        /// FROM left INNER ANY | LEFT/RIGHT SEMI | INNER ALL JOIN right
        /// ON left.join_group = right.join_group
        /// [WHERE timeSeriesThrowDuplicateSeriesIf(arrayExists((v, c) -> isNotNull(v) AND c >= 2,
        ///                                                    side_many.values, side_one.value_counts), left.join_group) = 0]
        /// [GROUP BY group HAVING timeSeriesThrowDuplicateSeriesIf(arrayExists(c -> c > 1, countForEach(values)), group) = 0]
        ///
        /// If the grouping is applied then `values` becomes `anyForEach(arrayMap(...))`: series with the same result `group`
        /// are merged step by step and an exception is thrown only if two of them have values at the same step.
        ASTPtr result_ast;
        bool metric_name_dropped_from_result = false;
        {
            SelectQueryBuilder builder;

            const String & left = sides[0].table_name;
            const String & right = sides[1].table_name;
            bool metric_name_dropped_from_join_group
                = sides[0].metric_name_dropped_from_join_group || sides[1].metric_name_dropped_from_join_group;

            /// A side has several rows with the same `join_group` after steps 1-2 if `join_group` isn't unique and the rows
            /// aren't merged by it alone: either the side isn't merged, or it's merged by `join_group` and `extra_labels_group`.
            bool left_has_multiple_rows_per_join_group
                = !sides[0].is_join_group_unique && (!sides[0].merge_by_join_group || sides[0].use_extra_labels_group);
            bool right_has_multiple_rows_per_join_group
                = !sides[1].is_join_group_unique && (!sides[1].merge_by_join_group || sides[1].use_extra_labels_group);

            /// Two rows of the JOIN can get the same result `group`, then they must be merged step by step.
            /// It happens if the left side of a one-to-one match has several rows with the same `join_group` (it isn't merged),
            /// the functions making the result group set it in other cases.
            bool check_no_duplicate_groups = one_to_one && left_has_multiple_rows_per_join_group;

            ASTPtr new_group;
            if (one_to_one)
            {
                chassert(!sides[1].use_original_group);
                new_group = makeResultGroupForOneToOne(
                    operator_node,
                    drop_metric_name,
                    left,
                    sides[0].use_original_group,
                    sides[0].metric_name_dropped_from_argument,
                    metric_name_dropped_from_join_group,
                    metric_name_dropped_from_result,
                    check_no_duplicate_groups);
            }
            else
            {
                new_group = makeResultGroupForManyToOne(
                    operator_node,
                    tags_to_copy,
                    drop_metric_name,
                    left,
                    sides[0].metric_name_dropped_from_argument,
                    right,
                    sides[1].metric_name_dropped_from_argument,
                    metric_name_dropped_from_result,
                    check_no_duplicate_groups);
            }

            builder.select_list.push_back(std::move(new_group));
            builder.select_list.back()->setAlias(ColumnNames::Group);

            ASTPtr values = makeResultValues(apply_function_to_ast, left, right);

            ASTPtr values_for_check;
            if (check_no_duplicate_groups)
            {
                values_for_check = values->clone();
                values = makeNoDuplicateSeriesPerStepValues(std::move(values));
            }

            builder.select_list.push_back(std::move(values));
            builder.select_list.back()->setAlias(ColumnNames::Values);

            builder.from_table = left;
            builder.join_table = right;

            builder.join_on = makeASTFunction(
                "equals",
                make_intrusive<ASTIdentifier>(Strings{left, ColumnNames::JoinGroup}),
                make_intrusive<ASTIdentifier>(Strings{right, ColumnNames::JoinGroup}));

            std::tie(builder.join_kind, builder.join_strictness)
                = getJoinKindAndStrictness(left_has_multiple_rows_per_join_group, right_has_multiple_rows_per_join_group);

            builder.where = makeNoDuplicatePerStepCheck(left, sides[0].use_value_counts, right, sides[1].use_value_counts);

            if (check_no_duplicate_groups)
            {
                builder.group_by.push_back(make_intrusive<ASTIdentifier>(ColumnNames::Group));
                builder.having = makeNoDuplicateSeriesPerStepCheck(std::move(values_for_check), make_intrusive<ASTIdentifier>(ColumnNames::Group));
            }

            result_ast = builder.getSelectQuery();
        }

        res.select_query = std::move(result_ast);
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
