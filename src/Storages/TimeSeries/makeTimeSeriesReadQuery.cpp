#include <Storages/TimeSeries/makeTimeSeriesReadQuery.h>

#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Core/Joins.h>
#include <Core/Names.h>
#include <Functions/IFunction.h>
#include <Interpreters/ActionsDAG.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Parsers/Prometheus/PrometheusQueryTree.h>
#include <Parsers/makeASTForLogicalFunction.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>

#include <optional>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
    extern const TimeSeriesSettingsBool filter_by_min_time_and_max_time;
    extern const TimeSeriesSettingsBool store_min_time_and_max_time;
}

namespace
{
    /// Wraps a single SELECT in an ASTSelectWithUnionQuery, which is the shape
    /// InterpreterSelectQueryAnalyzer accepts at the top level.
    ASTPtr wrapInUnionQuery(ASTPtr select_query)
    {
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));

        auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
        union_query->list_of_selects = list_of_selects;
        union_query->children.push_back(list_of_selects);
        return union_query;
    }

    /// Reads the `tags_to_columns` setting into a list of (tag_name, column_name) pairs — the tags that
    /// the "tags" table stores in their own columns instead of in the `tags` Map.
    std::vector<std::pair<String, String>> getPromotedTagColumns(const StorageTimeSeries & storage)
    {
        auto storage_settings = storage.getStorageSettings();
        const Map & tags_to_columns = (*storage_settings)[TimeSeriesSetting::tags_to_columns];

        std::vector<std::pair<String, String>> tag_columns;
        tag_columns.reserve(tags_to_columns.size());
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            tag_columns.emplace_back(tuple.at(0).safeGet<String>(), tuple.at(1).safeGet<String>());
        }
        return tag_columns;
    }

    /// Builds the outer `tags` column: combines the inner `tags` Map, the metric name (as the `__name__`
    /// tag), and the tags promoted to their own columns by the `tags_to_columns` setting into one
    /// Map(String, String), sorted by tag name with duplicates and empty values removed.
    ASTPtr makeNormalizedTagsColumn(const std::vector<std::pair<String, String>> & tag_columns)
    {
        ASTs args;
        args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
        args.push_back(make_intrusive<ASTLiteral>(String{TimeSeriesTagNames::MetricName}));
        args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
        for (const auto & [tag_name, column_name] : tag_columns)
        {
            args.push_back(make_intrusive<ASTLiteral>(tag_name));
            args.push_back(make_intrusive<ASTIdentifier>(column_name));
        }

        auto tags = makeASTFunction("timeSeriesNormalizeTags", std::move(args));
        tags->setAlias(TimeSeriesColumnNames::Tags);
        return tags;
    }

    /// Builds the outer `metric_name` column. The metric name is normally stored in the inner `metric_name`
    /// column, but it can instead live in the `tags` Map under `__name__` (e.g. a row inserted directly into
    /// the inner tags table with an empty `metric_name` column), so fall back to that.
    ASTPtr makeMetricNameColumn()
    {
        auto from_tags = makeASTFunction("arrayElement",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags),
            make_intrusive<ASTLiteral>(String{TimeSeriesTagNames::MetricName}));
        auto metric_name = makeASTFunction("if",
            makeASTFunction("empty", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName)),
            std::move(from_tags),
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
        metric_name->setAlias(TimeSeriesColumnNames::MetricName);
        return metric_name;
    }

    ASTPtr makeTagsTableElement(const StorageID & tags_table_id, const ASTPtr & index_filter);

    /// Builds an index-usable predicate to push onto the raw "tags" table scan, from the query filter's
    /// top-level equality conjuncts on key-like columns:
    ///   - `metric_name = <const>`            -> `metric_name IN ('', <consts>)`
    ///   - `tags['<tag>'] = <const>`, where `<tag>` is promoted to its own column via `tags_to_columns`,
    ///                                        -> `<column> IN ('', <consts>)`
    ///   - `timeSeriesSelectorMatchTags('<selector>', ...)` -> the same, built from the equality (`=`)
    ///                                        matchers of the constant PromQL selector (regex and negative
    ///                                        matchers can't use an index and are ignored)
    /// Conditions on different columns are combined with AND. The empty string keeps rows whose value is
    /// stored in the `tags` Map instead of the column; correctness is still guaranteed by the outer filter
    /// on the reconstructed columns, so this only needs to be a sound over-approximation. A column is only
    /// used by the primary key when it is part of the "tags" table's sorting key. Returns nullptr when no
    /// such conjunct is found.
    ASTPtr makeTagsTableIndexFilter(const ActionsDAG * filter_actions_dag,
                                    const std::vector<std::pair<String, String>> & tag_columns)
    {
        if (!filter_actions_dag || filter_actions_dag->getOutputs().empty())
            return nullptr;

        auto unwrap_alias = [](const ActionsDAG::Node * node)
        {
            while (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
                node = node->children.front();
            return node;
        };

        /// Input column names can be qualified (e.g. `__table1.metric_name`).
        auto input_is = [&](const ActionsDAG::Node * node, std::string_view column_name)
        {
            return node->type == ActionsDAG::ActionType::INPUT
                && (node->result_name == column_name || node->result_name.ends_with(String(".") + String(column_name)));
        };
        auto get_const_string = [](const ActionsDAG::Node * node, String & out)
        {
            if (node->type != ActionsDAG::ActionType::COLUMN || !node->column)
                return false;
            Field value = (*node->column)[0];
            if (value.getType() != Field::Types::String)
                return false;
            out = value.safeGet<String>();
            return true;
        };

        std::unordered_map<String, String> column_by_tag_name;
        for (const auto & [tag_name, column_name] : tag_columns)
            column_by_tag_name[tag_name] = column_name;

        /// Inner column name -> the constant values the filter restricts it to. Ordered for stable output.
        std::map<String, std::vector<String>> values_by_column;

        /// Records that the tag `tag_name` is restricted to `value`, if it maps to an indexable column
        /// (`__name__` -> the "metric_name" column; a promoted tag -> its column; otherwise ignored).
        auto record_tag_value = [&](const String & tag_name, const String & value)
        {
            if (tag_name == TimeSeriesTagNames::MetricName)
                values_by_column[TimeSeriesColumnNames::MetricName].push_back(value);
            else if (auto it = column_by_tag_name.find(tag_name); it != column_by_tag_name.end())
                values_by_column[it->second].push_back(value);
        };

        /// Records the equality matchers of a constant PromQL selector (the argument of
        /// timeSeriesSelectorMatchTags); an unparseable selector simply disables the push-down.
        auto record_selector_matchers = [&](const String & selector)
        {
            PrometheusQueryTree query_tree;
            if (!query_tree.tryParse(selector))
                return;
            const auto * root = query_tree.getRoot();
            if (!root || root->node_type != PrometheusQueryTree::NodeType::InstantSelector)
                return;
            for (const auto & matcher : static_cast<const PrometheusQueryTree::InstantSelector &>(*root).matchers)
            {
                if (matcher.matcher_type == PrometheusQueryTree::MatcherType::EQ)
                    record_tag_value(matcher.label_name, matcher.label_value);
            }
        };

        for (const auto * atom : ActionsDAG::extractConjunctionAtoms(filter_actions_dag->getOutputs().front()))
        {
            const auto * node = unwrap_alias(atom);
            if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base)
                continue;
            const auto function_name = node->function_base->getName();

            /// `metric_name = <value>` or `tags['<tag>'] = <value>`.
            if (function_name == "equals" && node->children.size() == 2)
            {
                const auto * lhs = unwrap_alias(node->children[0]);
                const auto * rhs = unwrap_alias(node->children[1]);

                /// One side must be a String constant (the compared value).
                String value;
                const ActionsDAG::Node * expr = nullptr;
                if (get_const_string(rhs, value))
                    expr = lhs;
                else if (get_const_string(lhs, value))
                    expr = rhs;
                else
                    continue;

                if (input_is(expr, TimeSeriesColumnNames::MetricName))
                {
                    record_tag_value(TimeSeriesTagNames::MetricName, value);
                }
                else if (expr->type == ActionsDAG::ActionType::FUNCTION && expr->function_base
                    && expr->function_base->getName() == "arrayElement" && expr->children.size() == 2)
                {
                    String tag_name;
                    if (input_is(unwrap_alias(expr->children[0]), TimeSeriesColumnNames::Tags)
                        && get_const_string(unwrap_alias(expr->children[1]), tag_name))
                        record_tag_value(tag_name, value);
                }
            }
            /// `timeSeriesSelectorMatchTags('<selector>', ...)` -> the selector's equality matchers.
            else if (function_name == "timeSeriesSelectorMatchTags" && !node->children.empty())
            {
                String selector;
                if (get_const_string(unwrap_alias(node->children[0]), selector))
                    record_selector_matchers(selector);
            }
        }

        if (values_by_column.empty())
            return nullptr;

        /// AND over columns of `<column> IN ('', <values>)`.
        ASTs conditions;
        for (const auto & [column_name, values] : values_by_column)
        {
            ASTs tuple_args;
            tuple_args.push_back(make_intrusive<ASTLiteral>(Field{String{}}));
            for (const auto & value : values)
                tuple_args.push_back(make_intrusive<ASTLiteral>(Field{value}));
            conditions.push_back(makeASTFunction("in",
                make_intrusive<ASTIdentifier>(column_name),
                makeASTFunction("tuple", std::move(tuple_args))));
        }
        return makeASTForLogicalAnd(std::move(conditions));
    }

    /// Follows ALIAS nodes down to the underlying node.
    const ActionsDAG::Node * unwrapAlias(const ActionsDAG::Node * node)
    {
        while (node->type == ActionsDAG::ActionType::ALIAS && !node->children.empty())
            node = node->children.front();
        return node;
    }

    /// Whether `node` is the input column `column_name` (possibly qualified, e.g. `__table1.timestamp`).
    bool isInputNamed(const ActionsDAG::Node * node, std::string_view column_name)
    {
        node = unwrapAlias(node);
        return node->type == ActionsDAG::ActionType::INPUT
            && (node->result_name == column_name || node->result_name.ends_with(String(".") + String(column_name)));
    }

    /// Whether the input column `column_name` is referenced anywhere in the subtree of `node`.
    bool referencesInput(const ActionsDAG::Node * node, std::string_view column_name)
    {
        if (isInputNamed(node, column_name))
            return true;
        for (const auto * child : node->children)
            if (referencesInput(child, column_name))
                return true;
        return false;
    }

    /// Whether any input column other than `allowed_column` is referenced in the subtree of `node`.
    bool referencesOtherInput(const ActionsDAG::Node * node, std::string_view allowed_column)
    {
        if (node->type == ActionsDAG::ActionType::INPUT && !isInputNamed(node, allowed_column))
            return true;
        for (const auto * child : node->children)
            if (referencesOtherInput(child, allowed_column))
                return true;
        return false;
    }

    /// Reads a constant value from a constant node.
    bool getConstField(const ActionsDAG::Node * node, Field & out)
    {
        node = unwrapAlias(node);
        if (node->type != ActionsDAG::ActionType::COLUMN || !node->column)
            return false;
        out = (*node->column)[0];
        return true;
    }

    /// Reconstructs the AST of an expression over the `timestamp` column so it can be pushed onto the "samples"
    /// table, rebinding the `timestamp` input to that table's `timestamp` column. The expression must be
    /// deterministic within the query and depend only on `timestamp` and constants; otherwise the representative
    /// timestamp wouldn't be guaranteed to satisfy the outer condition, so we fail closed.
    ASTPtr timestampConditionToSamplesAST(const ActionsDAG::Node * node)
    {
        node = unwrapAlias(node);
        switch (node->type)
        {
            case ActionsDAG::ActionType::INPUT:
                if (isInputNamed(node, TimeSeriesColumnNames::Timestamp))
                    return make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp);
                break;
            case ActionsDAG::ActionType::COLUMN:
                if (node->column && isColumnConst(*node->column))
                    return make_intrusive<ASTLiteral>((*node->column)[0]);
                break;
            case ActionsDAG::ActionType::FUNCTION:
                if (node->function_base && node->function_base->isDeterministicInScopeOfQuery())
                {
                    ASTs args;
                    args.reserve(node->children.size());
                    for (const auto * child : node->children)
                        args.push_back(timestampConditionToSamplesAST(child));
                    return makeASTFunction(node->function_base->getName(), std::move(args));
                }
                break;
            default:
                break;
        }

        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "A condition on the `timestamp` column of a TimeSeries table can only be pushed down when it is a "
            "deterministic expression over `timestamp` and constants");
    }

    /// If `node` is a `timestamp <cmp> <const>` comparison (cmp one of `=`, `<`, `<=`, `>`, `>=`, in either
    /// argument order), appends the matching coarse bound on the "tags" table's `min_time`/`max_time` columns:
    /// a series can hold a sample at time `t` only when `min_time <= t <= max_time`. Does nothing for any other
    /// shape — the samples-table predicate still enforces correctness; this is only a series-pruning optimization.
    void addTagsRangePredicate(const ActionsDAG::Node * node, ASTs & tags_conditions)
    {
        /// Maps a comparison to the one with the arguments swapped (for `<const> <cmp> timestamp`).
        static const std::unordered_map<String, String> flipped_comparison = {
            {"equals", "equals"},
            {"less", "greater"}, {"lessOrEquals", "greaterOrEquals"},
            {"greater", "less"}, {"greaterOrEquals", "lessOrEquals"}};

        if (node->type != ActionsDAG::ActionType::FUNCTION || !node->function_base || node->children.size() != 2)
            return;

        const auto * lhs = unwrapAlias(node->children[0]);
        const auto * rhs = unwrapAlias(node->children[1]);
        String op = node->function_base->getName();
        Field value;

        if (isInputNamed(lhs, TimeSeriesColumnNames::Timestamp) && getConstField(rhs, value))
        {
            if (!flipped_comparison.contains(op))
                return;
        }
        else if (isInputNamed(rhs, TimeSeriesColumnNames::Timestamp) && getConstField(lhs, value))
        {
            auto it = flipped_comparison.find(op);
            if (it == flipped_comparison.end())
                return;
            op = it->second;
        }
        else
        {
            return;
        }

        if (op == "greater" || op == "greaterOrEquals" || op == "equals")
            tags_conditions.push_back(makeASTFunction("greaterOrEquals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MaxTime),
                make_intrusive<ASTLiteral>(value)));
        if (op == "less" || op == "lessOrEquals" || op == "equals")
            tags_conditions.push_back(makeASTFunction("lessOrEquals",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MinTime),
                make_intrusive<ASTLiteral>(value)));
    }

    /// Splits a condition on the `timestamp` virtual column out of the query filter into a predicate for the
    /// "samples" table and one for the "tags" table. `timestamp` is filter-only (it never appears in the
    /// output). Each top-level conjunct that mentions `timestamp` must depend only on `timestamp` and constants
    /// (and be deterministic); such a conjunct is pushed onto the "samples" table verbatim, so the time series
    /// array keeps only matching samples. A conjunct that mixes `timestamp` with other columns (for example
    /// `timestamp > C OR metric_name = '...'`) can't be turned into a samples-table predicate and throws.
    /// `tags_time_filter` is a sound over-approximation on the `min_time`/`max_time` columns used to skip whole
    /// series, derived only from `timestamp <cmp> <const>` comparisons. Both outputs are null when the filter
    /// doesn't mention `timestamp`.
    void extractTimestampFilters(const ActionsDAG * filter_actions_dag, ASTPtr & samples_filter, ASTPtr & tags_time_filter)
    {
        samples_filter = nullptr;
        tags_time_filter = nullptr;
        if (!filter_actions_dag || filter_actions_dag->getOutputs().empty())
            return;

        ASTs samples_conditions;
        ASTs tags_conditions;

        for (const auto * atom : ActionsDAG::extractConjunctionAtoms(filter_actions_dag->getOutputs().front()))
        {
            const auto * node = unwrapAlias(atom);
            if (!referencesInput(node, TimeSeriesColumnNames::Timestamp))
                continue;

            if (referencesOtherInput(node, TimeSeriesColumnNames::Timestamp))
                throw Exception(ErrorCodes::NOT_IMPLEMENTED,
                    "A condition on the `timestamp` column of a TimeSeries table that also depends on other "
                    "columns can't be pushed down");

            samples_conditions.push_back(timestampConditionToSamplesAST(node));
            addTagsRangePredicate(node, tags_conditions);
        }

        if (!samples_conditions.empty())
            samples_filter = makeASTForLogicalAnd(std::move(samples_conditions));
        if (!tags_conditions.empty())
            tags_time_filter = makeASTForLogicalAnd(std::move(tags_conditions));
    }

    /// Combines two optional filter expressions with AND. Either may be null.
    ASTPtr combineFilters(ASTPtr first, ASTPtr second)
    {
        if (!first)
            return second;
        if (!second)
            return first;
        return makeASTForLogicalAnd(ASTs{std::move(first), std::move(second)});
    }

    /// Builds `SELECT <requested columns> FROM <tags_table_id>` — only the columns the caller
    /// asked for via `column_names` are returned.
    ASTPtr makeTagsOnlySelect(const StorageID & tags_table_id, const NameSet & column_names,
                              const std::vector<std::pair<String, String>> & tag_columns,
                              const ASTPtr & index_filter)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();

        if (column_names.contains(TimeSeriesColumnNames::MetricName))
            select_list->children.push_back(makeMetricNameColumn());
        if (column_names.contains(TimeSeriesColumnNames::Tags))
            select_list->children.push_back(makeNormalizedTagsColumn(tag_columns));

        /// If none of the "tags" table's columns are requested, we fall back to `1`.
        /// So for example `SELECT count() FROM time_series` is evaluated as
        /// `SELECT count() FROM (SELECT 1 FROM tags)`.
        if (select_list->children.empty())
            select_list->children.push_back(make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(makeTagsTableElement(tags_table_id, index_filter));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return wrapInUnionQuery(std::move(select_query));
    }

    /// Builds the `groupArray((timestamp, value)) AS time_series` aggregate expression used by
    /// the samples-side branches.
    ASTPtr makeTimeSeriesAggregate()
    {
        auto group_array = makeASTFunction("groupArray",
            makeASTFunction("tuple",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp),
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value)));
        group_array->setAlias(TimeSeriesColumnNames::TimeSeries);
        return group_array;
    }

    /// The internal alias for the representative timestamp inside a samples sub-select. It deliberately
    /// differs from `timestamp` so that `WHERE timestamp ...` and `groupArray((timestamp, value))` in the
    /// same sub-select still bind to the raw samples column rather than to this aggregate; a projection above
    /// renames it to `timestamp`.
    constexpr const char * representative_timestamp_alias = "__rep_timestamp";

    /// Builds `any(timestamp) AS __rep_timestamp`: a representative timestamp from a series' (already
    /// time-filtered) samples. The `timestamp` virtual column never appears in the output on its own, but the
    /// planner still re-applies the original `timestamp` condition on top of the storage read, so the read must
    /// expose a value that satisfies it. Every surviving sample does (the same condition was pushed onto the
    /// samples), so any one of them works.
    ASTPtr makeRepresentativeTimestamp()
    {
        auto any_timestamp = makeASTFunction("any", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp));
        any_timestamp->setAlias(representative_timestamp_alias);
        return any_timestamp;
    }

    /// Adds the per-series samples columns to a samples-side SELECT list: `groupArray((timestamp, value)) AS
    /// time_series` when the time series array is requested, and the representative `timestamp` when the
    /// filter-only `timestamp` column is requested.
    void addSamplesSelectList(ASTExpressionList & select_list, bool need_time_series, bool need_timestamp)
    {
        if (need_time_series)
            select_list.children.push_back(makeTimeSeriesAggregate());
        if (need_timestamp)
            select_list.children.push_back(makeRepresentativeTimestamp());
    }

    /// Sets `FROM <samples_table_id> [WHERE <samples_filter>] GROUP BY id` on a samples-side SELECT.
    void setSamplesFromGroupBy(ASTSelectQuery & select_query, const StorageID & samples_table_id, const ASTPtr & samples_filter)
    {
        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(samples_table_id);
        table_exp->children.push_back(table_exp->database_and_table_name);

        auto table_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        table_elem->table_expression = table_exp;
        table_elem->children.push_back(table_exp);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(table_elem);
        select_query.setExpression(ASTSelectQuery::Expression::TABLES, tables);

        if (samples_filter)
            select_query.setExpression(ASTSelectQuery::Expression::WHERE, samples_filter->clone());

        auto group_by = make_intrusive<ASTExpressionList>();
        group_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        select_query.setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);
    }

    /// Builds `(SELECT id, groupArray((timestamp, value)) AS time_series [, any(timestamp) AS __rep_timestamp]
    /// FROM <samples> [WHERE <samples_filter>] GROUP BY id)` wrapped in ASTSelectWithUnionQuery so it can sit
    /// inside an ASTSubquery in a JOIN. `id` is always selected as the join key.
    ASTPtr makeSamplesGroupedSubquery(const StorageID & samples_table_id, const ASTPtr & samples_filter,
                                      bool need_time_series, bool need_timestamp)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        addSamplesSelectList(*select_list, need_time_series, need_timestamp);
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        setSamplesFromGroupBy(*select_query, samples_table_id, samples_filter);
        return wrapInUnionQuery(std::move(select_query));
    }

    /// Builds the samples-only branch (no Tags involvement):
    /// `SELECT groupArray((timestamp, value)) AS time_series [, any(timestamp) AS __rep_timestamp]
    ///  FROM <samples> [WHERE <samples_filter>] GROUP BY id`.
    /// When the representative timestamp is needed, an extra projection renames `__rep_timestamp` to the
    /// requested `timestamp` column in a scope that doesn't reference the raw `timestamp` column.
    ASTPtr makeSamplesOnlySelect(const StorageID & samples_table_id, const ASTPtr & samples_filter,
                                 bool need_time_series, bool need_timestamp)
    {
        auto grouped = make_intrusive<ASTSelectQuery>();
        auto grouped_list = make_intrusive<ASTExpressionList>();
        addSamplesSelectList(*grouped_list, need_time_series, need_timestamp);
        grouped->setExpression(ASTSelectQuery::Expression::SELECT, grouped_list);
        setSamplesFromGroupBy(*grouped, samples_table_id, samples_filter);

        if (!need_timestamp)
            return wrapInUnionQuery(std::move(grouped));

        auto outer = make_intrusive<ASTSelectQuery>();
        auto outer_list = make_intrusive<ASTExpressionList>();
        if (need_time_series)
            outer_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
        auto timestamp = make_intrusive<ASTIdentifier>(representative_timestamp_alias);
        timestamp->setAlias(TimeSeriesColumnNames::Timestamp);
        outer_list->children.push_back(std::move(timestamp));
        outer->setExpression(ASTSelectQuery::Expression::SELECT, outer_list);

        auto grouped_exp = make_intrusive<ASTTableExpression>();
        grouped_exp->subquery = make_intrusive<ASTSubquery>(wrapInUnionQuery(std::move(grouped)));
        grouped_exp->subquery->setAlias("__samples");
        grouped_exp->children.push_back(grouped_exp->subquery);
        auto grouped_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        grouped_elem->table_expression = grouped_exp;
        grouped_elem->children.push_back(grouped_exp);
        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(grouped_elem);
        outer->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return wrapInUnionQuery(std::move(outer));
    }

    /// Builds the SELECT list of a multi-table read: one entry per requested outer column. `time_series`
    /// and the metadata columns are resolved from the joined "samples"/"metrics" sub-selects, so the caller
    /// must join those tables whenever the corresponding columns are requested.
    ASTPtr makeJoinedSelectList(const NameSet & column_names, const std::vector<std::pair<String, String>> & tag_columns)
    {
        auto select_list = make_intrusive<ASTExpressionList>();
        if (column_names.contains(TimeSeriesColumnNames::MetricName))
            select_list->children.push_back(makeMetricNameColumn());
        if (column_names.contains(TimeSeriesColumnNames::Tags))
            select_list->children.push_back(makeNormalizedTagsColumn(tag_columns));
        if (column_names.contains(TimeSeriesColumnNames::TimeSeries))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
        /// The filter-only `timestamp` column, when requested, is the representative from the joined samples
        /// (selected there as `__rep_timestamp` to avoid clashing with the raw column).
        if (column_names.contains(TimeSeriesColumnNames::Timestamp))
        {
            auto timestamp = make_intrusive<ASTIdentifier>(representative_timestamp_alias);
            timestamp->setAlias(TimeSeriesColumnNames::Timestamp);
            select_list->children.push_back(std::move(timestamp));
        }
        if (column_names.contains(TimeSeriesColumnNames::MetricFamily))
        {
            auto metric_family = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName);
            metric_family->setAlias(TimeSeriesColumnNames::MetricFamily);
            select_list->children.push_back(std::move(metric_family));
        }
        if (column_names.contains(TimeSeriesColumnNames::Type))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Type));
        if (column_names.contains(TimeSeriesColumnNames::Unit))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Unit));
        if (column_names.contains(TimeSeriesColumnNames::Help))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Help));
        return select_list;
    }

    /// Builds the `FROM <tags_table_id>` element that the multi-table reads are anchored on. When
    /// `index_filter` is set, the table is wrapped in `(SELECT * FROM tags WHERE <index_filter>) AS __tags`
    /// so the predicate is applied at the tags scan and its primary key can skip granules.
    ASTPtr makeTagsTableElement(const StorageID & tags_table_id, const ASTPtr & index_filter)
    {
        auto tags_exp = make_intrusive<ASTTableExpression>();

        if (index_filter)
        {
            auto inner = make_intrusive<ASTSelectQuery>();

            auto select_list = make_intrusive<ASTExpressionList>();
            select_list->children.push_back(make_intrusive<ASTAsterisk>());
            inner->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

            auto inner_exp = make_intrusive<ASTTableExpression>();
            inner_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(tags_table_id);
            inner_exp->children.push_back(inner_exp->database_and_table_name);
            auto inner_elem = make_intrusive<ASTTablesInSelectQueryElement>();
            inner_elem->table_expression = inner_exp;
            inner_elem->children.push_back(inner_exp);
            auto inner_tables = make_intrusive<ASTTablesInSelectQuery>();
            inner_tables->children.push_back(inner_elem);
            inner->setExpression(ASTSelectQuery::Expression::TABLES, inner_tables);

            inner->setExpression(ASTSelectQuery::Expression::WHERE, index_filter->clone());

            tags_exp->subquery = make_intrusive<ASTSubquery>(wrapInUnionQuery(std::move(inner)));
            tags_exp->subquery->setAlias("__tags");
            tags_exp->children.push_back(tags_exp->subquery);
        }
        else
        {
            tags_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(tags_table_id);
            tags_exp->children.push_back(tags_exp->database_and_table_name);
        }

        auto tags_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        tags_elem->table_expression = tags_exp;
        tags_elem->children.push_back(tags_exp);
        return tags_elem;
    }

    /// Builds `SEMI LEFT JOIN (samples grouped by id) AS __samples USING id` — attaches the `time_series`
    /// array (and the representative `timestamp`) and keeps only the "tags" rows that have samples in range.
    /// The alias is required by `joined_subquery_requires_alias` (on by default).
    ASTPtr makeSamplesSemiJoinElement(const StorageID & samples_table_id, const ASTPtr & samples_filter,
                                      bool need_time_series, bool need_timestamp)
    {
        auto samples_exp = make_intrusive<ASTTableExpression>();
        samples_exp->subquery = make_intrusive<ASTSubquery>(
            makeSamplesGroupedSubquery(samples_table_id, samples_filter, need_time_series, need_timestamp));
        samples_exp->subquery->setAlias("__samples");
        samples_exp->children.push_back(samples_exp->subquery);

        auto join = make_intrusive<ASTTableJoin>();
        join->kind = JoinKind::Left;
        join->strictness = JoinStrictness::Semi;
        auto using_list = make_intrusive<ASTExpressionList>();
        using_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        join->using_expression_list = using_list;
        join->children.push_back(join->using_expression_list);

        auto samples_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        samples_elem->table_join = join;
        samples_elem->table_expression = samples_exp;
        samples_elem->children.push_back(join);
        samples_elem->children.push_back(samples_exp);
        return samples_elem;
    }

    /// Builds `any(<column>) AS <column>` — picks one value per group for a non-key metadata column.
    ASTPtr makeAnyAggregate(const char * column_name)
    {
        auto func = makeASTFunction("any", make_intrusive<ASTIdentifier>(column_name));
        func->setAlias(column_name);
        return func;
    }

    /// Builds `SELECT <requested columns> FROM <metrics_table_id> GROUP BY metric_family_name`. The inner
    /// column `metric_family_name` is exposed under the outer column name `metric_family`. Grouping by the
    /// metric family collapses duplicate metadata rows to one row per family. The "metrics" table is a
    /// ReplacingMergeTree by default, but its engine is not guaranteed, so we deduplicate by aggregation
    /// rather than with FINAL.
    ASTPtr makeMetricsOnlySelect(const StorageID & metrics_table_id, const NameSet & column_names)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();

        if (column_names.contains(TimeSeriesColumnNames::MetricFamily))
        {
            auto metric_family = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName);
            metric_family->setAlias(TimeSeriesColumnNames::MetricFamily);
            select_list->children.push_back(std::move(metric_family));
        }
        if (column_names.contains(TimeSeriesColumnNames::Type))
            select_list->children.push_back(makeAnyAggregate(TimeSeriesColumnNames::Type));
        if (column_names.contains(TimeSeriesColumnNames::Unit))
            select_list->children.push_back(makeAnyAggregate(TimeSeriesColumnNames::Unit));
        if (column_names.contains(TimeSeriesColumnNames::Help))
            select_list->children.push_back(makeAnyAggregate(TimeSeriesColumnNames::Help));

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(metrics_table_id);
        table_exp->children.push_back(table_exp->database_and_table_name);

        auto table_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        table_elem->table_expression = table_exp;
        table_elem->children.push_back(table_exp);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(table_elem);
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        auto group_by = make_intrusive<ASTExpressionList>();
        group_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);

        return wrapInUnionQuery(std::move(select_query));
    }

    /// Builds `(SELECT metric_family_name, any(type) AS type, ... FROM <metrics> GROUP BY metric_family_name)`
    /// wrapped in ASTSelectWithUnionQuery so it can sit inside an ASTSubquery in a JOIN. Grouping by the
    /// metric family deduplicates the metadata rows (see makeMetricsOnlySelect). Besides `metric_family_name`
    /// (always selected — it is the join key) only the requested metadata columns are returned.
    ASTPtr makeDeduplicatedMetricsSubquery(const StorageID & metrics_table_id, const NameSet & column_names)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        if (column_names.contains(TimeSeriesColumnNames::Type))
            select_list->children.push_back(makeAnyAggregate(TimeSeriesColumnNames::Type));
        if (column_names.contains(TimeSeriesColumnNames::Unit))
            select_list->children.push_back(makeAnyAggregate(TimeSeriesColumnNames::Unit));
        if (column_names.contains(TimeSeriesColumnNames::Help))
            select_list->children.push_back(makeAnyAggregate(TimeSeriesColumnNames::Help));
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(metrics_table_id);
        table_exp->children.push_back(table_exp->database_and_table_name);

        auto table_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        table_elem->table_expression = table_exp;
        table_elem->children.push_back(table_exp);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(table_elem);
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        auto group_by = make_intrusive<ASTExpressionList>();
        group_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);

        return wrapInUnionQuery(std::move(select_query));
    }

    /// Builds `FULL JOIN (deduplicated metrics) AS __metrics ON metric_family_name = timeSeriesMetricNameToFamily(metric_name)`
    /// — attaches the metadata columns. The metric family computed from a time series' metric name links it
    /// to its metadata row. The FULL JOIN keeps every "tags" row (its metadata columns are empty when the
    /// family has no metadata row) and also every "metrics" row that no time series belongs to (its "tags"
    /// and "samples" columns are then empty). The alias is required by `joined_subquery_requires_alias`.
    ASTPtr makeMetricsFullJoinElement(const StorageID & metrics_table_id, const NameSet & column_names)
    {
        auto metrics_exp = make_intrusive<ASTTableExpression>();
        metrics_exp->subquery = make_intrusive<ASTSubquery>(makeDeduplicatedMetricsSubquery(metrics_table_id, column_names));
        metrics_exp->subquery->setAlias("__metrics");
        metrics_exp->children.push_back(metrics_exp->subquery);

        auto join = make_intrusive<ASTTableJoin>();
        join->kind = JoinKind::Full;
        join->strictness = JoinStrictness::All;
        join->on_expression = makeASTFunction("equals",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName),
            makeASTFunction("timeSeriesMetricNameToFamily",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName)));
        join->children.push_back(join->on_expression);

        auto metrics_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        metrics_elem->table_join = join;
        metrics_elem->table_expression = metrics_exp;
        metrics_elem->children.push_back(join);
        metrics_elem->children.push_back(metrics_exp);
        return metrics_elem;
    }

    /// Builds a multi-table read anchored on the "tags" table, optionally joining the "samples" table (for
    /// the `time_series` column) and/or the "metrics" table (for the metadata columns). The "tags" table is
    /// always read: it bridges "samples" (by `id`) and "metrics" (by the family computed from `metric_name`),
    /// so it is needed even when none of its own columns are selected.
    ASTPtr makeTagsBasedSelect(
        const StorageID & tags_table_id,
        const std::optional<StorageID> & samples_table_id,
        const std::optional<StorageID> & metrics_table_id,
        const NameSet & column_names,
        const std::vector<std::pair<String, String>> & tag_columns,
        const ASTPtr & tags_filter,
        const ASTPtr & samples_filter)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, makeJoinedSelectList(column_names, tag_columns));

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(makeTagsTableElement(tags_table_id, tags_filter));
        if (samples_table_id)
            tables->children.push_back(makeSamplesSemiJoinElement(*samples_table_id, samples_filter,
                column_names.contains(TimeSeriesColumnNames::TimeSeries),
                column_names.contains(TimeSeriesColumnNames::Timestamp)));
        if (metrics_table_id)
            tables->children.push_back(makeMetricsFullJoinElement(*metrics_table_id, column_names));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return wrapInUnionQuery(std::move(select_query));
    }
}


ASTPtr makeTimeSeriesReadQuery(
    const StorageTimeSeries & storage,
    const Names & column_names,
    const ContextPtr & context,
    const ActionsDAG * filter_actions_dag)
{
    NameSet requested{column_names.begin(), column_names.end()};

    /// Check which target tables are requested by outer columns.
    bool need_tags    = requested.contains(TimeSeriesColumnNames::MetricName)
                     || requested.contains(TimeSeriesColumnNames::Tags);

    /// `timestamp` is a filter-only virtual column. Both filtering and reading it require the "samples" table
    /// (to apply the time condition and to produce a representative value satisfying the outer condition).
    bool need_timestamp = requested.contains(TimeSeriesColumnNames::Timestamp);
    bool need_samples = requested.contains(TimeSeriesColumnNames::TimeSeries) || need_timestamp;

    bool need_metrics = requested.contains(TimeSeriesColumnNames::MetricFamily)
                     || requested.contains(TimeSeriesColumnNames::Type)
                     || requested.contains(TimeSeriesColumnNames::Unit)
                     || requested.contains(TimeSeriesColumnNames::Help);

    /// All-false default: read from the "tags" table so that `SELECT count() FROM time_series` returns
    /// the number of time series.
    if (!need_tags && !need_samples && !need_metrics)
        need_tags = true;

    auto tag_columns = getPromotedTagColumns(storage);

    /// Conditions on `metric_name` and promoted tag columns are pushed onto the "tags" table read so its
    /// primary key (or a custom sorting key over a promoted column) can skip granules.
    auto tags_index_filter = makeTagsTableIndexFilter(filter_actions_dag, tag_columns);

    /// A condition on the filter-only `timestamp` column becomes an exact predicate on the "samples" table
    /// and a coarse range predicate on the "tags" table.
    ASTPtr samples_filter;
    ASTPtr tags_time_filter;
    extractTimestampFilters(filter_actions_dag, samples_filter, tags_time_filter);

    /// The tags-table range pruning is valid only when the table stores and filters by min/max time.
    auto storage_settings = storage.getStorageSettings();
    if (tags_time_filter
        && !((*storage_settings)[TimeSeriesSetting::filter_by_min_time_and_max_time]
             && (*storage_settings)[TimeSeriesSetting::store_min_time_and_max_time]))
        tags_time_filter = nullptr;

    auto tags_filter = combineFilters(tags_index_filter, tags_time_filter);

    /// Single-table reads (no join).
    if (!need_samples && !need_metrics)
        return makeTagsOnlySelect(storage.getTargetTableID(ViewTarget::Tags, context), requested,
                                  tag_columns, tags_filter);
    if (need_samples && !need_tags && !need_metrics)
        return makeSamplesOnlySelect(storage.getTargetTableID(ViewTarget::Samples, context), samples_filter,
                                     requested.contains(TimeSeriesColumnNames::TimeSeries), need_timestamp);
    if (need_metrics && !need_tags && !need_samples)
        return makeMetricsOnlySelect(storage.getTargetTableID(ViewTarget::Metrics, context), requested);

    /// Multi-table reads anchored on the "tags" table: tags [+ samples] [+ metrics].
    auto tags_table_id = storage.getTargetTableID(ViewTarget::Tags, context);

    std::optional<StorageID> samples_table_id;
    if (need_samples)
        samples_table_id = storage.getTargetTableID(ViewTarget::Samples, context);

    std::optional<StorageID> metrics_table_id;
    if (need_metrics)
        metrics_table_id = storage.getTargetTableID(ViewTarget::Metrics, context);

    return makeTagsBasedSelect(tags_table_id, samples_table_id, metrics_table_id, requested,
                               tag_columns, tags_filter, samples_filter);
}

}
