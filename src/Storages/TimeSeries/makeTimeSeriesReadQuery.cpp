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

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
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

    /// Builds `(SELECT id, groupArray((timestamp, value)) AS time_series FROM <samples> GROUP BY id)`
    /// wrapped in ASTSelectWithUnionQuery so it can sit inside an ASTSubquery in a JOIN.
    ASTPtr makeSamplesGroupedSubquery(const StorageID & samples_table_id)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        select_list->children.push_back(makeTimeSeriesAggregate());
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(samples_table_id);
        table_exp->children.push_back(table_exp->database_and_table_name);

        auto table_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        table_elem->table_expression = table_exp;
        table_elem->children.push_back(table_exp);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(table_elem);
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        auto group_by = make_intrusive<ASTExpressionList>();
        group_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);

        return wrapInUnionQuery(std::move(select_query));
    }

    /// Builds `SELECT groupArray((timestamp, value)) AS time_series FROM <samples> GROUP BY id`
    /// — the samples-only branch (no Tags involvement).
    ASTPtr makeSamplesOnlySelect(const StorageID & samples_table_id)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(makeTimeSeriesAggregate());
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(samples_table_id);
        table_exp->children.push_back(table_exp->database_and_table_name);

        auto table_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        table_elem->table_expression = table_exp;
        table_elem->children.push_back(table_exp);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(table_elem);
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        auto group_by = make_intrusive<ASTExpressionList>();
        group_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        select_query->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);

        return wrapInUnionQuery(std::move(select_query));
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
    /// array and keeps only the "tags" rows that have samples. The alias is required by
    /// `joined_subquery_requires_alias` (on by default).
    ASTPtr makeSamplesSemiJoinElement(const StorageID & samples_table_id)
    {
        auto samples_exp = make_intrusive<ASTTableExpression>();
        samples_exp->subquery = make_intrusive<ASTSubquery>(makeSamplesGroupedSubquery(samples_table_id));
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
        const ASTPtr & index_filter)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, makeJoinedSelectList(column_names, tag_columns));

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(makeTagsTableElement(tags_table_id, index_filter));
        if (samples_table_id)
            tables->children.push_back(makeSamplesSemiJoinElement(*samples_table_id));
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

    bool need_samples = requested.contains(TimeSeriesColumnNames::TimeSeries);

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

    /// Single-table reads (no join).
    if (!need_samples && !need_metrics)
        return makeTagsOnlySelect(storage.getTargetTableID(ViewTarget::Tags, context), requested,
                                  tag_columns, tags_index_filter);
    if (need_samples && !need_tags && !need_metrics)
        return makeSamplesOnlySelect(storage.getTargetTableID(ViewTarget::Samples, context));
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
                               tag_columns, tags_index_filter);
}

}
