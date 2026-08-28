#include <Storages/TimeSeries/makeASTSelectFromTimeSeries.h>

#include <Access/Common/RowPolicyDefs.h>
#include <Access/EnabledRowPolicies.h>
#include <Analyzer/ColumnNode.h>
#include <Analyzer/ConstantNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/IQueryTreeNode.h>
#include <Common/SettingsChanges.h>
#include <Core/Field.h>
#include <Interpreters/Context.h>
#include <Core/Joins.h>
#include <Core/Names.h>
#include <Parsers/ASTAsterisk.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/SelectQueryInfo.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>
#include <Storages/TimeSeries/TimeSeriesSettings.h>
#include <Storages/TimeSeries/TimeSeriesTagNames.h>

#include <optional>
#include <string_view>


namespace DB
{

namespace TimeSeriesSetting
{
    extern const TimeSeriesSettingsMap tags_to_columns;
}

namespace
{
    /// Aliases of the subqueries reading the inner tables in the generated query.
    constexpr const char * samples_subquery_alias = "__samples";
    constexpr const char * tags_subquery_alias = "__tags";
    constexpr const char * metrics_families_subquery_alias = "__metrics_families";
    constexpr const char * metric_families_with_all_suffixes_subquery_alias = "__metric_families_with_all_suffixes";

    /// Alias of `concat(metric_family_name, arrayJoin(timeSeriesMetricTypeToSuffixes(type)))`
    /// (see `makeMetricsFullJoinElement`).
    constexpr const char * metric_family_with_suffix_alias = "__metric_family_with_suffix";

    /// Maps each tag name to the inner "tags" column that stores it instead of the `tags` Map: every tag with its
    /// own column via the `tags_to_columns` setting, plus the `__name__` tag, which lives in the dedicated
    /// `metric_name` column. Including `__name__` lets callers treat it like any other column-backed tag.
    std::unordered_map<String, String> getColumnsByTags(const TimeSeriesSettings & storage_settings)
    {
        const Map & tags_to_columns = storage_settings[TimeSeriesSetting::tags_to_columns];

        std::unordered_map<String, String> columns_by_tags;
        columns_by_tags.reserve(tags_to_columns.size() + 1);
        columns_by_tags.emplace(TimeSeriesTagNames::MetricName, TimeSeriesColumnNames::MetricName);
        for (const auto & tag_name_and_column_name : tags_to_columns)
        {
            const auto & tuple = tag_name_and_column_name.safeGet<Tuple>();
            columns_by_tags.emplace(tuple.at(0).safeGet<String>(), tuple.at(1).safeGet<String>());
        }
        return columns_by_tags;
    }

    /// Recursively checks that every use of the `tags` column in an AST expression is `tags['<const>']`,
    /// and collects the constant keys into `requested_tags`. Returns false on any other use of `tags`.
    bool findRequestedTagsInAST(const IAST & node, NameSet & requested_tags)
    {
        /// Returns true if `ast` is an identifier meaning the `tags` column.
        auto is_tags_column = [](const IAST & ast)
        {
            /// The filter is applied to the columns this read returns, so an identifier `tags` can only mean the `tags` column.
            const auto * identifier = ast.as<ASTIdentifier>();
            return identifier && identifier->name() == TimeSeriesColumnNames::Tags;
        };

        if (const auto * function = node.as<ASTFunction>(); function && function->name == "arrayElement"
            && function->arguments && function->arguments->children.size() == 2
            && is_tags_column(*function->arguments->children[0]))
        {
            const auto * key = function->arguments->children[1]->as<ASTLiteral>();
            if (key && key->value.getType() == Field::Types::String)
            {
                requested_tags.insert(key->value.safeGet<String>());
                return true;  /// a keyed access — don't descend into the `tags` argument
            }
            return false;  /// arrayElement(tags, <non-const>) — key unknown
        }

        if (is_tags_column(node))
            return false;  /// `tags` used some other way (whole Map, mapKeys, ...)

        if (const auto * identifier = node.as<ASTIdentifier>())
        {
            /// A compound identifier can refer to the `tags` column too (e.g. `mytable.tags` or the
            /// subcolumn `tags.keys`); we can't be sure for it, so we return false.
            for (const auto & name_part : identifier->name_parts)
            {
                if (name_part == TimeSeriesColumnNames::Tags)
                    return false;
            }
        }

        for (const auto & child : node.children)
        {
            if (child && !findRequestedTagsInAST(*child, requested_tags))
                return false;
        }
        return true;
    }

    /// Recursively checks that every use of the storage's `tags` column is `arrayElement(tags, '<const>')`,
    /// and collects the constant keys into `requested_tags`. Returns false on any other use of `tags`.
    bool findRequestedTagsInQueryTree(const IQueryTreeNode & node, const IQueryTreeNode & column_source, NameSet & requested_tags)
    {
        /// Returns true if `checked_node` is this table's `tags` Map column (and not, say, a column named
        /// `tags` from a different table expression in the query).
        auto is_tags_column = [&](const IQueryTreeNode & checked_node)
        {
            const auto * column = checked_node.as<ColumnNode>();
            return column && column->getColumnName() == TimeSeriesColumnNames::Tags
                && column->getColumnSource().get() == &column_source;
        };

        if (const auto * function = node.as<FunctionNode>(); function && function->getFunctionName() == "arrayElement")
        {
            const auto & arguments = function->getArguments().getNodes();
            if (arguments.size() == 2 && is_tags_column(*arguments[0]))
            {
                const auto * key = arguments[1]->as<ConstantNode>();
                if (key && key->getValue().getType() == Field::Types::String)
                {
                    requested_tags.insert(key->getValue().safeGet<String>());
                    return true;  /// a keyed access — don't descend into the `tags` argument
                }
                return false;  /// arrayElement(tags, <non-const>) — key unknown
            }
        }

        if (is_tags_column(node))
            return false;  /// `tags` used some other way (whole Map, mapKeys, ...)

        for (const auto & child : node.getChildren())
        {
            /// A query-tree node can have null children (optional clauses); skip them.
            if (child && !findRequestedTagsInQueryTree(*child, column_source, requested_tags))
                return false;
        }
        return true;
    }

    /// If the query and the filters applied outside its query tree use the `tags` column only as
    /// `tags['<const key>']`, returns those keys, otherwise returns an empty set.
    /// This allows building a reduced `tags` Map, reading only the columns needed for those keys.
    /// The tags checked by row policy filters and the `additional_table_filters` setting are included,
    /// so such a filter gets real values and for example
    /// a row policy `tags['env'] != 'secret'` keeps filtering.
    NameSet findRequestedTags(const StorageTimeSeries & storage, const SelectQueryInfo & query_info, const ContextPtr & context)
    {
        NameSet requested_tags;

        /// First scan the query tree itself.
        if (!query_info.query_tree || !query_info.table_expression
            || !findRequestedTagsInQueryTree(*query_info.query_tree, *query_info.table_expression, requested_tags))
            return {};

        /// A row policy and the `additional_table_filters` setting are not part of the query tree, so their
        /// expressions are scanned separately. (`additional_result_filter` needs no scan: it sees only the
        /// query's result columns, so any use of `tags` in it is already visible to the query tree scan.)
        auto storage_id = storage.getStorageID();
        auto row_policy_filter = context->getRowPolicyFilter(
            storage_id.getDatabaseName(), storage_id.getTableName(), RowPolicyFilterType::SELECT_FILTER);
        if (row_policy_filter
            && (!row_policy_filter->expression || !findRequestedTagsInAST(*row_policy_filter->expression, requested_tags)))
            return {};

        /// The planner parses the `additional_table_filters` entry matching this table into
        /// `query_info.additional_filter_ast` before calling read.
        if (query_info.additional_filter_ast
            && !findRequestedTagsInAST(*query_info.additional_filter_ast, requested_tags))
            return {};

        return requested_tags;
    }

    /// Builds the `arrayZip(groupArray(timestamp), groupArray(value)) AS time_series` expression used by
    /// the samples-side branches. Both `groupArray` states are filled by the same aggregation in the same
    /// row order, so element i of both arrays comes from the same sample.
    /// This form is used instead of `groupArray(tuple(timestamp, value))` because `arrayZip` makes tuples
    /// without element names regardless of the `enable_named_columns_in_function_tuple` setting (which
    /// would give `tuple` named elements, mismatching the declared type of the `time_series` column),
    /// and because `groupArray` over a plain column is faster than over tuples.
    ASTPtr makeGroupArrayOfSamples()
    {
        auto array_zip = makeASTFunction("arrayZip",
            makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Timestamp)),
            makeASTFunction("groupArray", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Value)));
        array_zip->setAlias(TimeSeriesColumnNames::TimeSeries);
        return array_zip;
    }

    /// Returns an expression for the value of the tag `tag_name`.
    /// It is either `toString(ifNull(<tag_column_name>, ''))` (if `tag_column_name` is specified)
    /// or `toString(tags['<tag_name>'])`.
    ASTPtr makeExpressionForOuterTag(std::string_view tag_name, std::optional<std::string_view> tag_column_name)
    {
        ASTPtr value;

        /// ifNull(<tag_column>, '') is required because <tag_column> is allowed to be Nullable,
        /// and we need to match the type of tags['tag_name'] which is String.
        if (tag_column_name)
            value = makeASTFunction("ifNull",
                make_intrusive<ASTIdentifier>(String{*tag_column_name}), make_intrusive<ASTLiteral>(String{}));
        else
            value = makeASTFunction("arrayElement",
                make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags),
                make_intrusive<ASTLiteral>(tag_name));

        /// `toString` normalizes the value to `String` even when the source column has another string type
        /// (e.g. LowCardinality(String)); for a `String` source it just reuses the column.
        return makeASTFunction("toString", std::move(value));
    }

    /// Returns an expression for the outer `metric_name` column.
    ASTPtr makeExpressionForOuterMetricName()
    {
        auto metric_name = makeExpressionForOuterTag(TimeSeriesTagNames::MetricName, TimeSeriesColumnNames::MetricName);
        metric_name->setAlias(TimeSeriesColumnNames::MetricName);
        return metric_name;
    }

    /// Returns an expression for the outer `tags` column.
    /// It is either `map('<tag_name_1>', toString(ifNull(<tag_column_1>, '')), ...)`
    /// with only the requested tags (if `requested_tags` is not empty;
    /// or `timeSeriesTagsToMap(tags, '<tag_name_1>', <tag_column_1>, ...)` with all the tags.
    ASTPtr makeExpressionForOuterTags(const NameSet & requested_tags,
                                      const std::unordered_map<String, String> & columns_by_tags)
    {
        ASTPtr tags;
        if (!requested_tags.empty())
        {
            /// A reduced Map containing only the requested tags, each resolved from its cheapest source:
            /// it avoids reading the other tag columns and the per-row normalization while producing the same
            /// value for each requested tag. Used when the query touches `tags` only as `tags['<const key>']`.
            ASTs map_args;
            for (const auto & tag_name : requested_tags)
            {
                std::optional<std::string_view> tag_column_name;
                if (auto it = columns_by_tags.find(tag_name); it != columns_by_tags.end())
                    tag_column_name = it->second;

                map_args.push_back(make_intrusive<ASTLiteral>(tag_name));
                map_args.push_back(makeExpressionForOuterTag(tag_name, tag_column_name));
            }
            tags = makeASTFunction("map", std::move(map_args));
        }
        else
        {
            /// The full Map: combines the inner `tags` Map, the metric name (as the `__name__` tag), and the tags
            /// that have their own columns via the `tags_to_columns` setting into one Map(String, String),
            /// sorted by tag name with duplicates and empty values removed.
            ASTs args;
            args.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
            /// `columns_by_tags` already includes `__name__` -> `metric_name`.
            for (const auto & [tag_name, column_name] : columns_by_tags)
            {
                args.push_back(make_intrusive<ASTLiteral>(tag_name));
                args.push_back(make_intrusive<ASTIdentifier>(column_name));
            }
            tags = makeASTFunction("timeSeriesTagsToMap", std::move(args));
        }

        tags->setAlias(TimeSeriesColumnNames::Tags);
        return tags;
    }

    /// Wraps a single SELECT in an ASTSelectWithUnionQuery, which is the shape
    /// InterpreterSelectQueryAnalyzer accepts at the top level.
    ASTPtr makeSelectWithUnionQuery(ASTPtr select_query)
    {
        auto list_of_selects = make_intrusive<ASTExpressionList>();
        list_of_selects->children.push_back(std::move(select_query));

        auto union_query = make_intrusive<ASTSelectWithUnionQuery>();
        union_query->list_of_selects = list_of_selects;
        union_query->children.push_back(list_of_selects);
        return union_query;
    }

    /// Builds a plain-table `ASTTableExpression` referring to `table_id`.
    ASTPtr makeTableExpression(const StorageID & table_id)
    {
        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(table_id);
        table_exp->children.push_back(table_exp->database_and_table_name);
        return table_exp;
    }

    /// Builds a `FROM` element referring to the plain table `table_id`.
    ASTPtr makeTableElement(const StorageID & table_id)
    {
        auto table_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        table_elem->table_expression = makeTableExpression(table_id);
        table_elem->children.push_back(table_elem->table_expression);
        return table_elem;
    }

    /// Wraps a SELECT into a subquery `FROM` element `(<select_query>) AS <alias>`. The alias is required
    /// by `joined_subquery_requires_alias` when the element takes part in a JOIN.
    ASTPtr makeTableElementFromSubquery(ASTPtr select_query, const String & alias)
    {
        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->subquery = make_intrusive<ASTSubquery>(makeSelectWithUnionQuery(std::move(select_query)));
        table_exp->subquery->setAlias(alias);
        table_exp->children.push_back(table_exp->subquery);

        auto table_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        table_elem->table_expression = table_exp;
        table_elem->children.push_back(std::move(table_exp));
        return table_elem;
    }

    /// Builds a one-table FROM: an `ASTTablesInSelectQuery` with the table `table_id` as its single entry.
    ASTPtr makeSingleTableList(const StorageID & table_id)
    {
        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(makeTableElement(table_id));
        return tables;
    }

    /// Builds a subquery to read from the "samples" table:
    /// (
    ///     SELECT id, arrayZip(groupArray(timestamp), groupArray(value)) AS time_series
    ///     FROM <samples>
    ///     GROUP BY id
    /// ) AS __samples
    ASTPtr makeSamplesTableElement(const StorageID & samples_table_id)
    {
        auto inner = make_intrusive<ASTSelectQuery>();

        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        select_list->children.push_back(makeGroupArrayOfSamples());
        inner->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        inner->setExpression(ASTSelectQuery::Expression::TABLES, makeSingleTableList(samples_table_id));

        auto group_by = make_intrusive<ASTExpressionList>();
        group_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        inner->setExpression(ASTSelectQuery::Expression::GROUP_BY, group_by);

        return makeTableElementFromSubquery(std::move(inner), samples_subquery_alias);
    }

    /// Builds a subquery to read from the "tags" table. When `deduplicate_by_id` is set, it is
    /// (
    ///     SELECT *
    ///     FROM <tags>
    ///     LIMIT 1 BY id
    /// ) AS __tags
    /// Here `LIMIT 1 BY id` deduplicates series: unmerged AggregatingMergeTree
    /// parts can hold duplicate rows per series `id`.
    /// If `deduplicate_by_id` is false, the function just returns the plain table <tags>.
    ASTPtr makeTagsTableElement(const StorageID & tags_table_id, bool deduplicate_by_id)
    {
        if (!deduplicate_by_id)
            return makeTableElement(tags_table_id);

        auto inner = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(make_intrusive<ASTAsterisk>());
        inner->setExpression(ASTSelectQuery::Expression::SELECT, select_list);
        inner->setExpression(ASTSelectQuery::Expression::TABLES, makeSingleTableList(tags_table_id));

        inner->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));
        auto limit_by = make_intrusive<ASTExpressionList>();
        limit_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        inner->setExpression(ASTSelectQuery::Expression::LIMIT_BY, limit_by);

        return makeTableElementFromSubquery(std::move(inner), tags_subquery_alias);
    }

    /// Builds a subquery to read from the "metrics" table:
    /// (
    ///     SELECT *
    ///     FROM <metrics>
    ///     WHERE notEmpty(metric_family_name)
    ///     LIMIT 1 BY metric_family_name
    /// ) AS __metrics_families
    /// Here `LIMIT 1 BY` keeps one whole metadata row per family: metadata is re-inserted with every write and the
    /// "metrics" engine isn't guaranteed to be ReplacingMergeTree, so duplicates are expected at read time.
    /// Rows without a family name are skipped.
    ASTPtr makeMetricsTableElement(const StorageID & metrics_table_id)
    {
        auto inner = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();
        select_list->children.push_back(make_intrusive<ASTAsterisk>());
        inner->setExpression(ASTSelectQuery::Expression::SELECT, select_list);
        inner->setExpression(ASTSelectQuery::Expression::TABLES, makeSingleTableList(metrics_table_id));

        /// Rows without a family name are skipped (in the JOIN they could only expand to
        /// bare-suffix members, e.g. `_total`, that could match a valid metric `_total`).
        inner->setExpression(ASTSelectQuery::Expression::WHERE,
            makeASTFunction("notEmpty", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName)));

        inner->setExpression(ASTSelectQuery::Expression::LIMIT_BY_LENGTH, make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));
        auto limit_by = make_intrusive<ASTExpressionList>();
        limit_by->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName));
        inner->setExpression(ASTSelectQuery::Expression::LIMIT_BY, limit_by);

        return makeTableElementFromSubquery(std::move(inner), metrics_families_subquery_alias);
    }

    /// Builds the SELECT list of a multi-table read: one entry per requested outer column.
    ASTPtr makeJoinedSelectList(const NameSet & requested_columns, const NameSet & requested_tags,
                                const std::unordered_map<String, String> & columns_by_tags)
    {
        auto select_list = make_intrusive<ASTExpressionList>();
        if (requested_columns.contains(TimeSeriesColumnNames::MetricName))
            select_list->children.push_back(makeExpressionForOuterMetricName());
        if (requested_columns.contains(TimeSeriesColumnNames::Tags))
            select_list->children.push_back(makeExpressionForOuterTags(requested_tags, columns_by_tags));
        if (requested_columns.contains(TimeSeriesColumnNames::TimeSeries))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
        if (requested_columns.contains(TimeSeriesColumnNames::MetricFamily))
        {
            auto metric_family = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName);
            metric_family->setAlias(TimeSeriesColumnNames::MetricFamily);
            select_list->children.push_back(std::move(metric_family));
        }
        if (requested_columns.contains(TimeSeriesColumnNames::Type))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Type));
        if (requested_columns.contains(TimeSeriesColumnNames::Unit))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Unit));
        if (requested_columns.contains(TimeSeriesColumnNames::Help))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Help));
        return select_list;
    }

    /// Builds a JOIN clause for the "tags" table to join it to the "samples" table:
    /// SEMI LEFT JOIN tags USING id
    ASTPtr makeTagsSemiJoinElement(const StorageID & tags_table_id)
    {
        auto join = make_intrusive<ASTTableJoin>();
        join->kind = JoinKind::Left;
        join->strictness = JoinStrictness::Semi;
        auto using_list = make_intrusive<ASTExpressionList>();
        using_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::ID));
        join->using_expression_list = using_list;
        join->children.push_back(join->using_expression_list);

        /// Duplicate tags rows per `id` (unmerged parts) are harmless here,
        /// so the "tags" table is read without the `LIMIT 1 BY id` deduplication.
        auto tags_elem = makeTagsTableElement(tags_table_id, /* deduplicate_by_id= */ false);

        auto & tags_elem_ref = *tags_elem->as<ASTTablesInSelectQueryElement>();
        tags_elem_ref.table_join = join;
        tags_elem_ref.children.push_back(join);
        return tags_elem;
    }

    /// Builds a JOIN clause for the "metrics" table to join it to the "tags" table:
    /// FULL JOIN
    /// (
    ///     SELECT *, concat(metric_family_name, arrayJoin(timeSeriesMetricTypeToSuffixes(type))) AS __metric_family_with_suffix
    ///     FROM
    ///     (
    ///         SELECT *
    ///         FROM <metrics>
    ///         WHERE notEmpty(metric_family_name)
    ///         LIMIT 1 BY metric_family_name
    ///     ) AS __metrics_families
    /// ) AS __metric_families_with_all_suffixes
    /// ON toString(ifNull(metric_name, '')) = __metric_family_with_suffix
    ///
    /// The FULL JOIN keeps every "tags" row (its metadata columns are empty when no member matches) and also
    /// every "metrics" member row that no time series belongs to (its "tags"/"samples" columns are then empty).
    ASTPtr makeMetricsFullJoinElement(const StorageID & metrics_table_id)
    {
        auto expanded_list = make_intrusive<ASTExpressionList>();
        expanded_list->children.push_back(make_intrusive<ASTAsterisk>());

        auto metric_family_with_suffix = makeASTFunction("concat",
            make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName),
            makeASTFunction("arrayJoin",
                makeASTFunction("timeSeriesMetricTypeToSuffixes", make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Type))));
        metric_family_with_suffix->setAlias(metric_family_with_suffix_alias);
        expanded_list->children.push_back(std::move(metric_family_with_suffix));

        auto expanded = make_intrusive<ASTSelectQuery>();
        expanded->setExpression(ASTSelectQuery::Expression::SELECT, expanded_list);
        auto expanded_tables = make_intrusive<ASTTablesInSelectQuery>();
        expanded_tables->children.push_back(makeMetricsTableElement(metrics_table_id));
        expanded->setExpression(ASTSelectQuery::Expression::TABLES, expanded_tables);

        auto join = make_intrusive<ASTTableJoin>();
        join->kind = JoinKind::Full;
        join->strictness = JoinStrictness::All;
        join->on_expression = makeASTFunction("equals",
            makeExpressionForOuterTag(TimeSeriesTagNames::MetricName, TimeSeriesColumnNames::MetricName),
            make_intrusive<ASTIdentifier>(metric_family_with_suffix_alias));
        join->children.push_back(join->on_expression);

        auto metrics_elem = makeTableElementFromSubquery(std::move(expanded), metric_families_with_all_suffixes_subquery_alias);
        auto & metrics_elem_ref = *metrics_elem->as<ASTTablesInSelectQueryElement>();
        metrics_elem_ref.table_join = join;
        metrics_elem_ref.children.push_back(join);
        return metrics_elem;
    }

    /// Builds a query reading only from the "samples" table:
    /// SELECT time_series
    /// FROM
    /// (
    ///     SELECT id, arrayZip(groupArray(timestamp), groupArray(value)) AS time_series
    ///     FROM <samples>
    ///     GROUP BY id
    /// ) AS __samples
    ///
    /// Unlike the joined read (where the SEMI JOIN with the "tags" table drops them), this branch also returns
    /// samples whose id has no "tags" row - possible only after direct writes into the inner "samples" table.
    ASTPtr buildSelectQueryFromSamplesOnly(const StorageID & samples_table_id, const NameSet & requested_columns)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();

        if (requested_columns.contains(TimeSeriesColumnNames::TimeSeries))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));

        /// This branch is only taken when `time_series` is requested (see makeASTSelectFromTimeSeries).
        chassert(!select_list->children.empty());

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(makeSamplesTableElement(samples_table_id));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return makeSelectWithUnionQuery(std::move(select_query));
    }

    /// Builds a query reading only from the "tags" table, for example:
    /// SELECT toString(ifNull(metric_name, '')) AS metric_name,
    ///        timeSeriesTagsToMap(tags, '__name__', metric_name) AS tags
    /// FROM
    /// (
    ///     SELECT *
    ///     FROM <tags>
    ///     LIMIT 1 BY id
    /// ) AS __tags
    ///
    /// Only the requested columns are selected; if none of them is requested (e.g. for SELECT count()),
    /// the literal 1 is selected instead.
    ASTPtr buildSelectQueryFromTagsOnly(const StorageID & tags_table_id, const NameSet & requested_columns,
                                        const NameSet & requested_tags,
                                        const std::unordered_map<String, String> & columns_by_tags,
                                        bool deduplicate_tags_by_id)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();

        if (requested_columns.contains(TimeSeriesColumnNames::MetricName))
            select_list->children.push_back(makeExpressionForOuterMetricName());
        if (requested_columns.contains(TimeSeriesColumnNames::Tags))
            select_list->children.push_back(makeExpressionForOuterTags(requested_tags, columns_by_tags));

        /// If none of the "tags" table's columns are requested, we fall back to `1`.
        /// So for example `SELECT count() FROM time_series` is evaluated as
        /// `SELECT count() FROM (SELECT 1 FROM tags)`.
        if (select_list->children.empty())
            select_list->children.push_back(make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(makeTagsTableElement(tags_table_id, deduplicate_tags_by_id));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return makeSelectWithUnionQuery(std::move(select_query));
    }

    /// Builds a query reading only from the "metrics" table:
    /// SELECT metric_family_name AS metric_family, type, unit, help
    /// FROM
    /// (
    ///     SELECT *
    ///     FROM <metrics>
    ///     WHERE notEmpty(metric_family_name)
    ///     LIMIT 1 BY metric_family_name
    /// ) AS __metrics_families
    ///
    /// Only the requested columns are selected.
    ASTPtr buildSelectQueryFromMetricsOnly(const StorageID & metrics_table_id, const NameSet & requested_columns)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();

        if (requested_columns.contains(TimeSeriesColumnNames::MetricFamily))
        {
            auto metric_family = make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricFamilyName);
            metric_family->setAlias(TimeSeriesColumnNames::MetricFamily);
            select_list->children.push_back(std::move(metric_family));
        }
        if (requested_columns.contains(TimeSeriesColumnNames::Type))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Type));
        if (requested_columns.contains(TimeSeriesColumnNames::Unit))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Unit));
        if (requested_columns.contains(TimeSeriesColumnNames::Help))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Help));

        /// This branch is only taken when a metadata column is requested (see makeASTSelectFromTimeSeries).
        chassert(!select_list->children.empty());

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(makeMetricsTableElement(metrics_table_id));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return makeSelectWithUnionQuery(std::move(select_query));
    }

    /// Builds a query reading from multiple target tables. For example, when all the columns are requested:
    /// SELECT toString(ifNull(metric_name, '')) AS metric_name,
    ///        timeSeriesTagsToMap(tags, '__name__', metric_name) AS tags,
    ///        time_series,
    ///        metric_family_name AS metric_family,
    ///        type, unit, help
    /// FROM
    /// (
    ///     SELECT id, arrayZip(groupArray(timestamp), groupArray(value)) AS time_series
    ///     FROM <samples>
    ///     GROUP BY id
    /// ) AS __samples
    /// SEMI LEFT JOIN <tags> USING (id)
    /// FULL JOIN
    /// (
    ///     SELECT *, concat(metric_family_name, arrayJoin(timeSeriesMetricTypeToSuffixes(type))) AS __metric_family_with_suffix
    ///     FROM
    ///     (
    ///         SELECT *
    ///         FROM <metrics>
    ///         WHERE notEmpty(metric_family_name)
    ///         LIMIT 1 BY metric_family_name
    ///     ) AS __metrics_families
    /// ) AS __metric_families_with_all_suffixes
    /// ON toString(ifNull(metric_name, '')) = __metric_family_with_suffix
    ///
    /// The "samples" subquery, when read, anchors the query (the aggregated samples stream through the joins
    /// as the probe side); when the "samples" table is not read the query anchors on the "tags" table.
    /// The "tags" table is always read — it bridges "samples" (joined by id) and "metrics" (joined by matching
    /// metric_name against the expanded member names).
    ASTPtr buildSelectQueryFromMultipleTables(
        const StorageID & tags_table_id,
        const std::optional<StorageID> & samples_table_id,
        const std::optional<StorageID> & metrics_table_id,
        const NameSet & requested_columns,
        const NameSet & requested_tags,
        const std::unordered_map<String, String> & columns_by_tags,
        bool deduplicate_tags_by_id)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        select_query->setExpression(ASTSelectQuery::Expression::SELECT,
            makeJoinedSelectList(requested_columns, requested_tags, columns_by_tags));

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        if (samples_table_id)
        {
            /// Samples-anchored: samples are the (streamed) probe side, tags/metrics the smaller build sides.
            tables->children.push_back(makeSamplesTableElement(*samples_table_id));
            tables->children.push_back(makeTagsSemiJoinElement(tags_table_id));
        }
        else
        {
            tables->children.push_back(makeTagsTableElement(tags_table_id, deduplicate_tags_by_id));
        }
        if (metrics_table_id)
            tables->children.push_back(makeMetricsFullJoinElement(*metrics_table_id));
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return makeSelectWithUnionQuery(std::move(select_query));
    }
}


ASTPtr makeASTSelectFromTimeSeries(
    const StorageTimeSeries & storage,
    const NameSet & requested_columns,
    const SelectQueryInfo & query_info,
    const ContextPtr & context)
{
    bool need_samples = requested_columns.contains(TimeSeriesColumnNames::TimeSeries);

    bool need_tags = requested_columns.contains(TimeSeriesColumnNames::MetricName)
                  || requested_columns.contains(TimeSeriesColumnNames::Tags);

    bool need_metrics = requested_columns.contains(TimeSeriesColumnNames::MetricFamily)
                     || requested_columns.contains(TimeSeriesColumnNames::Type)
                     || requested_columns.contains(TimeSeriesColumnNames::Unit)
                     || requested_columns.contains(TimeSeriesColumnNames::Help);

    auto storage_settings = storage.getStorageSettings();

    /// With `FINAL` the read deduplicates unmerged "tags" parts (`LIMIT 1 BY id`), so a series is returned
    /// exactly once. Without it the read is cheaper, but a series inserted repeatedly may be returned once per
    /// unmerged part until a background merge collapses them.
    bool deduplicate_tags_by_id = query_info.isFinal();

    /// If we read both "samples" and "metrics" tables then we also need to read the "tags" table as a bridge between them.
    /// If we read neither "samples" nor "metrics" tables then we need to read the "tags" table even if it's not requested
    /// (so that `SELECT count() FROM time_series` returns the number of time series).
    if (need_samples == need_metrics)
        need_tags = true;

    /// Collect information about each target table we're going to read.
    std::optional<StorageID> samples_table_id;
    if (need_samples)
        samples_table_id = storage.getTargetTableID(ViewTarget::Samples, context);

    std::optional<StorageID> tags_table_id;

    /// `requested_tags` is non-empty only if the query and its filters request some specific tags
    /// (for example `SELECT tags['job'] FROM time_series`).
    NameSet requested_tags;

    /// Setting `tags_to_columns`.
    std::unordered_map<String, String> columns_by_tags;

    if (need_tags)
    {
        tags_table_id = storage.getTargetTableID(ViewTarget::Tags, context);
        requested_tags = findRequestedTags(storage, query_info, context);
        columns_by_tags = getColumnsByTags(*storage_settings);
    }

    std::optional<StorageID> metrics_table_id;
    if (need_metrics)
        metrics_table_id = storage.getTargetTableID(ViewTarget::Metrics, context);

    /// Single-table reads (no join).
    if (need_samples && !need_tags && !need_metrics)
        return buildSelectQueryFromSamplesOnly(*samples_table_id, requested_columns);

    if (need_tags && !need_samples && !need_metrics)
        return buildSelectQueryFromTagsOnly(*tags_table_id, requested_columns, requested_tags, columns_by_tags,
                                            deduplicate_tags_by_id);

    if (need_metrics && !need_tags && !need_samples)
        return buildSelectQueryFromMetricsOnly(*metrics_table_id, requested_columns);

    /// Multi-table reads: anchored on "samples" when it is read, otherwise on "tags".
    chassert(need_tags);
    return buildSelectQueryFromMultipleTables(*tags_table_id, samples_table_id, metrics_table_id, requested_columns,
                                           requested_tags, columns_by_tags, deduplicate_tags_by_id);
}

SettingsChanges getSettingsForSelectFromTimeSeries(bool final)
{
    SettingsChanges changes;

    /// If `aggregate_functions_null_for_empty` is 1 then the `time_series` column would become Nullable and
    /// could return NULL instead of an empty array (because that setting rewrites every aggregate,
    /// including the `groupArray`s in `arrayZip(groupArray(timestamp), groupArray(value))`,
    /// to its `...OrNull` variant).
    changes.emplace_back("aggregate_functions_null_for_empty", Field{false});

    /// If `join_use_nulls` is 1 then the generated query would return NULLs in the non-Nullable outer columns:
    /// in `metric_family`/`type`/`unit`/`help` for a series with no metadata row, and in `time_series` for a
    /// metric family with no series (because the unmatched side of a FULL JOIN then produces NULLs instead of
    /// the default values - an empty string / empty array - on which the reconstruction relies).
    changes.emplace_back("join_use_nulls", Field{false});

    /// If `join_algorithm` is `full_sorting_merge` or `partial_merge` then the generated query would throw
    /// NOT_IMPLEMENTED (because the merge-join algorithms do not implement the SEMI and FULL joins it uses).
    /// `hash` supports every join kind. Parallelism follows `parallel_hash_join_threshold`.
    changes.emplace_back("join_algorithm", Field{"hash"});

    /// If `optimize_aggregation_in_order` is 0 then the GROUP BY id over the "samples" table would build a hash
    /// table of all the series in memory (because only this setting lets the aggregation stream in sorting-key
    /// order, which is possible here: `id` is the first column of the default samples sorting key `(id, timestamp)`).
    changes.emplace_back("optimize_aggregation_in_order", Field{true});

    if (!final)
    {
        /// If `allow_aggregate_partitions_independently` is 0 then partitions of the "samples" table would never
        /// be aggregated in fully independent pipelines even when its partition key is a function of `id`, and
        /// if `force_aggregate_partitions_independently` is 0 then that optimization could still be skipped
        /// when the optimizer decides it would not help (e.g. too few partitions).
        /// Enabled only without FINAL: under FINAL the canonical merged execution is kept.
        ///
        /// TODO: Prefer a per-block no-merge aggregation mode once one exists (a proposed
        /// `group_by_each_block_no_merge` setting): per-block aggregation without merging is streaming,
        /// needs no precondition on the partition key, and its sliced output (several `time_series` rows
        /// per series) is a valid non-FINAL result.
        changes.emplace_back("allow_aggregate_partitions_independently", Field{true});
        changes.emplace_back("force_aggregate_partitions_independently", Field{true});
    }

    return changes;
}

}
