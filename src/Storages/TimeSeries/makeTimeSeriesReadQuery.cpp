#include <Storages/TimeSeries/makeTimeSeriesReadQuery.h>

#include <Core/Joins.h>
#include <Core/Names.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/ASTTablesInSelectQuery.h>
#include <Storages/StorageTimeSeries.h>
#include <Storages/TimeSeries/TimeSeriesColumnNames.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
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

    /// Builds `SELECT <requested columns> FROM <tags_table_id>` — only the columns the caller
    /// asked for via `column_names` are returned.
    ASTPtr makeTagsOnlySelect(const StorageID & tags_table_id, const NameSet & column_names)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();
        auto select_list = make_intrusive<ASTExpressionList>();

        if (column_names.contains(TimeSeriesColumnNames::MetricName))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
        if (column_names.contains(TimeSeriesColumnNames::Tags))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));

        /// If none of the "tags" table's columns are requested, we fall back to `1`.
        /// So for example `SELECT count() FROM time_series` is evaluated as
        /// `SELECT count() FROM (SELECT 1 FROM tags)`.
        if (select_list->children.empty())
            select_list->children.push_back(make_intrusive<ASTLiteral>(static_cast<UInt8>(1)));

        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        auto table_exp = make_intrusive<ASTTableExpression>();
        table_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(tags_table_id);
        table_exp->children.push_back(table_exp->database_and_table_name);

        auto table_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        table_elem->table_expression = table_exp;
        table_elem->children.push_back(table_exp);

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(table_elem);
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
    /// — the bare-samples branch (no Tags involvement).
    ASTPtr makeBareSamplesSelect(const StorageID & samples_table_id)
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

    /// Builds:
    ///   SELECT <requested cols>, time_series
    ///   FROM <tags_table_id>
    ///   SEMI LEFT JOIN (samples_grouped_subquery) USING id
    ASTPtr makeTagsAndSamplesSelect(
        const StorageID & tags_table_id,
        const StorageID & samples_table_id,
        const NameSet & column_names)
    {
        auto select_query = make_intrusive<ASTSelectQuery>();

        auto select_list = make_intrusive<ASTExpressionList>();
        if (column_names.contains(TimeSeriesColumnNames::MetricName))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::MetricName));
        if (column_names.contains(TimeSeriesColumnNames::Tags))
            select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::Tags));
        /// This branch is only taken when `time_series` is requested.
        select_list->children.push_back(make_intrusive<ASTIdentifier>(TimeSeriesColumnNames::TimeSeries));
        select_query->setExpression(ASTSelectQuery::Expression::SELECT, select_list);

        /// FROM tags
        auto tags_exp = make_intrusive<ASTTableExpression>();
        tags_exp->database_and_table_name = make_intrusive<ASTTableIdentifier>(tags_table_id);
        tags_exp->children.push_back(tags_exp->database_and_table_name);

        auto tags_elem = make_intrusive<ASTTablesInSelectQueryElement>();
        tags_elem->table_expression = tags_exp;
        tags_elem->children.push_back(tags_exp);

        /// SEMI LEFT JOIN (samples_grouped_subquery) AS S USING id
        /// The alias is required by `joined_subquery_requires_alias` (on by default).
        auto samples_exp = make_intrusive<ASTTableExpression>();
        samples_exp->subquery = make_intrusive<ASTSubquery>(makeSamplesGroupedSubquery(samples_table_id));
        samples_exp->subquery->setAlias("__samples_grouped");
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

        auto tables = make_intrusive<ASTTablesInSelectQuery>();
        tables->children.push_back(tags_elem);
        tables->children.push_back(samples_elem);
        select_query->setExpression(ASTSelectQuery::Expression::TABLES, tables);

        return wrapInUnionQuery(std::move(select_query));
    }
}


ASTPtr makeTimeSeriesReadQuery(
    const StorageTimeSeries & storage,
    const Names & column_names,
    const ContextPtr & context)
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

    /// The metrics branches will be added in subsequent commits.
    if (need_metrics)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "SELECT from a TimeSeries table does not support columns `metric_family`, `type`, `unit`, "
            "or `help` yet.");
    }

    auto tags_table_id = storage.getTargetTableID(ViewTarget::Tags, context);
    if (!need_samples)
        return makeTagsOnlySelect(tags_table_id, requested);

    auto samples_table_id = storage.getTargetTableID(ViewTarget::Samples, context);
    if (!need_tags)
        return makeBareSamplesSelect(samples_table_id);

    return makeTagsAndSamplesSelect(tags_table_id, samples_table_id, requested);
}

}
