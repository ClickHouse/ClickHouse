#include <Storages/TimeSeries/makeTimeSeriesReadQuery.h>

#include <Core/Names.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
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

    /// Currently only need_tags can be not false.
    /// Other cases are not supported yet.
    if (need_samples || need_metrics)
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED,
            "SELECT from a TimeSeries table currently supports only `metric_name` and `tags` columns. "
            "Reading `time_series`, `metric_family`, `type`, `unit`, or `help` is not implemented yet.");
    }

    auto tags_table_id = storage.getTargetTableID(ViewTarget::Tags, context);
    return makeTagsOnlySelect(tags_table_id, requested);
}

}
