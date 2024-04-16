#include <algorithm>
#include <iterator>

#include <Common/escapeString.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Planner/Utils.h>

#include <Processors/QueryPlan/FilterStep.h>
#include <Processors/QueryPlan/StreamingAdapterStep.h>
#include <Processors/QueryPlan/ExpressionStep.h>

#include <Storages/MergeTree/StreamingUtils.h>
#include <Poco/Logger.h>

namespace DB
{

Names extendColumnsWithStreamingAux(const Names & columns_to_read)
{
    Names ext = columns_to_read;
    // TODO: change to constants
    ext.push_back("_queue_partition_id");
    ext.push_back("_queue_block_number");
    ext.push_back("_queue_block_offset");

    std::sort(ext.begin(), ext.end());
    ext.erase(std::unique(ext.begin(), ext.end()), ext.end());

    return ext;
}

void addCursorFilterStep(QueryPlan & query_plan, SelectQueryInfo & info)
{
    auto & planner_context = info.planner_context;
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();
    const auto & cursor_tree = info.table_expression_modifiers->getStreamSettings()->tree;

    std::vector<String> partition_filters;
    constexpr static auto kPartitionFilterPattern = FMT_STRING(
        "(_queue_partition_id = '{partition_id}' AND "
        "(_queue_block_number > {block_number} OR (_queue_block_number = {block_number} AND _queue_block_offset > {block_offset})))");

    for (const auto & [partition_id, value] : *cursor_tree)
    {
        const auto & partition_cursor = std::get<CursorTreeNodePtr>(value);
        const auto block_number = partition_cursor->getValue("block_number");
        const auto block_offset = partition_cursor->getValue("block_offset");

        auto partition_filter = fmt::format(
            kPartitionFilterPattern,
            fmt::arg("partition_id", escapeString(partition_id)),
            fmt::arg("block_number", block_number),
            fmt::arg("block_offset", block_offset));

        partition_filters.push_back(std::move(partition_filter));
    }

    if (partition_filters.empty())
      return;

    String filter = boost::algorithm::join(partition_filters, " OR ");

    ParserExpression parser;
    auto cursor_filter_ast = parseQuery(
        parser, filter.data(), filter.data() + filter.size(), "cursor filter",
        settings.max_query_size, settings.max_parser_depth, settings.max_parser_backtracks);

    chassert(cursor_filter_ast);
    Names current_names = query_plan.getCurrentDataStream().header.getNames();
    NameSet current_name_set = {std::make_move_iterator(current_names.begin()), std::make_move_iterator(current_names.end())};
    auto filter_info = buildFilterInfo(cursor_filter_ast, info.table_expression, planner_context, current_name_set);

    if (!filter_info.actions)
        return;

    auto filter_step = std::make_unique<FilterStep>(
        query_plan.getCurrentDataStream(), filter_info.actions, filter_info.column_name, filter_info.do_remove_column);
    filter_step->setStepDescription("Cursor filter");
    query_plan.addStep(std::move(filter_step));
}

void addDropAuxColumnsStep(QueryPlan & query_plan, const Block & desired_header)
{
    if (blocksHaveEqualStructure(query_plan.getCurrentDataStream().header, desired_header))
      return;

    auto convert_actions_dag = ActionsDAG::makeConvertingActions(
        query_plan.getCurrentDataStream().header.getColumnsWithTypeAndName(),
        desired_header.getColumnsWithTypeAndName(),
        ActionsDAG::MatchColumnsMode::Name);

    auto expression_step = std::make_unique<ExpressionStep>(query_plan.getCurrentDataStream(), convert_actions_dag);
    expression_step->setStepDescription("Drop auxiliary columns");
    query_plan.addStep(std::move(expression_step));
}

}
