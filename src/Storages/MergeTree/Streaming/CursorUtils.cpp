#include <Storages/MergeTree/Streaming/CursorUtils.h>

#include <Common/escapeString.h>
#include <Core/Settings.h>
#include <Core/Streaming/CursorTree.h>
#include <Interpreters/Context.h>
#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>
#include <Planner/PlannerContext.h>
#include <Planner/Utils.h>

#include <boost/algorithm/string/join.hpp>

namespace DB
{

namespace Setting
{
    extern const SettingsUInt64 max_query_size;
    extern const SettingsUInt64 max_parser_depth;
    extern const SettingsUInt64 max_parser_backtracks;
}

MergeTreeCursor buildMergeTreeCursor(const CursorTreeNodePtr & cursor_tree)
{
    MergeTreeCursor cursor;

    if (!cursor_tree)
        return cursor;

    for (const auto & [partition_id, node] : *cursor_tree)
    {
        const auto & partition_node = std::get<CursorTreeNodePtr>(node);
        cursor[partition_id] = PartitionCursor{
            .block_number = partition_node->getValue("block_number"),
            .block_offset = partition_node->getValue("block_offset"),
        };
    }

    return cursor;
}

std::optional<FilterDAGInfo> convertCursorToFilter(const MergeTreeCursor & cursor, SelectQueryInfo & query_info)
{
    if (cursor.empty())
        return std::nullopt;

    auto & planner_context = query_info.planner_context;
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    std::vector<String> partition_filters;
    constexpr static auto kPartitionFilterPattern = FMT_STRING(
        "(_partition_id = '{partition_id}' AND "
        "(_block_number > {block_number} OR (_block_number = {block_number} AND _block_offset > {block_offset})))");

    for (const auto & [partition_id, data] : cursor)
    {
        auto partition_filter = fmt::format(
            kPartitionFilterPattern,
            fmt::arg("partition_id", escapeString(partition_id)),
            fmt::arg("block_number", data.block_number),
            fmt::arg("block_offset", data.block_offset));

        partition_filters.push_back(std::move(partition_filter));
    }

    const String filter = boost::algorithm::join(partition_filters, " OR ");

    ParserExpression parser;
    auto cursor_filter_ast = parseQuery(
        parser,
        filter.data(),
        filter.data() + filter.size(),
        "cursor filter",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    chassert(cursor_filter_ast);
    return buildFilterInfo(cursor_filter_ast, query_info.table_expression, planner_context);
}

}
