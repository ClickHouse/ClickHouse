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

FilterDAGInfo buildPartitionFilter(
    const String & partition_id,
    const PartitionCursor & last_emitted_position,
    Int64 safe_block_number,
    SelectQueryInfo & query_info)
{
    /// TODO(michicosun): what to do if query was stopped in the middle of the block.
    chassert(safe_block_number > last_emitted_position.block_number);

    auto & planner_context = query_info.planner_context;
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    static constexpr auto kPattern = FMT_STRING(
        "(_partition_id = '{partition_id}' AND _block_number <= {safe_block_number} AND "
        "(_block_number > {last_bn} OR (_block_number = {last_bn} AND _block_offset > {last_bo})))");

    const String filter = fmt::format(
        kPattern,
        fmt::arg("partition_id", escapeString(partition_id)),
        fmt::arg("safe_block_number", safe_block_number),
        fmt::arg("last_bn", last_emitted_position.block_number),
        fmt::arg("last_bo", last_emitted_position.block_offset));

    ParserExpression parser;
    auto filter_ast = parseQuery(
        parser,
        filter.data(),
        filter.data() + filter.size(),
        "snapshot partition filter",
        settings[Setting::max_query_size],
        settings[Setting::max_parser_depth],
        settings[Setting::max_parser_backtracks]);

    chassert(filter_ast);
    return buildFilterInfo(filter_ast, query_info.table_expression, planner_context);
}

}
