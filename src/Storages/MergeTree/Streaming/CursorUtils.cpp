#include <Common/escapeString.h>

#include <Parsers/ExpressionListParsers.h>
#include <Parsers/parseQuery.h>

#include <Planner/Utils.h>

#include <Storages/MergeTree/Streaming/CursorUtils.h>

namespace DB
{

MergeTreeCursor buildMergeTreeCursor(const CursorTreeNodePtr & cursor_tree)
{
    MergeTreeCursor cursor;

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

std::shared_ptr<CursorInfo> buildMergeTreeCursorInfo(
    const String & storage_full_name,
    const String & partition_id,
    const std::optional<String> & keeper_key,
    Int64 block_number,
    Int64 block_offset)
{
    CursorTreeNodePtr tree = std::make_shared<CursorTreeNode>();
    auto & partition_subtree = tree->next(partition_id);
    partition_subtree->setValue("block_number", block_number);
    partition_subtree->setValue("block_offset", block_offset);

    CursorDataMap data = {{storage_full_name, CursorData{.tree = std::move(tree), .keeper_key = keeper_key}}};
    return std::make_shared<CursorInfo>(std::move(data));
}

std::map<String, Int64> buildInitialBlockNumberOffsets(
    const MergeTreeCursor & cursor,
    const MergeTreeData::DataPartsVector & snapshot_data_parts,
    const RangesInDataParts & analyzed_data_parts)
{
    std::map<String, Int64> block_number_offsets;

    auto is_covered = [](const String & partition_id, Int64 block_number, const MergeTreeData::DataPartPtr & part)
    {
        if (part->info.partition_id != partition_id)
            return false;

        if (part->info.min_block <= block_number && block_number <= part->info.max_block)
            return true;

        return false;
    };

    for (const auto & [partition_id, data] : cursor)
    {
        bool covered_by_snapshot = false;
        bool covered_by_analysis = false;

        for (const auto & part : snapshot_data_parts)
        {
            if (is_covered(partition_id, data.block_number, part))
            {
                covered_by_snapshot = true;
                break;
            }
        }

        for (const auto & part : analyzed_data_parts)
        {
            if (is_covered(partition_id, data.block_number, part.data_part))
            {
                covered_by_analysis = true;
                break;
            }
        }

        if (covered_by_snapshot)
        {
            if (covered_by_analysis)
                block_number_offsets[partition_id] = data.block_number - 1;
            else
                block_number_offsets[partition_id] = data.block_number;
        }
        else
        {
            chassert(!covered_by_analysis);
            block_number_offsets[partition_id] = data.block_number;
        }
    }

    return block_number_offsets;
}

std::optional<FilterDAGInfo> convertCursorToFilter(const MergeTreeCursor & cursor, SelectQueryInfo & info)
{
    auto & planner_context = info.planner_context;
    const auto & query_context = planner_context->getQueryContext();
    const auto & settings = query_context->getSettingsRef();

    std::vector<String> partition_filters;
    constexpr static auto kPartitionFilterPattern = FMT_STRING(
        "(_queue_partition_id = '{partition_id}' AND "
        "(_queue_block_number > {block_number} OR (_queue_block_number = {block_number} AND _queue_block_offset > {block_offset})))");

    for (const auto & [partition_id, data] : cursor)
    {
        auto partition_filter = fmt::format(
            kPartitionFilterPattern,
            fmt::arg("partition_id", escapeString(partition_id)),
            fmt::arg("block_number", data.block_number),
            fmt::arg("block_offset", data.block_offset));

        partition_filters.push_back(std::move(partition_filter));
    }

    if (partition_filters.empty())
        return std::nullopt;

    String filter = boost::algorithm::join(partition_filters, " OR ");

    ParserExpression parser;
    auto cursor_filter_ast = parseQuery(
        parser,
        filter.data(),
        filter.data() + filter.size(),
        "cursor filter",
        settings.max_query_size,
        settings.max_parser_depth,
        settings.max_parser_backtracks);

    chassert(cursor_filter_ast);
    return buildFilterInfo(cursor_filter_ast, info.table_expression, planner_context);
}

}
