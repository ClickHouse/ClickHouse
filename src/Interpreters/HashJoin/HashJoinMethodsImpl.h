#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/HashJoin/JoinRightColumns.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>

#include <algorithm>
#include <span>
#include <type_traits>

namespace DB
{

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
JoinResultPtr HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinBlockImpl(
    const HashJoin & join, Block block, const Block & block_with_columns_to_add, bool is_join_get)
{
    ScatteredBlock scattered_block{std::move(block)};
    return joinBlockImpl(join, std::move(scattered_block), block_with_columns_to_add, is_join_get);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
JoinResultPtr HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinBlockImpl(
    const HashJoin & join,
    ScatteredBlock block,
    const Block & block_with_columns_to_add,
    bool is_join_get)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    std::vector<JoinOnKeyColumns> join_on_keys;
    const auto & onexprs = join.table_join->getClauses();
    for (size_t i = 0; i < onexprs.size(); ++i)
    {
        const auto & key_names = !is_join_get ? onexprs[i].key_names_left : onexprs[i].key_names_right;
        join_on_keys.emplace_back(
            block, key_names, onexprs[i].condColumnNames().first, join.data->tables[i]->keySizes(),
            HashJoin::isLowCardinalityType(join.keyType()));
    }

    /// Only MapsAll keeps every right row of a key, so only there do the recorded words resolve to
    /// exact rows. The residual path is excluded: its words count output rows rather than left rows,
    /// and both metrics come from elsewhere there
    constexpr bool refs_can_carry_stats = join_features.is_maps_all
        && (join_features.inner || join_features.left || join_features.full);
    const bool record_refs_for_stats = refs_can_carry_stats && join.recordsRowRefsForStats();

    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `NotJoinedHash`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    AddedColumns added_columns(
        block,
        block_with_columns_to_add,
        join.savedBlockSample(),
        join,
        std::move(join_on_keys),
        join.table_join->getMixedJoinExpression(),
        join.additional_filter_required_rhs_pos,
        join_features.is_asof_join,
        is_join_get,
        record_refs_for_stats);

    if (join.matched_rows_stats && join.matched_rows_stats->hasRightFlags())
        added_columns.match_stats = join.matched_rows_stats.get();

    bool has_required_right_keys = (join.required_right_keys.columns() != 0);
    added_columns.need_filter = join_features.need_filter || has_required_right_keys;
    added_columns.max_joined_block_rows = join.max_joined_block_rows;

    if (!added_columns.max_joined_block_rows)
        added_columns.max_joined_block_rows = std::numeric_limits<size_t>::max();

    const auto functions = JoinProbe::selectFunctions<KIND, STRICTNESS, MapsTemplate>(
        join.keyType(), join.data->tables.size(), added_columns.additional_filter_expression != nullptr, added_columns.record_row_refs);
    size_t processed_rows
        = JoinProbe::JoinRightColumns(functions, join.data->tables, added_columns, *join.used_flags, block.getSelector()).run();
    /// Do not hold memory for join_on_keys anymore
    added_columns.join_on_keys.clear();

    if (auto * stats = join.matched_rows_stats.get())
    {
        stats->collectProbeBlock(processed_rows, countMatchedLeftRows<KIND, STRICTNESS>(added_columns, processed_rows));

        const bool right_matches_marked_inline = added_columns.additional_filter_expression != nullptr;
        if (stats->hasRightFlags() && !right_matches_marked_inline)
            markRightMatchedFromRowRefs(*stats, added_columns);
    }

    std::optional<ScatteredBlock> next_scattered_block;
    if (processed_rows < block.rows())
    {
        auto [raw_block, raw_selector] = std::move(block).detachData();
        auto split_selector = raw_selector.split(processed_rows);
        block = ScatteredBlock(raw_block, std::move(split_selector.first));
        next_scattered_block = ScatteredBlock(std::move(raw_block), std::move(split_selector.second));
    }

    /// The counter only advances where the probe records row refs, so without them its zero counts
    /// nothing and must stay distinguishable from a measured zero.
    const std::optional<size_t> matched_right_rows
        = added_columns.record_row_refs ? std::optional<size_t>(added_columns.lazy_output.hash_table_matches) : std::nullopt;

    auto join_result = std::make_unique<HashJoinResult>(
        std::move(added_columns.lazy_output),
        std::move(added_columns.columns),
        std::move(added_columns.offsets_to_replicate),
        std::move(added_columns.filter),
        std::move(added_columns.matched_rows),
        matched_right_rows,
        std::move(block),
        HashJoinResult::Properties{
            *join.table_join,
            join.required_right_keys,
            join.required_right_keys_sources,
            join.max_joined_block_rows,
            join.max_joined_block_bytes,
            join.data->allocated_size / std::max<size_t>(1, join.data->rows_to_join),
            join_features.need_filter,
            is_join_get,
            join.joined_block_split_single_row,
            join.enable_lazy_columns_replication,
            join.enable_lazy_columns_indexing
        });

    if (next_scattered_block)
        join_result->setNextBlock(std::move(next_scattered_block.value()));
    return join_result;
}

}
