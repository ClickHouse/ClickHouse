#pragma once

#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/HashJoin/JoinRightColumnsImpl.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>

#include <algorithm>
#include <span>
#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
BuildResult HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::insertFromBlockImpl(
    HashJoin & join, HashJoinTable & table, const HashJoinTable::InsertRows & rows, size_t stream)
{
    static_assert((STRICTNESS == JoinStrictness::Asof) == std::is_same_v<MapsTemplate, HashJoin::MapsAsof>);
    const HashJoinTable::InsertSettings settings{
        .any_take_last_row = join.anyTakeLastRow(),
        .enable_software_prefetch = join.enableSoftwarePrefetch(),
        .must_keep_right_blocks = join.mustKeepRightBlocks(),
        .asof_type = join.getAsofType(),
        .asof_inequality = join.getAsofInequality()};
    const auto add_bytes = [&](size_t bytes) { join.data->addBytes(join.data->bucket_bytes, bytes); };
    return table.insert<MapsTemplate>(settings, rows, stream, add_bytes);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
JoinResultPtr HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinBlockImpl(
    const HashJoin & join, Block block, const Block & block_with_columns_to_add, bool is_join_get)
{
    ScatteredBlock scattered_block{std::move(block)};
    /// Only `joinGet` calls this. Its join has one disjunct and no additional filter, so one call probes the whole block.
    JoinProbe::LeftBlockState left_block_state;
    return joinBlockImpl(join, std::move(scattered_block), block_with_columns_to_add, left_block_state, is_join_get);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
JoinResultPtr HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinBlockImpl(
    const HashJoin & join,
    ScatteredBlock block,
    const Block & block_with_columns_to_add,
    JoinProbe::LeftBlockState & left_block_state,
    bool is_join_get)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    auto & disjuncts = left_block_state.disjuncts;
    if (disjuncts.empty())
    {
        disjuncts.reserve(join.table_join->getClauses().size());
        for (const auto & onexpr : join.table_join->getClauses())
        {
            const auto & key_names = !is_join_get ? onexpr.key_names_left : onexpr.key_names_right;
            disjuncts.emplace_back(
                JoinOnKeyColumns(block, key_names, onexpr.condColumnNames().first, HashJoin::isLowCardinalityType(join.keyType())));
        }
        left_block_state.key_type = join.keyType();
    }
    else if (left_block_state.key_type != join.keyType())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "The key type of a hash join changed between the calls of a block");

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
        join_features.is_asof_join ? disjuncts[0].columns.key_columns.back() : nullptr,
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
        join.keyType(), join.data->tables.size(), added_columns.additional_filter_expression != nullptr);
    size_t processed_rows
        = JoinProbe::JoinRightColumns(functions, join.data->tables, added_columns, left_block_state, *join.used_flags, block.getSelector())
              .run();
    if (processed_rows == block.rows())
        left_block_state = {};

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
