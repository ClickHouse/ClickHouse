#pragma once

#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/HashJoin/JoinRightColumnsImpl.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/JoinUtils.h>

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
    for (const auto & onexpr : join.table_join->getClauses())
    {
        const auto & key_names = !is_join_get ? onexpr.key_names_left : onexpr.key_names_right;
        join_on_keys.emplace_back(block, key_names, onexpr.condColumnNames().first, HashJoin::isLowCardinalityType(join.keyType()));
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

    size_t processed_rows = 0;
    if (join.data->tables.size() > 1 || added_columns.additional_filter_expression)
    {
        const auto functions = JoinProbe::selectFunctions<KIND, STRICTNESS, MapsTemplate>(
            join.keyType(), join.data->tables.size(), added_columns.additional_filter_expression != nullptr);
        processed_rows
            = JoinProbe::JoinRightColumns(functions, join.data->tables, added_columns, *join.used_flags, block.getSelector()).run();
    }
    else
        processed_rows = switchJoinRightColumns(join.data->tables, added_columns, block.getSelector(), *join.used_flags);
    /// Do not hold memory for join_on_keys anymore
    added_columns.join_on_keys.clear();

    if (auto * stats = join.matched_rows_stats.get())
    {
        const size_t probed_rows = processed_rows ? processed_rows : block.rows();
        stats->collectProbeBlock(probed_rows, countMatchedLeftRows<KIND, STRICTNESS>(added_columns, probed_rows));

        const bool right_matches_marked_inline = added_columns.additional_filter_expression != nullptr;
        if (stats->hasRightFlags() && !right_matches_marked_inline)
            markRightMatchedFromRowRefs(*stats, added_columns);
    }

    std::optional<ScatteredBlock> next_scattered_block;
    if (0 < processed_rows && processed_rows < block.rows())
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

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::switchJoinRightColumns(
    Tables tables,
    AddedColumns & added_columns,
    const ScatteredBlock::Selector & selector,
    JoinStuff::JoinUsedFlags & used_flags)
{
    switch (tables.front()->type())
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: return joinRightColumnsSwitchNullability<HashJoin::Type::TYPE>(tables, added_columns, selector, used_flags);
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M

    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <HashJoin::Type TYPE, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchNullability(
    Tables tables,
    AddedColumns & added_columns,
    const ScatteredBlock::Selector & selector,
    JoinStuff::JoinUsedFlags & used_flags)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    /// The join kinds that filter always set `added_columns.need_filter`.
    if constexpr (!join_features.need_filter)
    {
        if (!added_columns.need_filter)
            return joinRightColumnsSwitchFastPath<TYPE, false>(tables, added_columns, selector, used_flags);
    }
    return joinRightColumnsSwitchFastPath<TYPE, true>(tables, added_columns, selector, used_flags);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <HashJoin::Type TYPE, bool need_filter, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchFastPath(
    Tables tables,
    AddedColumns & added_columns,
    const ScatteredBlock::Selector & selector,
    JoinStuff::JoinUsedFlags & used_flags)
{
    chassert(tables.size() == 1 && !added_columns.additional_filter_expression);

    /// The overwhelmingly common case - non-nullable keys and no condition in the ON section -
    /// is compiled without the per-row null-map and join-mask checks; everything else takes the
    /// generic variant that does both checks at runtime.
    const bool fast_path = std::ranges::all_of(
        added_columns.join_on_keys,
        [](const auto & elem) { return !elem.null_map && elem.join_mask_column.getKind() == JoinCommon::JoinMask::Kind::AllTrue; });

    if (selector.isContinuousRange())
    {
        if (fast_path)
            return joinRightColumns<TYPE, need_filter, /*fast_path=*/true>(tables, added_columns, used_flags, selector.getRange());
        else
            return joinRightColumns<TYPE, need_filter, /*fast_path=*/false>(tables, added_columns, used_flags, selector.getRange());
    }
    /// The probe always runs on a whole block as one continuous range; only the build scatters rows by index.
    UNREACHABLE();
}

/// A set map records that a key is present and nothing else: LEFT ANTI emits a row only when the key
/// is missing, and LEFT SEMI has no right-side value to emit, so a match only marks the left row.
template <
    JoinKind KIND,
    JoinStrictness STRICTNESS,
    bool need_filter,
    typename MapsTemplate,
    typename Map,
    typename KeyGetter,
    typename AddedColumns>
requires SetJoinMaps<MapsTemplate>
void processMatch(
    const typename KeyGetter::FindResult &,
    [[maybe_unused]] AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags &,
    [[maybe_unused]] size_t i,
    size_t,
    IColumn::Offset &)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    static_assert(
        join_features.left && (join_features.is_semi_join || join_features.is_anti_join),
        "A set map is only chosen for LEFT SEMI and LEFT ANTI");

    if constexpr (join_features.is_semi_join)
        setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
}

template <
    JoinKind KIND,
    JoinStrictness STRICTNESS,
    bool need_filter,
    typename MapsTemplate,
    typename Map,
    typename KeyGetter,
    typename AddedColumns>
requires MappedJoinMaps<MapsTemplate>
void processMatch(
    const typename KeyGetter::FindResult & find_result,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    size_t i,
    size_t ind,
    IColumn::Offset & current_offset)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    auto & mapped = find_result.getMapped();
    if constexpr (join_features.is_asof_join)
    {
        const IColumn & left_asof_key = added_columns.leftAsofKey();

        const auto * row_ref = mapped->findAsof(left_asof_key, ind);
        if (row_ref)
        {
            setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
            added_columns.appendFromBlock(row_ref->encode());
        }
        else
            addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
    }
    else if constexpr (join_features.is_all_join)
    {
        /// This branch records a key's whole cell word. The emit reads such a word correctly only in
        /// the `Lists` shape, which `emits_whole_key_per_word` selects, so it has to cover this branch.
        static_assert(join_features.emits_whole_key_per_word);
        setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
        used_flags.template setUsed<join_features.need_flags, false>(find_result);
        /// An ALL join emits a right row for each matching left row, so nothing is claimed here.
        KnownRowsHolder<false> known_rows;
        addFoundRowAll(mapped, added_columns, current_offset, known_rows, nullptr, /*is_last_disjunct=*/true);
    }
    else if constexpr ((join_features.is_any_join || join_features.is_semi_join) && join_features.right)
    {
        static_assert(join_features.emits_whole_key_per_word);
        /// A single disjunct reaches a right row through one key only, so claiming the key claims all of its rows.
        if (used_flags.template setUsedOnce<join_features.need_flags, false>(find_result))
        {
            setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
            KnownRowsHolder<false> known_rows;
            addFoundRowAll(mapped, added_columns, current_offset, known_rows, nullptr, /*is_last_disjunct=*/true);
        }
    }
    else if constexpr (join_features.is_any_join && join_features.inner)
    {
        bool used_once = used_flags.template setUsedOnce<join_features.need_flags, false>(find_result);

        /// Use first appeared left key only
        if (used_once)
        {
            setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
            added_columns.appendFromBlock(firstRefWord(mapped));
        }
    }
    else if constexpr (join_features.is_any_join && join_features.full)
    {
        /// TODO
    }
    else if constexpr (join_features.is_anti_join)
    {
        if constexpr (join_features.right && join_features.need_flags)
            used_flags.template setUsed<join_features.need_flags, false>(find_result);
    }
    else /// ANY LEFT, SEMI LEFT, old ANY (RightAny)
    {
        setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
        used_flags.template setUsed<join_features.need_flags, false>(find_result);
        added_columns.appendFromBlock(firstRefWord(mapped));
    }
}

/// Joins right table columns which indexes are present in right_indexes using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <HashJoin::Type TYPE, bool need_filter, bool fast_path, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumns(
    Tables tables,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    const ScatteredBlock::Selector::Range & selector)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    using Map = const JoinMapOf<TYPE, MapsTemplate>;
    using KeyGetter = typename KeyGetterForType<TYPE, Map>::Type;

    size_t rows = ScatteredBlock::Selector::size(selector);

    if constexpr (need_filter)
    {
        added_columns.filter = IColumn::Filter(rows, 0);
        added_columns.matched_rows.reserve(rows);
    }
    /// Deliberately the same condition as the `addFoundRowAll` branches of `processMatch` below.
    if constexpr (join_features.emits_whole_key_per_word)
        added_columns.lazy_output.output_by_row_list = true;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = IColumn::Offsets(rows);

    IColumn::Offset current_offset = 0;
    tables[0]->probe<TYPE, MapsTemplate, fast_path>(
        added_columns.join_on_keys.at(0),
        selector,
        added_columns.enable_prefetch,
        current_offset,
        [&](size_t i, size_t ind, const auto & find_result, IColumn::Offset & offset) __attribute__((always_inline))
        {
            if (find_result.isFound())
            {
                processMatch<KIND, STRICTNESS, need_filter, MapsTemplate, Map, KeyGetter>(
                    find_result, added_columns, used_flags, i, ind, offset);
            }
            else
            {
                if constexpr (join_features.is_anti_join && join_features.left)
                    setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
                addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, offset);
            }

            if constexpr (join_features.need_replication)
            {
                added_columns.offsets_to_replicate[i] = offset;
            }
        });

    return 0;
}

}
