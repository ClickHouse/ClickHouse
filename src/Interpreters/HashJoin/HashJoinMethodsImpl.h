#pragma once

#include <Columns/ColumnsCommon.h>
#include <Columns/IColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/HashJoin/MatchedRowsStats.h>
#include <Interpreters/JoinUtils.h>

#include <algorithm>
#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
BuildResult HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::insertFromBlockImpl(
    HashJoin & join, HashJoinTable & table, const HashJoinTable::InsertRows & rows, Arena & arena)
{
    static_assert((STRICTNESS == JoinStrictness::Asof) == std::is_same_v<MapsTemplate, HashJoin::MapsAsof>);
    switch (join.data->type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        return table.insert<HashJoin::Type::TYPE, MapsTemplate>(join, rows, arena, *join.data);

            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
    UNREACHABLE();
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
    const auto & onexprs = join.table_join->getClauses();
    for (size_t i = 0; i < onexprs.size(); ++i)
    {
        const auto & key_names = !is_join_get ? onexprs[i].key_names_left : onexprs[i].key_names_right;
        join_on_keys.emplace_back(
            block, key_names, onexprs[i].condColumnNames().first, join.data->tables[i]->keySizes(),
            HashJoin::isLowCardinalityType(join.data->type));
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

    size_t processed_rows = switchJoinRightColumns(
        join.data->tables, added_columns, block.getSelector(), join.data->type, *join.used_flags, join.data->key_range);
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
    HashJoin::Type type,
    JoinStuff::JoinUsedFlags & used_flags,
    KeyRange key_range)
{
    switch (type)
    {
#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        return joinRightColumnsSwitchNullability<HashJoin::Type::TYPE>(tables, added_columns, selector, used_flags, key_range);
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
    JoinStuff::JoinUsedFlags & used_flags,
    KeyRange key_range)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    /// The join kinds that filter always set `added_columns.need_filter`.
    if constexpr (!join_features.need_filter)
    {
        if (!added_columns.need_filter)
            return joinRightColumnsSwitchMultipleDisjuncts<TYPE, false>(tables, added_columns, selector, used_flags, key_range);
    }
    return joinRightColumnsSwitchMultipleDisjuncts<TYPE, true>(tables, added_columns, selector, used_flags, key_range);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <HashJoin::Type TYPE, bool need_filter, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchMultipleDisjuncts(
    Tables tables,
    AddedColumns & added_columns,
    const ScatteredBlock::Selector & selector,
    JoinStuff::JoinUsedFlags & used_flags,
    KeyRange key_range)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    if constexpr (join_features.is_maps_all)
    {
        if (added_columns.additional_filter_expression)
        {
            const bool mark_per_row_used = join_features.right || join_features.full || tables.size() > 1;
            return joinRightColumnsWithAdditionalFilter<TYPE>(
                tables, added_columns, used_flags, selector, need_filter, mark_per_row_used, key_range);
        }
    }

    if (added_columns.additional_filter_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Additional filter expression is not supported for this JOIN");

    /// The overwhelmingly common case - non-nullable keys and no condition in the ON section -
    /// is compiled without the per-row null-map and join-mask checks; everything else takes the
    /// generic variant that does both checks at runtime.
    const bool fast_path = std::ranges::all_of(
        added_columns.join_on_keys,
        [](const auto & elem) { return !elem.null_map && elem.join_mask_column.getKind() == JoinCommon::JoinMask::Kind::AllTrue; });

    if (tables.size() > 1 || added_columns.join_on_keys.empty())
    {
        /// Several clauses have no required right keys (see the `HashJoin` constructor), and the probe never
        /// scatters a block, so only these copies can run.
        if constexpr (need_filter == join_features.need_filter)
        {
            if (selector.isContinuousRange())
            {
                if (fast_path)
                    return joinRightColumns<TYPE, need_filter, /*fast_path=*/true>(
                        tables, added_columns, used_flags, selector.getRange(), key_range);
                else
                    return joinRightColumns<TYPE, need_filter, /*fast_path=*/false>(
                        tables, added_columns, used_flags, selector.getRange(), key_range);
            }
        }
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Unexpected hash join of {} clauses with need_filter = {} on a {} block",
            tables.size(), need_filter, selector.isContinuousRange() ? "continuous" : "scattered");
    }

    chassert(tables.size() == 1);
    if (selector.isContinuousRange())
    {
        if (fast_path)
            return joinRightColumns<TYPE, need_filter, /*fast_path=*/true>(
                *tables[0], added_columns, used_flags, selector.getRange(), key_range);
        else
            return joinRightColumns<TYPE, need_filter, /*fast_path=*/false>(
                *tables[0], added_columns, used_flags, selector.getRange(), key_range);
    }
    /// The probe always runs on a whole block as one continuous range; only the build scatters rows by index.
    UNREACHABLE();
}

template <bool need_filter>
void setUsed(IColumn::Filter & filter [[maybe_unused]], size_t pos [[maybe_unused]], IColumn::Offsets & matched_rows [[maybe_unused]])
{
    if constexpr (need_filter)
    {
        filter[pos] = 1;
        matched_rows.push_back(pos);
    }
}

/// A set map records that a key is present and nothing else: LEFT ANTI emits a row only when the key
/// is missing, and LEFT SEMI has no right-side value to emit, so a match only marks the left row.
template <
    JoinKind KIND,
    JoinStrictness STRICTNESS,
    bool need_filter,
    bool flag_per_row,
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
    IColumn::Offset &,
    KnownRowsHolder<flag_per_row> &,
    bool)
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
    bool flag_per_row,
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
    IColumn::Offset & current_offset,
    KnownRowsHolder<flag_per_row> & known_rows,
    bool is_last_disjunct)
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
        used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
        /// An ALL join emits a right row for each matching left row, so nothing is claimed here.
        addFoundRowAll<Map>(mapped, added_columns, current_offset, known_rows, nullptr, is_last_disjunct);
    }
    else if constexpr ((join_features.is_any_join || join_features.is_semi_join) && join_features.right)
    {
        static_assert(join_features.emits_whole_key_per_word);
        /// Each right row is emitted for exactly one left row, so the claimed rows must be the emitted
        /// ones. With several disjuncts a right row is reachable through several keys, hence the claim
        /// is taken per row by `addFoundRowAll`, which emits only what it claims.
        if constexpr (flag_per_row)
        {
            static_assert(join_features.need_flags, "RIGHT ANY/SEMI JOIN needs flags to claim the rows it emits");
            if (addFoundRowAll<Map>(mapped, added_columns, current_offset, known_rows, &used_flags, is_last_disjunct))
                setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
        }
        /// A single disjunct reaches a right row through one key only, so claiming the key claims all of its rows.
        else if (used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result))
        {
            setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
            addFoundRowAll<Map>(mapped, added_columns, current_offset, known_rows, nullptr, is_last_disjunct);
        }
    }
    else if constexpr (join_features.is_any_join && join_features.inner)
    {
        bool used_once = used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result);

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
            used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
    }
    else /// ANY LEFT, SEMI LEFT, old ANY (RightAny)
    {
        setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
        used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
        added_columns.appendFromBlock(firstRefWord(mapped));
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <HashJoin::Type TYPE, bool need_filter, bool fast_path, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumns(
    const HashJoinTable & table,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    const ScatteredBlock::Selector::Range & selector,
    KeyRange key_range)
{
    static constexpr bool flag_per_row = false; // Always false in single map case
    const auto & join_keys = added_columns.join_on_keys.at(0);

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
    if constexpr (!flag_per_row && join_features.emits_whole_key_per_word)
        added_columns.lazy_output.output_by_row_list = true;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = IColumn::Offsets(rows);

    IColumn::Offset current_offset = 0;
    table.probe<TYPE, MapsTemplate, fast_path>(
        join_keys,
        key_range,
        selector,
        added_columns.enable_prefetch,
        current_offset,
        [&](size_t i, size_t ind, const auto & find_result, IColumn::Offset & offset) __attribute__((always_inline))
        {
            bool right_row_found = false;
            KnownRowsHolder<flag_per_row> dummy_known_rows;

            if (find_result.isFound())
            {
                right_row_found = true;
                processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsTemplate, Map, KeyGetter>(
                    find_result, added_columns, used_flags, i, ind, offset, dummy_known_rows, /*is_last_disjunct=*/ true);
            }

            if (!right_row_found)
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

/// Joins right table columns which indexes are present in right_indexes using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <HashJoin::Type TYPE, bool need_filter, bool fast_path, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumns(
    Tables tables,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    const ScatteredBlock::Selector::Range & selector,
    KeyRange key_range)
{
    static constexpr bool flag_per_row = true; // Always true in multiple maps case

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
    if constexpr (!flag_per_row && join_features.emits_whole_key_per_word)
        added_columns.lazy_output.output_by_row_list = true;

    if constexpr (join_features.need_replication)
    {
        added_columns.offsets_to_replicate.clear();
        added_columns.offsets_to_replicate.reserve(rows);
    }

    constexpr bool stops_at_first_match
        = join_features.is_any_or_semi_join && !(join_features.is_any_join && (join_features.right || join_features.full));

    size_t max_joined_rows = added_columns.max_joined_block_rows > 0 ? added_columns.max_joined_block_rows : std::numeric_limits<size_t>::max();

    IColumn::Offset current_offset = 0;
    return HashJoinTable::probeAll<
        TYPE,
        MapsTemplate,
        fast_path,
        stops_at_first_match,
        /*prefetch_later_maps=*/true,
        KnownRowsHolder<flag_per_row>>(
        tables,
        added_columns.join_on_keys,
        key_range,
        selector,
        added_columns.enable_prefetch,
        max_joined_rows,
        current_offset,
        [&](size_t i, size_t ind, const auto & find_result, bool is_last_disjunct, KnownRowsHolder<flag_per_row> & known_rows,
            IColumn::Offset & offset) __attribute__((always_inline))
        {
            processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsTemplate, Map, KeyGetter>(
                find_result, added_columns, used_flags, i, ind, offset, known_rows, is_last_disjunct);
        },
        [&](size_t i, bool right_row_found, IColumn::Offset & offset) __attribute__((always_inline))
        {
            if (!right_row_found)
            {
                if constexpr (join_features.is_anti_join && join_features.left)
                    setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
                addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, offset);
            }

            if constexpr (join_features.need_replication)
                added_columns.offsets_to_replicate.push_back(offset);
        });
}

template <typename AddedColumns, typename Selector>
static ColumnPtr buildAdditionalFilter(
    const Selector & selector,
    const PODArray<UInt64> & selected_rows,
    const IColumn::Offsets & row_replicate_offset,
    const AddedColumns & added_columns)
{
    ColumnPtr result_column;
    do
    {
        if (selected_rows.empty())
        {
            result_column = ColumnUInt8::create();
            break;
        }

        if (!added_columns.additional_filter_expression)
        {
            auto filter = ColumnUInt8::create();
            filter->insertMany(1, selected_rows.size());
            result_column = std::move(filter);
            break;
        }

        auto required_cols = added_columns.additional_filter_expression->getRequiredColumnsWithTypes();
        if (required_cols.empty())
        {
            Block block;
            added_columns.additional_filter_expression->execute(block);
            result_column = block.getByPosition(0).column->cloneResized(selected_rows.size());
            break;
        }

        ColumnsWithTypeAndName required_columns;
        required_columns.reserve(required_cols.size());
        auto rhs_pos_it = added_columns.additional_filter_required_rhs_pos.begin();
        auto req_cols_it = required_cols.begin();
        for (size_t pos = 0; pos < required_cols.size(); ++pos, ++req_cols_it)
        {
            if (rhs_pos_it != added_columns.additional_filter_required_rhs_pos.end() && pos == rhs_pos_it->first)
            {
                const auto & req_col = *req_cols_it;
                required_columns.emplace_back(nullptr, req_col.type, req_col.name);

                auto col = req_col.type->createColumn();
                for (const UInt64 selected_row : selected_rows)
                {
                    const auto * block = added_columns.lazy_output.stored_columns[refWordBlockNo(selected_row)];
                    const auto [src_col, row_pos] = getBlockColumnAndRow(block, refWordRowNo(selected_row), rhs_pos_it->second);
                    col->insertFrom(*src_col, row_pos);
                }
                required_columns[pos].column = std::move(col);
                ++rhs_pos_it;
            }
            else
            {
                const auto & col_name = req_cols_it->name;
                const auto * src_col = added_columns.left_block.findByName(col_name);
                if (!src_col)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "required columns: [{}], but not found any in left table. left table: {}, required column: {}",
                        required_cols.toString(),
                        added_columns.left_block.dumpNames(),
                        col_name);

                auto new_col = src_col->column->cloneEmpty();
                for (size_t i = 0; i < row_replicate_offset.size(); ++i)
                {
                    size_t rows = row_replicate_offset[i] - row_replicate_offset[i - 1];
                    if (rows)
                    {
                        new_col->insertManyFrom(*src_col->column, selector[i], rows);
                    }
                }
                required_columns.push_back({std::move(new_col), src_col->type, col_name});
            }
        }

        Block executed_block(std::move(required_columns));

        for (const auto & col : executed_block.getColumnsWithTypeAndName())
            if (!col.column || !col.type)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Illegal nullptr column in input block: {}", executed_block.dumpStructure());

        added_columns.additional_filter_expression->execute(executed_block);
        result_column = executed_block.getByPosition(0).column->convertToFullColumnIfConst();
        executed_block.clear();
    } while (false);

    result_column = result_column->convertToFullIfWrapped()->convertToFullColumnIfLowCardinality();
    if (result_column->isNullable())
    {
        /// Convert Nullable(UInt8) to UInt8 ensuring that nulls are zeros
        /// Trying to avoid copying data, since we are the only owner of the column.
        ColumnPtr mask_column = assert_cast<const ColumnNullable &>(*result_column).getNullMapColumnPtr();

        MutableColumnPtr mutable_column;
        {
            ColumnPtr nested_column = assert_cast<const ColumnNullable &>(*result_column).getNestedColumnPtr();
            result_column.reset();
            mutable_column = IColumn::mutate(std::move(nested_column));
        }

        auto & column_data = assert_cast<ColumnUInt8 &>(*mutable_column).getData();
        const auto & mask_column_data = assert_cast<const ColumnUInt8 &>(*mask_column).getData();
        for (size_t i = 0; i < column_data.size(); ++i)
        {
            if (mask_column_data[i])
                column_data[i] = 0;
        }
        return mutable_column;
    }
    return result_column;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <HashJoin::Type TYPE, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsWithAdditionalFilter(
    Tables tables,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]],
    const ScatteredBlock::Selector & selector,
    bool need_filter [[maybe_unused]],
    bool flag_per_row [[maybe_unused]],
    KeyRange key_range)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    using Map = const JoinMapOf<TYPE, MapsTemplate>;
    using KeyGetter = typename KeyGetterForType<TYPE, Map>::Type;

    size_t left_block_rows = selector.size();
    if (need_filter)
    {
        added_columns.filter = IColumn::Filter(left_block_rows, 0);
        added_columns.matched_rows.reserve(left_block_rows);
    }

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = IColumn::Offsets(left_block_rows);

    using FindResult = typename KeyGetter::FindResult;

    /// One ref per right row, so the additional filter can accept or reject each row.
    class PreSelectedRows
    {
    public:
        explicit PreSelectedRows(PODArray<UInt64> & container_) : container(container_) {}
        void appendFromBlock(UInt64 ref_word) { container.push_back(ref_word); }
        static constexpr bool appendsWholeKey() { return false; }

        PODArray<UInt64> & container;
    };

    PODArray<UInt64> selected_rows;
    selected_rows.reserve(left_block_rows);
    std::vector<FindResult> find_results;
    find_results.reserve(left_block_rows);
    IColumn::Offset total_added_rows = 0;

    IColumn::Offsets row_replicate_offset;
    row_replicate_offset.reserve(left_block_rows);

    size_t max_joined_rows = added_columns.max_joined_block_rows;
    if (max_joined_rows == 0)
        max_joined_rows = std::numeric_limits<size_t>::max();

    {
        IColumn::Offset current_added_rows = 0;

        struct KnownRows
        {
            KnownRowsHolder<true> all_flag_known_rows;
            KnownRowsHolder<false> single_flag_know_rows;
        };
        HashJoinTable::probeAll<
            TYPE,
            MapsTemplate,
            /*fast_path=*/false,
            /*stops_at_first_match=*/false,
            /*prefetch_later_maps=*/false,
            KnownRows,
            /*lazy_row_filter=*/true>(
            tables,
            added_columns.join_on_keys,
            key_range,
            selector.getRange(),
            added_columns.enable_prefetch,
            max_joined_rows,
            current_added_rows,
            [&](size_t, size_t, const FindResult & find_result, bool is_last_disjunct, KnownRows & known_rows, IColumn::Offset & offset)
            {
                auto & mapped = find_result.getMapped();
                find_results.push_back(find_result);
                /// it's different from `joinRightColumns`.
                PreSelectedRows selected_rows_view{selected_rows};
                if (flag_per_row)
                    addFoundRowAll<Map, true>(mapped, selected_rows_view, offset, known_rows.all_flag_known_rows, nullptr, is_last_disjunct);
                else
                    addFoundRowAll<Map, false>(mapped, selected_rows_view, offset, known_rows.single_flag_know_rows, nullptr, is_last_disjunct);
            },
            [&](size_t, bool, IColumn::Offset & offset) { row_replicate_offset.push_back(offset); });

        if (selected_rows.size() != current_added_rows)
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Sizes are mismatched. selected_rows.size:{}, current_added_rows:{}, row_replicate_offset.size:{}",
                selected_rows.size(),
                current_added_rows,
                row_replicate_offset.size());
    }

    left_block_rows = row_replicate_offset.size();

    {
        auto filter_col = buildAdditionalFilter(selector, selected_rows, row_replicate_offset, added_columns);

        const PaddedPODArray<UInt8> & filter_flags = assert_cast<const ColumnUInt8 &>(*filter_col).getData();

        if (added_columns.match_stats) [[unlikely]]
            for (size_t row = 0; row < selected_rows.size(); ++row)
                if (filter_flags[row])
                    added_columns.match_stats->markRightMatched(selected_rows[row]);

        [[maybe_unused]] UInt64 matched_left = 0;
        size_t prev_replicated_row = 0;
        auto * selected_right_row_it = selected_rows.begin();
        size_t find_result_index = 0;
        for (size_t i = 0, n = row_replicate_offset.size(); i < n; ++i)
        {
            bool any_matched = false;
            /// right/full join or multiple disjuncts, we need to mark used flags for each row.
            if (flag_per_row)
            {
                for (size_t replicated_row = prev_replicated_row; replicated_row < row_replicate_offset[i]; ++replicated_row)
                {
                    if (filter_flags[replicated_row])
                    {
                        const UInt64 selected_ref = *selected_right_row_it;
                        if constexpr (join_features.is_semi_join || join_features.is_any_join)
                        {
                            /// For LEFT/INNER SEMI/ANY JOIN, we need to add only first appeared row from left,
                            if constexpr (join_features.left || join_features.inner)
                            {
                                if (!any_matched)
                                {
                                    // For inner join, we need mark each right row'flag, because we only use each right row once.
                                    auto used_once = used_flags.template setUsedOnce<join_features.need_flags, true>(
                                        refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                                    if (used_once)
                                    {
                                        any_matched = true;
                                        total_added_rows += 1;
                                        added_columns.appendFromBlock(selected_ref);
                                    }
                                }
                            }
                            else
                            {
                                auto used_once = used_flags.template setUsedOnce<join_features.need_flags, true>(
                                    refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                                if (used_once)
                                {
                                    any_matched = true;
                                    total_added_rows += 1;
                                    added_columns.appendFromBlock(selected_ref);
                                }
                            }
                        }
                        else if constexpr (join_features.is_anti_join)
                        {
                            any_matched = true;
                            if constexpr (join_features.right && join_features.need_flags)
                                used_flags.template setUsed<true, true>(refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                        }
                        else
                        {
                            any_matched = true;
                            total_added_rows += 1;
                            added_columns.appendFromBlock(selected_ref);
                            used_flags.template setUsed<join_features.need_flags, true>(refWordBlockNo(selected_ref), refWordRowNo(selected_ref), 0);
                        }
                    }

                    ++selected_right_row_it;
                }
            }
            else
            {
                for (size_t replicated_row = prev_replicated_row; replicated_row < row_replicate_offset[i]; ++replicated_row)
                {
                    if constexpr (join_features.is_anti_join)
                    {
                        any_matched |= filter_flags[replicated_row];
                    }
                    else if constexpr (join_features.need_replication)
                    {
                        if (filter_flags[replicated_row])
                        {
                            any_matched = true;
                            added_columns.appendFromBlock(*selected_right_row_it);
                            total_added_rows += 1;
                        }
                        ++selected_right_row_it;
                    }
                    else
                    {
                        if (filter_flags[replicated_row])
                        {
                            any_matched = true;
                            added_columns.appendFromBlock(*selected_right_row_it);
                            total_added_rows += 1;
                            selected_right_row_it = selected_right_row_it + row_replicate_offset[i] - replicated_row;
                            break;
                        }
                        ++selected_right_row_it;
                    }
                }
            }


            if constexpr (join_features.is_anti_join)
            {
                if (!any_matched)
                {
                    if constexpr (join_features.left)
                        if (need_filter)
                            setUsed<true>(added_columns.filter, i, added_columns.matched_rows);
                    addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, total_added_rows);
                }
            }
            else
            {
                if (!any_matched)
                {
                    addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, total_added_rows);
                }
                else
                {
                    if (!flag_per_row)
                        used_flags.template setUsed<join_features.need_flags, false>(find_results[find_result_index]);
                    if (need_filter)
                        setUsed<true>(added_columns.filter, i, added_columns.matched_rows);
                }
            }
            if constexpr (leftMatchedSource(KIND, STRICTNESS) == LeftMatchedSource::DefaultRowMarkers)
                matched_left += any_matched;

            find_result_index += (prev_replicated_row != row_replicate_offset[i]);

            if constexpr (join_features.need_replication)
            {
                added_columns.offsets_to_replicate[i] = total_added_rows;
            }
            prev_replicated_row = row_replicate_offset[i];
        }

        if constexpr (leftMatchedSource(KIND, STRICTNESS) == LeftMatchedSource::DefaultRowMarkers)
            added_columns.matched_left_rows = matched_left;
    }

    if constexpr (join_features.need_replication)
    {
        added_columns.offsets_to_replicate.resize(left_block_rows);
        added_columns.filter.resize(left_block_rows);
    }
    else if (need_filter)
    {
        /// The loop above may break early at max_joined_block_rows, producing fewer left rows
        /// than the selector size the filter was allocated for. Trim the filter to the number of
        /// processed rows so the required right key column built from it matches the left block,
        /// which is cut to left_block_rows downstream.
        added_columns.filter.resize(left_block_rows);
    }
    return left_block_rows;
}

}
