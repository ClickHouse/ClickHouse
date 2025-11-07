#pragma once

#include <Columns/IColumn.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/HashJoin/AddedColumns.h>
#include <Interpreters/HashJoin/HashJoinMethods.h>
#include <Interpreters/HashJoin/HashJoinResult.h>
#include <Interpreters/JoinUtils.h>

#include <algorithm>
#include <type_traits>

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED_JOIN_KEYS;
extern const int LOGICAL_ERROR;
}
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::insertFromBlockImpl(
    HashJoin & join,
    HashJoin::Type type,
    MapsTemplate & maps,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ColumnsInfo * stored_columns_info,
    const ScatteredBlock::Selector & selector,
    ConstNullMapPtr null_map,
    const JoinCommon::JoinMask & join_mask,
    Arena & pool,
    bool & is_inserted,
    bool & all_values_unique)
{
    switch (type)
    {
        case HashJoin::Type::EMPTY:
            [[fallthrough]];
        case HashJoin::Type::CROSS:
            /// Do nothing. We will only save block, and it is enough
            is_inserted = true;
            break;

#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        if (selector.isContinuousRange()) \
            insertFromBlockImplTypeCase< \
                typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
                join, *maps.TYPE, key_columns, key_sizes, stored_columns_info, selector.getRange(), null_map, join_mask, pool, is_inserted, all_values_unique); \
        else \
            insertFromBlockImplTypeCase< \
                typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
                join, *maps.TYPE, key_columns, key_sizes, stored_columns_info, selector.getIndexes(), null_map, join_mask, pool, is_inserted, all_values_unique); \
        break;

            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
JoinResultPtr HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinBlockImpl(
    const HashJoin & join, Block block, const Block & block_with_columns_to_add, const MapsTemplateVector & maps_, bool is_join_get)
{
    ScatteredBlock scattered_block{std::move(block)};
    return joinBlockImpl(join, std::move(scattered_block), block_with_columns_to_add, maps_, is_join_get);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
JoinResultPtr HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinBlockImpl(
    const HashJoin & join,
    ScatteredBlock block,
    const Block & block_with_columns_to_add,
    const MapsTemplateVector & maps_,
    bool is_join_get)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    std::vector<JoinOnKeyColumns> join_on_keys;
    const auto & onexprs = join.table_join->getClauses();
    for (size_t i = 0; i < onexprs.size(); ++i)
    {
        const auto & key_names = !is_join_get ? onexprs[i].key_names_left : onexprs[i].key_names_right;
        join_on_keys.emplace_back(block, key_names, onexprs[i].condColumnNames().first, join.key_sizes[i]);
    }


    /** For LEFT/INNER JOIN, the saved blocks do not contain keys.
      * For FULL/RIGHT JOIN, the saved blocks contain keys;
      *  but they will not be used at this stage of joining (and will be in `AdderNonJoined`), and they need to be skipped.
      * For ASOF, the last column is used as the ASOF column
      */
    AddedColumns<!join_features.is_any_join> added_columns(
        block,
        block_with_columns_to_add,
        join.savedBlockSample(),
        join,
        std::move(join_on_keys),
        join.table_join->getMixedJoinExpression(),
        join.additional_filter_required_rhs_pos,
        join_features.is_asof_join,
        is_join_get);

    bool has_required_right_keys = (join.required_right_keys.columns() != 0);
    added_columns.need_filter = join_features.need_filter || has_required_right_keys;
    added_columns.max_joined_block_rows = join.max_joined_block_rows;
    if (!added_columns.max_joined_block_rows)
        added_columns.max_joined_block_rows = std::numeric_limits<size_t>::max();
    else
        added_columns.reserve(join_features.need_replication);

    switchJoinRightColumns(maps_, added_columns, block.getSelector(), join.data->type, *join.used_flags);
    /// Do not hold memory for join_on_keys anymore
    added_columns.join_on_keys.clear();

    return std::make_unique<HashJoinResult>(
        std::move(added_columns.lazy_output),
        std::move(added_columns.columns),
        std::move(added_columns.offsets_to_replicate),
        std::move(added_columns.filter),
        std::move(added_columns.matched_rows),
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
        });
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename KeyGetter, bool is_asof_join>
KeyGetter HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::createKeyGetter(const ColumnRawPtrs & key_columns, const Sizes & key_sizes)
{
    if constexpr (is_asof_join)
    {
        auto key_column_copy = key_columns;
        auto key_size_copy = key_sizes;
        key_column_copy.pop_back();
        key_size_copy.pop_back();
        return KeyGetter(key_column_copy, key_size_copy, nullptr);
    }
    else
        return KeyGetter(key_columns, key_sizes, nullptr);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename KeyGetter, typename HashMap, typename Selector>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::insertFromBlockImplTypeCase(
    HashJoin & join,
    HashMap & map,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const ColumnsInfo * stored_columns_info,
    const Selector & selector,
    ConstNullMapPtr null_map,
    const JoinCommon::JoinMask & join_mask,
    Arena & pool,
    bool & is_inserted,
    bool & all_values_unique)
{
    [[maybe_unused]] constexpr bool mapped_one = std::is_same_v<typename HashMap::mapped_type, RowRef>;
    constexpr bool is_asof_join = STRICTNESS == JoinStrictness::Asof;

    const IColumn * asof_column [[maybe_unused]] = nullptr;
    if constexpr (is_asof_join)
        asof_column = key_columns.back();

    auto key_getter = createKeyGetter<KeyGetter, is_asof_join>(key_columns, key_sizes);

    /// For ALL and ASOF join always insert values
    is_inserted = !mapped_one || is_asof_join;

    size_t rows = 0;
    if constexpr (std::is_same_v<std::decay_t<Selector>, ScatteredBlock::Indexes>)
        rows = selector.getData().size();
    else
        rows = selector.second - selector.first;

    for (size_t i = 0; i < rows; ++i)
    {
        size_t ind = 0;
        if constexpr (std::is_same_v<std::decay_t<Selector>, ScatteredBlock::Indexes>)
            ind = selector.getData()[i];
        else
            ind = selector.first + i;

        chassert(!null_map || ind < null_map->size());
        if (null_map && (*null_map)[ind])
        {
            /// nulls are not inserted into hash table,
            /// keep them for RIGHT and FULL joins
            is_inserted = true;
            continue;
        }

        /// Check condition for right table from ON section
        if (join_mask.isRowFiltered(ind))
            continue;

        if constexpr (is_asof_join)
            Inserter<HashMap, KeyGetter>::insertAsof(join, map, key_getter, stored_columns_info, ind, pool, *asof_column);
        else if constexpr (mapped_one)
            is_inserted |= Inserter<HashMap, KeyGetter>::insertOne(join, map, key_getter, stored_columns_info, ind, pool);
        else
            all_values_unique &= Inserter<HashMap, KeyGetter>::insertAll(join, map, key_getter, stored_columns_info, ind, pool);
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename AddedColumns>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::switchJoinRightColumns(
    const std::vector<const MapsTemplate *> & mapv,
    AddedColumns & added_columns,
    const ScatteredBlock::Selector & selector,
    HashJoin::Type type,
    JoinStuff::JoinUsedFlags & used_flags)
{
    constexpr bool is_asof_join = STRICTNESS == JoinStrictness::Asof;
    switch (type)
    {
        case HashJoin::Type::EMPTY: {
            if constexpr (!is_asof_join)
            {
                using KeyGetter = KeyGetterEmpty<typename MapsTemplate::MappedType>;
                std::vector<KeyGetter> key_getter_vector;
                key_getter_vector.emplace_back();

                using MapTypeVal = typename KeyGetter::MappedType;
                std::vector<const MapTypeVal *> a_map_type_vector;
                a_map_type_vector.emplace_back();
                return joinRightColumnsSwitchNullability<KeyGetter>(
                    std::move(key_getter_vector), a_map_type_vector, added_columns, selector, used_flags);
            }
            throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys. Type: {}", type);
        }
#define M(TYPE) \
    case HashJoin::Type::TYPE: { \
        using MapTypeVal = const typename std::remove_reference_t<decltype(MapsTemplate::TYPE)>::element_type; \
        using KeyGetter = typename KeyGetterForType<HashJoin::Type::TYPE, MapTypeVal>::Type; \
        std::vector<const MapTypeVal *> a_map_type_vector(mapv.size()); \
        std::vector<KeyGetter> key_getter_vector; \
        for (size_t d = 0; d < added_columns.join_on_keys.size(); ++d) \
        { \
            const auto & join_on_key = added_columns.join_on_keys[d]; \
            a_map_type_vector[d] = mapv[d]->TYPE.get(); \
            key_getter_vector.push_back( \
                std::move(createKeyGetter<KeyGetter, is_asof_join>(join_on_key.key_columns, join_on_key.key_sizes))); \
        } \
        return joinRightColumnsSwitchNullability<KeyGetter>(std::move(key_getter_vector), a_map_type_vector, added_columns, selector, used_flags); \
    }
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M

        default:
            throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys (type: {})", type);
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename KeyGetter, typename Map, typename AddedColumns>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchNullability(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    const ScatteredBlock::Selector & selector,
    JoinStuff::JoinUsedFlags & used_flags)
{
    if (added_columns.need_filter)
        joinRightColumnsSwitchMultipleDisjuncts<KeyGetter, Map, true>(
            std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, selector, used_flags);
    else
        joinRightColumnsSwitchMultipleDisjuncts<KeyGetter, Map, false>(
            std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, selector, used_flags);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename KeyGetter, typename Map, bool need_filter, typename AddedColumns>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchMultipleDisjuncts(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    const ScatteredBlock::Selector & selector,
    JoinStuff::JoinUsedFlags & used_flags)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    if constexpr (join_features.is_maps_all)
    {
        if (added_columns.additional_filter_expression)
        {
            const bool mark_per_row_used = join_features.right || join_features.full || mapv.size() > 1;
            joinRightColumnsWithAddtitionalFilter<KeyGetter, Map>(
                std::forward<std::vector<KeyGetter>>(key_getter_vector),
                mapv,
                added_columns,
                used_flags,
                selector,
                need_filter,
                mark_per_row_used);

            return;
        }
    }

    if (added_columns.additional_filter_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Additional filter expression is not supported for this JOIN");

    if (selector.isContinuousRange())
    {
        if (mapv.size() > 1 || added_columns.join_on_keys.empty())
        {
            if (std::ranges::any_of(added_columns.join_on_keys, [](const auto & elem) { return elem.null_map; }))
                joinRightColumnsSwitchJoinMaskKind<KeyGetter, Map, need_filter, /*check_null_map=*/true>(
                    std::move(key_getter_vector), mapv, added_columns, used_flags, selector.getRange());
            else
                joinRightColumnsSwitchJoinMaskKind<KeyGetter, Map, need_filter, /*check_null_map=*/false>(
                    std::move(key_getter_vector), mapv, added_columns, used_flags, selector.getRange());
        }
        else
        {
            chassert(key_getter_vector.size() == 1);
            if (added_columns.join_on_keys.at(0).null_map)
                joinRightColumnsSwitchJoinMaskKind<KeyGetter, Map, need_filter, /*check_null_map=*/true>(
                    key_getter_vector.at(0), mapv.at(0), added_columns, used_flags, selector.getRange());
            else
                joinRightColumnsSwitchJoinMaskKind<KeyGetter, Map, need_filter, /*check_null_map=*/false>(
                    key_getter_vector.at(0), mapv.at(0), added_columns, used_flags, selector.getRange());
        }
    }
    else
    {
        if (mapv.size() > 1 || added_columns.join_on_keys.empty())
        {
            if (std::ranges::any_of(added_columns.join_on_keys, [](const auto & elem) { return elem.null_map; }))
                joinRightColumnsSwitchJoinMaskKind<KeyGetter, Map, need_filter, /*check_null_map=*/true>(
                    std::move(key_getter_vector), mapv, added_columns, used_flags, selector.getIndexes());
            else
                joinRightColumnsSwitchJoinMaskKind<KeyGetter, Map, need_filter, /*check_null_map=*/false>(
                    std::move(key_getter_vector), mapv, added_columns, used_flags, selector.getIndexes());
        }
        else
        {
            chassert(key_getter_vector.size() == 1);
            if (added_columns.join_on_keys.at(0).null_map)
                joinRightColumnsSwitchJoinMaskKind<KeyGetter, Map, need_filter, /*check_null_map=*/true>(
                    key_getter_vector.at(0), mapv.at(0), added_columns, used_flags, selector.getIndexes());
            else
                joinRightColumnsSwitchJoinMaskKind<KeyGetter, Map, need_filter, /*check_null_map=*/false>(
                    key_getter_vector.at(0), mapv.at(0), added_columns, used_flags, selector.getIndexes());
        }
    }
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

template <
    JoinKind KIND,
    JoinStrictness STRICTNESS,
    bool need_filter,
    bool flag_per_row,
    typename MapsTemplate,
    typename Map,
    typename KeyGetter,
    typename AddedColumns>
void processMatch(
    const typename KeyGetter::FindResult & find_result,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    size_t i,
    size_t ind,
    IColumn::Offset & current_offset,
    KnownRowsHolder<flag_per_row> & known_rows)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    auto & mapped = find_result.getMapped();
    if constexpr (join_features.is_asof_join)
    {
        const IColumn & left_asof_key = added_columns.leftAsofKey();

        auto row_ref = mapped->findAsof(left_asof_key, ind);
        if (row_ref && row_ref->columns_info)
        {
            setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
            if constexpr (flag_per_row)
                used_flags.template setUsed<join_features.need_flags, flag_per_row>(&row_ref->columns_info->columns, row_ref->row_num, 0);
            else
                used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);

            added_columns.appendFromBlock(row_ref, join_features.add_missing);
        }
        else
            addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
    }
    else if constexpr (join_features.is_all_join)
    {
        setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
        used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
        auto used_flags_opt = join_features.need_flags ? &used_flags : nullptr;
        addFoundRowAll<Map, join_features.add_missing>(mapped, added_columns, current_offset, known_rows, used_flags_opt);
    }
    else if constexpr ((join_features.is_any_join || join_features.is_semi_join) && join_features.right)
    {
        /// Use first appeared left key + it needs left columns replication
        bool used_once = used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result);
        if (used_once)
        {
            auto used_flags_opt = join_features.need_flags ? &used_flags : nullptr;
            setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
            addFoundRowAll<Map, join_features.add_missing>(mapped, added_columns, current_offset, known_rows, used_flags_opt);
        }
    }
    else if constexpr (join_features.is_any_join && join_features.inner)
    {
        bool used_once = used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result);

        /// Use first appeared left key only
        if (used_once)
        {
            setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
            added_columns.appendFromBlock(&mapped, join_features.add_missing);
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
        added_columns.appendFromBlock(&mapped, join_features.add_missing);
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <
    typename KeyGetter,
    typename Map,
    bool need_filter,
    bool check_null_map,
    JoinCommon::JoinMask::Kind join_mask_kind,
    typename AddedColumns,
    typename Selector>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumns(
    KeyGetter & key_getter, const Map * map, AddedColumns & added_columns, JoinStuff::JoinUsedFlags & used_flags, const Selector & selector)
{
    static constexpr bool flag_per_row = false; // Always false in single map case
    const auto & join_keys = added_columns.join_on_keys.at(0);

    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    size_t rows = ScatteredBlock::Selector::size(selector);
    if constexpr (need_filter)
    {
        added_columns.filter = IColumn::Filter(rows, 0);
        added_columns.matched_rows.reserve(rows);
    }
    if constexpr (!flag_per_row && (STRICTNESS == JoinStrictness::All || (STRICTNESS == JoinStrictness::Semi && KIND == JoinKind::Right)))
        added_columns.lazy_output.output_by_row_list = true;

    Arena pool;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = IColumn::Offsets(rows);

    IColumn::Offset current_offset = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        size_t ind = 0;
        if constexpr (std::is_same_v<std::decay_t<Selector>, ScatteredBlock::Indexes>)
            ind = selector.getData()[i];
        else
            ind = selector.first + i;

        bool right_row_found = false;
        KnownRowsHolder<flag_per_row> dummy_known_rows;

        bool skip_row = false;
        if constexpr (check_null_map)
            skip_row = join_keys.null_map && (*join_keys.null_map)[ind];

        if (!skip_row)
        {
            bool row_acceptable;
            if constexpr (join_mask_kind == JoinCommon::JoinMask::Kind::AllFalse)
                row_acceptable = false;
            else if constexpr (join_mask_kind == JoinCommon::JoinMask::Kind::AllTrue)
                row_acceptable = true;
            else
                row_acceptable = !join_keys.isRowFiltered(ind);

            using FindResult = typename KeyGetter::FindResult;
            auto find_result = row_acceptable ? key_getter.findKey(*map, ind, pool) : FindResult();

            if (find_result.isFound())
            {
                right_row_found = true;
                processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsTemplate, Map, KeyGetter>(
                    find_result, added_columns, used_flags, i, ind, current_offset, dummy_known_rows);
            }
        }

        if (!right_row_found)
        {
            if constexpr (join_features.is_anti_join && join_features.left)
                setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
            addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
        }

        if constexpr (join_features.need_replication)
        {
            added_columns.offsets_to_replicate[i] = current_offset;
        }
    }

    added_columns.applyLazyDefaults();
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename KeyGetter, typename Map, bool need_filter, bool check_null_map, typename AddedColumns, typename Selector>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchJoinMaskKind(
    KeyGetter & key_getter, const Map * map, AddedColumns & added_columns, JoinStuff::JoinUsedFlags & used_flags, const Selector & selector)
{
    switch (added_columns.join_on_keys.at(0).join_mask_column.getKind())
    {
        case JoinCommon::JoinMask::Kind::Unknown:
            return joinRightColumns<KeyGetter, Map, need_filter, check_null_map, JoinCommon::JoinMask::Kind::Unknown>(
                key_getter, map, added_columns, used_flags, selector);
        case JoinCommon::JoinMask::Kind::AllFalse:
            return joinRightColumns<KeyGetter, Map, need_filter, check_null_map, JoinCommon::JoinMask::Kind::AllFalse>(
                key_getter, map, added_columns, used_flags, selector);
        case JoinCommon::JoinMask::Kind::AllTrue:
            return joinRightColumns<KeyGetter, Map, need_filter, check_null_map, JoinCommon::JoinMask::Kind::AllTrue>(
                key_getter, map, added_columns, used_flags, selector);
    }
}

/// Joins right table columns which indexes are present in right_indexes using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <
    typename KeyGetter,
    typename Map,
    bool need_filter,
    bool check_null_map,
    JoinCommon::JoinMask::Kind join_mask_kind,
    typename AddedColumns,
    typename Selector>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumns(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    const Selector & selector)
{
    static constexpr bool flag_per_row = true; // Always true in multiple maps case

    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    size_t rows = ScatteredBlock::Selector::size(selector);
    if constexpr (need_filter)
    {
        added_columns.filter = IColumn::Filter(rows, 0);
        added_columns.matched_rows.reserve(rows);
    }
    if constexpr (!flag_per_row && (STRICTNESS == JoinStrictness::All || (STRICTNESS == JoinStrictness::Semi && KIND == JoinKind::Right)))
        added_columns.lazy_output.output_by_row_list = true;

    Arena pool;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = IColumn::Offsets(rows);

    IColumn::Offset current_offset = 0;
    for (size_t i = 0; i < rows; ++i)
    {
        size_t ind = 0;
        if constexpr (std::is_same_v<std::decay_t<Selector>, ScatteredBlock::Indexes>)
            ind = selector.getData()[i];
        else
            ind = selector.first + i;

        bool right_row_found = false;
        KnownRowsHolder<flag_per_row> known_rows;
        for (size_t onexpr_idx = 0; onexpr_idx < added_columns.join_on_keys.size(); ++onexpr_idx)
        {
            const auto & join_keys = added_columns.join_on_keys[onexpr_idx];
            bool skip_row = false;
            if constexpr (check_null_map)
                skip_row = join_keys.null_map && (*join_keys.null_map)[ind];

            if (!skip_row)
            {
                bool row_acceptable;
                if constexpr (join_mask_kind == JoinCommon::JoinMask::Kind::AllFalse)
                    row_acceptable = false;
                else if constexpr (join_mask_kind == JoinCommon::JoinMask::Kind::AllTrue)
                    row_acceptable = true;
                else
                    row_acceptable = !join_keys.isRowFiltered(ind);

                using FindResult = typename KeyGetter::FindResult;
                auto find_result = row_acceptable ? key_getter_vector[onexpr_idx].findKey(*(mapv[onexpr_idx]), ind, pool) : FindResult();

                if (find_result.isFound())
                {
                    right_row_found = true;
                    processMatch<KIND, STRICTNESS, need_filter, flag_per_row, MapsTemplate, Map, KeyGetter>(
                        find_result, added_columns, used_flags, i, ind, current_offset, known_rows);

                    if constexpr ((join_features.is_any_join && join_features.inner) || (join_features.is_any_or_semi_join))
                        break;
                }
            }
        }

        if (!right_row_found)
        {
            if constexpr (join_features.is_anti_join && join_features.left)
                setUsed<need_filter>(added_columns.filter, i, added_columns.matched_rows);
            addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
        }

        if constexpr (join_features.need_replication)
        {
            added_columns.offsets_to_replicate[i] = current_offset;
        }
    }

    added_columns.applyLazyDefaults();
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <
    typename KeyGetter,
    typename Map,
    bool need_filter,
    bool check_null_map,
    typename AddedColumns,
    typename Selector>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchJoinMaskKind(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    const Selector & selector)
{
    using Kind = JoinCommon::JoinMask::Kind;
    if (std::ranges::all_of(added_columns.join_on_keys, [](const auto & elem) { return elem.join_mask_column.getKind() == Kind::AllTrue; }))
    {
        joinRightColumns<KeyGetter, Map, need_filter, check_null_map, Kind::AllTrue>(
            std::move(key_getter_vector), mapv, added_columns, used_flags, selector);
    }
    else if (std::ranges::all_of(added_columns.join_on_keys, [](const auto & elem) { return elem.join_mask_column.getKind() == Kind::AllFalse; }))
    {
        joinRightColumns<KeyGetter, Map, need_filter, check_null_map, Kind::AllFalse>(
            std::move(key_getter_vector), mapv, added_columns, used_flags, selector);
    }
    else
    {
        joinRightColumns<KeyGetter, Map, need_filter, check_null_map, Kind::Unknown>(
            std::move(key_getter_vector), mapv, added_columns, used_flags, selector);
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename AddedColumns, typename Selector>
ColumnPtr HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::buildAdditionalFilter(
    const Selector & selector,
    const std::vector<const RowRef *> & selected_rows,
    const std::vector<size_t> & row_replicate_offset,
    AddedColumns & added_columns)
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
                for (const auto & selected_row : selected_rows)
                {
                    const auto [src_col, row_num] = getBlockColumnAndRow(selected_row, rhs_pos_it->second);
                    col->insertFrom(*src_col, row_num);
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
                size_t prev_left_offset = 0;
                for (size_t i = 0; i < row_replicate_offset.size(); ++i)
                {
                    const size_t & left_offset = row_replicate_offset[i];
                    size_t rows = left_offset - prev_left_offset;
                    if (rows)
                        new_col->insertManyFrom(*src_col->column, selector[i], rows);
                    prev_left_offset = left_offset;
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

    result_column = result_column->convertToFullIfNeeded();
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
template <typename KeyGetter, typename Map, typename AddedColumns>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsWithAddtitionalFilter(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]],
    const ScatteredBlock::Selector & selector,
    bool need_filter [[maybe_unused]],
    bool flag_per_row [[maybe_unused]])
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    const size_t left_block_rows = selector.size();
    if (need_filter)
    {
        added_columns.filter = IColumn::Filter(left_block_rows, 0);
        added_columns.matched_rows.reserve(left_block_rows);
    }

    std::unique_ptr<Arena> pool;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = IColumn::Offsets(left_block_rows);

    std::vector<size_t> row_replicate_offset;
    row_replicate_offset.reserve(left_block_rows);

    using FindResult = typename KeyGetter::FindResult;
    PreSelectedRows selected_rows;
    selected_rows.reserve(left_block_rows);
    std::vector<FindResult> find_results;
    find_results.reserve(left_block_rows);
    IColumn::Offset total_added_rows = 0;
    IColumn::Offset current_added_rows = 0;

    {
        pool = std::make_unique<Arena>();
        current_added_rows = 0;
        for (size_t ind : selector)
        {
            KnownRowsHolder<true> all_flag_known_rows;
            KnownRowsHolder<false> single_flag_know_rows;
            for (size_t join_clause_idx = 0; join_clause_idx < added_columns.join_on_keys.size(); ++join_clause_idx)
            {
                const auto & join_keys = added_columns.join_on_keys[join_clause_idx];
                if (join_keys.null_map && (*join_keys.null_map)[ind])
                    continue;

                bool row_acceptable = !join_keys.isRowFiltered(ind);
                auto find_result
                    = row_acceptable ? key_getter_vector[join_clause_idx].findKey(*(mapv[join_clause_idx]), ind, *pool) : FindResult();

                if (find_result.isFound())
                {
                    auto & mapped = find_result.getMapped();
                    find_results.push_back(find_result);
                    /// We don't add missing in addFoundRowAll here. we will add it after filter is applied.
                    /// it's different from `joinRightColumns`.
                    if (flag_per_row)
                        addFoundRowAll<Map, false, true>(mapped, selected_rows, current_added_rows, all_flag_known_rows, nullptr);
                    else
                        addFoundRowAll<Map, false, false>(mapped, selected_rows, current_added_rows, single_flag_know_rows, nullptr);
                }
            }
            row_replicate_offset.push_back(current_added_rows);
        }
    }

    if (selected_rows.size() != current_added_rows)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Sizes are mismatched. selected_rows.size:{}, current_added_rows:{}, row_replicate_offset.size:{}",
            selected_rows.size(),
            current_added_rows,
            row_replicate_offset.size());

    auto filter_col = buildAdditionalFilter(selector, selected_rows, row_replicate_offset, added_columns);

    {
        const PaddedPODArray<UInt8> & filter_flags = assert_cast<const ColumnUInt8 &>(*filter_col).getData();

        size_t prev_replicated_row = 0;
        auto selected_right_row_it = selected_rows.begin();
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
                        if constexpr (join_features.is_semi_join || join_features.is_any_join)
                        {
                            /// For LEFT/INNER SEMI/ANY JOIN, we need to add only first appeared row from left,
                            if constexpr (join_features.left || join_features.inner)
                            {
                                if (!any_matched)
                                {
                                    // For inner join, we need mark each right row'flag, because we only use each right row once.
                                    auto used_once = used_flags.template setUsedOnce<join_features.need_flags, true>(
                                        &(*selected_right_row_it)->columns_info->columns, (*selected_right_row_it)->row_num, 0);
                                    if (used_once)
                                    {
                                        any_matched = true;
                                        total_added_rows += 1;
                                        added_columns.appendFromBlock(*selected_right_row_it, join_features.add_missing);
                                    }
                                }
                            }
                            else
                            {
                                auto used_once = used_flags.template setUsedOnce<join_features.need_flags, true>(
                                    &(*selected_right_row_it)->columns_info->columns, (*selected_right_row_it)->row_num, 0);
                                if (used_once)
                                {
                                    any_matched = true;
                                    total_added_rows += 1;
                                    added_columns.appendFromBlock(*selected_right_row_it, join_features.add_missing);
                                }
                            }
                        }
                        else if constexpr (join_features.is_anti_join)
                        {
                            any_matched = true;
                            if constexpr (join_features.right && join_features.need_flags)
                                used_flags.template setUsed<true, true>(&(*selected_right_row_it)->columns_info->columns, (*selected_right_row_it)->row_num, 0);
                        }
                        else
                        {
                            any_matched = true;
                            total_added_rows += 1;
                            added_columns.appendFromBlock(*selected_right_row_it, join_features.add_missing);
                            used_flags.template setUsed<join_features.need_flags, true>(&(*selected_right_row_it)->columns_info->columns, (*selected_right_row_it)->row_num, 0);
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
                            added_columns.appendFromBlock(*selected_right_row_it, join_features.add_missing);
                            total_added_rows += 1;
                        }
                        ++selected_right_row_it;
                    }
                    else
                    {
                        if (filter_flags[replicated_row])
                        {
                            any_matched = true;
                            added_columns.appendFromBlock(*selected_right_row_it, join_features.add_missing);
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
                    if constexpr (join_features.add_missing)
                        added_columns.applyLazyDefaults();
                }
            }
            find_result_index += (prev_replicated_row != row_replicate_offset[i]);

            if constexpr (join_features.need_replication)
            {
                added_columns.offsets_to_replicate[i] = total_added_rows;
            }
            prev_replicated_row = row_replicate_offset[i];
        }
    }

    if constexpr (join_features.need_replication)
    {
        added_columns.offsets_to_replicate.resize_assume_reserved(left_block_rows);
        added_columns.filter.resize_assume_reserved(left_block_rows);
    }
    added_columns.applyLazyDefaults();
}

}
