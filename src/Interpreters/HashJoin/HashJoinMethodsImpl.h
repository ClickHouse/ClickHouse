#pragma once
#include <type_traits>
#include <Interpreters/HashJoin/HashJoinMethods.h>
#include "Columns/IColumn.h"
#include "Interpreters/HashJoin/ScatteredBlock.h"

namespace DB
{
namespace ErrorCodes
{
extern const int UNSUPPORTED_JOIN_KEYS;
extern const int LOGICAL_ERROR;
}
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::insertFromBlockImpl(
    HashJoin & join,
    HashJoin::Type type,
    MapsTemplate & maps,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const Block * stored_block,
    const ScatteredBlock::Selector & selector,
    ConstNullMapPtr null_map,
    UInt8ColumnDataPtr join_mask,
    Arena & pool,
    bool & is_inserted)
{
    switch (type)
    {
        case HashJoin::Type::EMPTY:
            [[fallthrough]];
        case HashJoin::Type::CROSS:
            /// Do nothing. We will only save block, and it is enough
            is_inserted = true;
            return 0;

#define M(TYPE) \
    case HashJoin::Type::TYPE: \
        if (selector.isContinuousRange()) \
            return insertFromBlockImplTypeCase< \
                typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
                join, *maps.TYPE, key_columns, key_sizes, stored_block, selector.getRange(), null_map, join_mask, pool, is_inserted); \
        else \
            return insertFromBlockImplTypeCase< \
                typename KeyGetterForType<HashJoin::Type::TYPE, std::remove_reference_t<decltype(*maps.TYPE)>>::Type>( \
                join, *maps.TYPE, key_columns, key_sizes, stored_block, selector.getIndexes(), null_map, join_mask, pool, is_inserted); \
        break;

            APPLY_FOR_JOIN_VARIANTS(M)
#undef M
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
Block HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinBlockImpl(
    const HashJoin & join, Block & block, const Block & block_with_columns_to_add, const MapsTemplateVector & maps_, bool is_join_get)
{
    ScatteredBlock scattered_block{block};
    auto ret = joinBlockImpl(join, scattered_block, block_with_columns_to_add, maps_, is_join_get);
    ret.filterBySelector();
    scattered_block.filterBySelector();
    block = std::move(scattered_block.getSourceBlock());
    return ret.getSourceBlock();
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
ScatteredBlock HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinBlockImpl(
    const HashJoin & join,
    ScatteredBlock & block,
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

    auto & source_block = block.getSourceBlock();
    size_t existing_columns = source_block.columns();

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
        join_features.is_asof_join,
        is_join_get);

    bool has_required_right_keys = (join.required_right_keys.columns() != 0);
    added_columns.need_filter = join_features.need_filter || has_required_right_keys;
    added_columns.max_joined_block_rows = join.max_joined_block_rows;
    if (!added_columns.max_joined_block_rows)
        added_columns.max_joined_block_rows = std::numeric_limits<size_t>::max();
    else
        added_columns.reserve(join_features.need_replication);

    const size_t num_joined = switchJoinRightColumns(maps_, added_columns, join.data->type, *join.used_flags);
    /// Do not hold memory for join_on_keys anymore
    added_columns.join_on_keys.clear();
    auto remaining_block = block.cut(num_joined);

    if (is_join_get)
        added_columns.buildJoinGetOutput();
    else
        added_columns.buildOutput();

    if constexpr (join_features.need_filter)
        block.filter(added_columns.filter);

    block.filterBySelector();

    const auto & table_join = join.table_join;
    std::set<size_t> block_columns_to_erase;
    if (join.canRemoveColumnsFromLeftBlock())
    {
        std::unordered_set<String> left_output_columns;
        for (const auto & out_column : table_join->getOutputColumns(JoinTableSide::Left))
            left_output_columns.insert(out_column.name);
        for (size_t i = 0; i < source_block.columns(); ++i)
        {
            if (!left_output_columns.contains(source_block.getByPosition(i).name))
                block_columns_to_erase.insert(i);
        }
    }

    for (size_t i = 0; i < added_columns.size(); ++i)
        source_block.insert(added_columns.moveColumn(i));

    std::vector<size_t> right_keys_to_replicate [[maybe_unused]];

    if constexpr (join_features.need_filter)
    {
        /// Add join key columns from right block if needed using value from left table because of equality
        for (size_t i = 0; i < join.required_right_keys.columns(); ++i)
        {
            const auto & right_key = join.required_right_keys.getByPosition(i);
            /// asof column is already in block.
            if (join_features.is_asof_join && right_key.name == join.table_join->getOnlyClause().key_names_right.back())
                continue;

            const auto & left_column = block.getByName(join.required_right_keys_sources[i]);
            const auto & right_col_name = join.getTableJoin().renamedRightColumnName(right_key.name);
            auto right_col = copyLeftKeyColumnToRight(right_key.type, right_col_name, left_column);
            source_block.insert(std::move(right_col));
        }
    }
    else if (has_required_right_keys)
    {
        /// Add join key columns from right block if needed.
        for (size_t i = 0; i < join.required_right_keys.columns(); ++i)
        {
            const auto & right_key = join.required_right_keys.getByPosition(i);
            auto right_col_name = join.getTableJoin().renamedRightColumnName(right_key.name);
            /// asof column is already in block.
            if (join_features.is_asof_join && right_key.name == join.table_join->getOnlyClause().key_names_right.back())
                continue;

            const auto & left_column = block.getByName(join.required_right_keys_sources[i]);
            auto right_col = copyLeftKeyColumnToRight(right_key.type, right_col_name, left_column, &added_columns.filter);
            source_block.insert(std::move(right_col));

            if constexpr (join_features.need_replication)
                right_keys_to_replicate.push_back(source_block.getPositionByName(right_col_name));
        }
    }

    if constexpr (join_features.need_replication)
    {
        IColumn::Offsets & offsets = *added_columns.offsets_to_replicate;

        chassert(block);
        chassert(offsets.size() == block.rows());

        auto && columns = block.getSourceBlock().getColumns();
        for (size_t i = 0; i < existing_columns; ++i)
            columns[i] = columns[i]->replicate(offsets);
        for (size_t pos : right_keys_to_replicate)
            columns[pos] = columns[pos]->replicate(offsets);

        block.getSourceBlock().setColumns(columns);
        block = ScatteredBlock(std::move(block).getSourceBlock());
    }

    block.getSourceBlock().erase(block_columns_to_erase);

    return remaining_block;
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
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::insertFromBlockImplTypeCase(
    HashJoin & join,
    HashMap & map,
    const ColumnRawPtrs & key_columns,
    const Sizes & key_sizes,
    const Block * stored_block,
    const Selector & selector,
    ConstNullMapPtr null_map,
    UInt8ColumnDataPtr join_mask,
    Arena & pool,
    bool & is_inserted)
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
        chassert(!join_mask || ind < join_mask->size());
        if (join_mask && !(*join_mask)[ind])
            continue;

        if constexpr (is_asof_join)
            Inserter<HashMap, KeyGetter>::insertAsof(join, map, key_getter, stored_block, ind, pool, *asof_column);
        else if constexpr (mapped_one)
            is_inserted |= Inserter<HashMap, KeyGetter>::insertOne(join, map, key_getter, stored_block, ind, pool);
        else
            Inserter<HashMap, KeyGetter>::insertAll(join, map, key_getter, stored_block, ind, pool);
    }
    return map.getBufferSizeInCells();
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::switchJoinRightColumns(
    const std::vector<const MapsTemplate *> & mapv,
    AddedColumns & added_columns,
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
                    std::move(key_getter_vector), a_map_type_vector, added_columns, used_flags);
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
        return joinRightColumnsSwitchNullability<KeyGetter>(std::move(key_getter_vector), a_map_type_vector, added_columns, used_flags); \
    }
            APPLY_FOR_JOIN_VARIANTS(M)
#undef M

        default:
            throw Exception(ErrorCodes::UNSUPPORTED_JOIN_KEYS, "Unsupported JOIN keys (type: {})", type);
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename KeyGetter, typename Map, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchNullability(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags)
{
    if (added_columns.need_filter)
    {
        return joinRightColumnsSwitchMultipleDisjuncts<KeyGetter, Map, true>(
            std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags);
    }

    return joinRightColumnsSwitchMultipleDisjuncts<KeyGetter, Map, false>(
        std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags);
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename KeyGetter, typename Map, bool need_filter, typename AddedColumns>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsSwitchMultipleDisjuncts(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    if constexpr (join_features.is_maps_all)
    {
        if (added_columns.additional_filter_expression)
        {
            bool mark_per_row_used = join_features.right || join_features.full || mapv.size() > 1;
            return joinRightColumnsWithAddtitionalFilter<KeyGetter, Map>(
                std::forward<std::vector<KeyGetter>>(key_getter_vector), mapv, added_columns, used_flags, need_filter, mark_per_row_used);
        }
    }

    if (added_columns.additional_filter_expression)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Additional filter expression is not supported for this JOIN");

    auto & block = added_columns.src_block;
    if (block.getSelector().isContinuousRange())
    {
        if (mapv.size() > 1)
            return joinRightColumns<KeyGetter, Map, need_filter, true>(
                std::move(key_getter_vector), mapv, added_columns, used_flags, block.getSelector().getRange());
        else
            return joinRightColumns<KeyGetter, Map, need_filter, false>(
                std::move(key_getter_vector), mapv, added_columns, used_flags, block.getSelector().getRange());
    }
    else
    {
        if (mapv.size() > 1)
            return joinRightColumns<KeyGetter, Map, need_filter, true>(
                std::move(key_getter_vector), mapv, added_columns, used_flags, block.getSelector().getIndexes());
        else
            return joinRightColumns<KeyGetter, Map, need_filter, false>(
                std::move(key_getter_vector), mapv, added_columns, used_flags, block.getSelector().getIndexes());
    }
}


/// Joins right table columns which indexes are present in right_indexes using specified map.
/// Makes filter (1 if row presented in right table) and returns offsets to replicate (for ALL JOINS).
template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename KeyGetter, typename Map, bool need_filter, bool flag_per_row, typename AddedColumns, typename Selector>
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumns(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags,
    const Selector & selector)
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;

    auto & block = added_columns.src_block;
    size_t rows = block.rows();
    if constexpr (need_filter)
        added_columns.filter = IColumn::Filter(rows, 0);
    if constexpr (!flag_per_row && (STRICTNESS == JoinStrictness::All || (STRICTNESS == JoinStrictness::Semi && KIND == JoinKind::Right)))
        added_columns.output_by_row_list = true;

    Arena pool;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(rows);

    IColumn::Offset current_offset = 0;
    size_t max_joined_block_rows = added_columns.max_joined_block_rows;
    size_t i = 0;
    for (; i < rows; ++i)
    {
        size_t ind = 0;
        if constexpr (std::is_same_v<std::decay_t<Selector>, ScatteredBlock::Indexes>)
            ind = selector.getData()[i];
        else
            ind = selector.first + i;

        if constexpr (join_features.need_replication)
        {
            if (unlikely(current_offset >= max_joined_block_rows))
            {
                added_columns.offsets_to_replicate->resize(i);
                added_columns.filter.resize(i);
                break;
            }
        }

        bool right_row_found = false;
        KnownRowsHolder<flag_per_row> known_rows;
        for (size_t onexpr_idx = 0; onexpr_idx < added_columns.join_on_keys.size(); ++onexpr_idx)
        {
            const auto & join_keys = added_columns.join_on_keys[onexpr_idx];
            if (join_keys.null_map && (*join_keys.null_map)[ind])
                continue;

            bool row_acceptable = !join_keys.isRowFiltered(ind);
            using FindResult = typename KeyGetter::FindResult;
            auto find_result = row_acceptable ? key_getter_vector[onexpr_idx].findKey(*(mapv[onexpr_idx]), ind, pool) : FindResult();

            if (find_result.isFound())
            {
                right_row_found = true;
                auto & mapped = find_result.getMapped();
                if constexpr (join_features.is_asof_join)
                {
                    const IColumn & left_asof_key = added_columns.leftAsofKey();

                    auto row_ref = mapped->findAsof(left_asof_key, ind);
                    if (row_ref && row_ref->block)
                    {
                        setUsed<need_filter>(added_columns.filter, i);
                        if constexpr (flag_per_row)
                            used_flags.template setUsed<join_features.need_flags, flag_per_row>(row_ref->block, row_ref->row_num, 0);
                        else
                            used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);

                        added_columns.appendFromBlock(row_ref, join_features.add_missing);
                    }
                    else
                        addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
                }
                else if constexpr (join_features.is_all_join)
                {
                    setUsed<need_filter>(added_columns.filter, i);
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
                        setUsed<need_filter>(added_columns.filter, i);
                        addFoundRowAll<Map, join_features.add_missing>(mapped, added_columns, current_offset, known_rows, used_flags_opt);
                    }
                }
                else if constexpr (join_features.is_any_join && join_features.inner)
                {
                    bool used_once = used_flags.template setUsedOnce<join_features.need_flags, flag_per_row>(find_result);

                    /// Use first appeared left key only
                    if (used_once)
                    {
                        setUsed<need_filter>(added_columns.filter, i);
                        added_columns.appendFromBlock(&mapped, join_features.add_missing);
                    }

                    break;
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
                    setUsed<need_filter>(added_columns.filter, i);
                    used_flags.template setUsed<join_features.need_flags, flag_per_row>(find_result);
                    added_columns.appendFromBlock(&mapped, join_features.add_missing);

                    if (join_features.is_any_or_semi_join)
                    {
                        break;
                    }
                }
            }
        }

        if (!right_row_found)
        {
            if constexpr (join_features.is_anti_join && join_features.left)
                setUsed<need_filter>(added_columns.filter, i);
            addNotFoundRow<join_features.add_missing, join_features.need_replication>(added_columns, current_offset);
        }

        if constexpr (join_features.need_replication)
        {
            (*added_columns.offsets_to_replicate)[i] = current_offset;
        }
    }

    added_columns.applyLazyDefaults();
    return i;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <bool need_filter>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::setUsed(IColumn::Filter & filter [[maybe_unused]], size_t pos [[maybe_unused]])
{
    if constexpr (need_filter)
        filter[pos] = 1;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
template <typename AddedColumns>
ColumnPtr HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::buildAdditionalFilter(
    size_t left_start_row,
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
        const Block & sample_right_block = *((*selected_rows.begin())->block);
        if (!sample_right_block || !added_columns.additional_filter_expression)
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
        NameSet required_column_names;
        for (auto & col : required_cols)
            required_column_names.insert(col.name);

        Block executed_block;
        size_t right_col_pos = 0;
        for (const auto & col : sample_right_block.getColumnsWithTypeAndName())
        {
            if (required_column_names.contains(col.name))
            {
                auto new_col = col.column->cloneEmpty();
                for (const auto & selected_row : selected_rows)
                {
                    const auto & src_col = selected_row->block->getByPosition(right_col_pos);
                    new_col->insertFrom(*src_col.column, selected_row->row_num);
                }
                executed_block.insert({std::move(new_col), col.type, col.name});
            }
            right_col_pos += 1;
        }

        for (const auto & col_name : required_column_names)
        {
            const auto * src_col = added_columns.left_block.findByName(col_name);
            if (!src_col)
                continue;
            auto new_col = src_col->column->cloneEmpty();
            size_t prev_left_offset = 0;
            for (size_t i = 1; i < row_replicate_offset.size(); ++i)
            {
                const size_t & left_offset = row_replicate_offset[i];
                size_t rows = left_offset - prev_left_offset;
                if (rows)
                    new_col->insertManyFrom(*src_col->column, left_start_row + i - 1, rows);
                prev_left_offset = left_offset;
            }
            executed_block.insert({std::move(new_col), src_col->type, col_name});
        }
        if (!executed_block)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "required columns: [{}], but not found any in left/right table. right table: {}, left table: {}",
                required_cols.toString(),
                sample_right_block.dumpNames(),
                added_columns.left_block.dumpNames());
        }

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
size_t HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::joinRightColumnsWithAddtitionalFilter(
    std::vector<KeyGetter> && key_getter_vector,
    const std::vector<const Map *> & mapv,
    AddedColumns & added_columns,
    JoinStuff::JoinUsedFlags & used_flags [[maybe_unused]],
    bool need_filter [[maybe_unused]],
    bool flag_per_row [[maybe_unused]])
{
    constexpr JoinFeatures<KIND, STRICTNESS, MapsTemplate> join_features;
    size_t left_block_rows = added_columns.rows_to_add;
    if (need_filter)
        added_columns.filter = IColumn::Filter(left_block_rows, 0);

    std::unique_ptr<Arena> pool;

    if constexpr (join_features.need_replication)
        added_columns.offsets_to_replicate = std::make_unique<IColumn::Offsets>(left_block_rows);

    std::vector<size_t> row_replicate_offset;
    row_replicate_offset.reserve(left_block_rows);

    using FindResult = typename KeyGetter::FindResult;
    size_t max_joined_block_rows = added_columns.max_joined_block_rows;
    size_t left_row_iter = 0;
    PreSelectedRows selected_rows;
    selected_rows.reserve(left_block_rows);
    std::vector<FindResult> find_results;
    find_results.reserve(left_block_rows);
    bool exceeded_max_block_rows = false;
    IColumn::Offset total_added_rows = 0;
    IColumn::Offset current_added_rows = 0;

    auto collect_keys_matched_rows_refs = [&]()
    {
        pool = std::make_unique<Arena>();
        find_results.clear();
        row_replicate_offset.clear();
        row_replicate_offset.push_back(0);
        current_added_rows = 0;
        selected_rows.clear();
        for (; left_row_iter < left_block_rows; ++left_row_iter)
        {
            if constexpr (join_features.need_replication)
            {
                if (unlikely(total_added_rows + current_added_rows >= max_joined_block_rows))
                {
                    break;
                }
            }
            KnownRowsHolder<true> all_flag_known_rows;
            KnownRowsHolder<false> single_flag_know_rows;
            for (size_t join_clause_idx = 0; join_clause_idx < added_columns.join_on_keys.size(); ++join_clause_idx)
            {
                const auto & join_keys = added_columns.join_on_keys[join_clause_idx];
                if (join_keys.null_map && (*join_keys.null_map)[left_row_iter])
                    continue;

                bool row_acceptable = !join_keys.isRowFiltered(left_row_iter);
                auto find_result = row_acceptable
                    ? key_getter_vector[join_clause_idx].findKey(*(mapv[join_clause_idx]), left_row_iter, *pool)
                    : FindResult();

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
    };

    auto copy_final_matched_rows = [&](size_t left_start_row, ColumnPtr filter_col)
    {
        const PaddedPODArray<UInt8> & filter_flags = assert_cast<const ColumnUInt8 &>(*filter_col).getData();

        size_t prev_replicated_row = 0;
        auto selected_right_row_it = selected_rows.begin();
        size_t find_result_index = 0;
        for (size_t i = 1, n = row_replicate_offset.size(); i < n; ++i)
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
                                        (*selected_right_row_it)->block, (*selected_right_row_it)->row_num, 0);
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
                                    (*selected_right_row_it)->block, (*selected_right_row_it)->row_num, 0);
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
                                used_flags.template setUsed<true, true>((*selected_right_row_it)->block, (*selected_right_row_it)->row_num, 0);
                        }
                        else
                        {
                            any_matched = true;
                            total_added_rows += 1;
                            added_columns.appendFromBlock(*selected_right_row_it, join_features.add_missing);
                            used_flags.template setUsed<join_features.need_flags, true>((*selected_right_row_it)->block, (*selected_right_row_it)->row_num, 0);
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
                            setUsed<true>(added_columns.filter, left_start_row + i - 1);
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
                        setUsed<true>(added_columns.filter, left_start_row + i - 1);
                    if constexpr (join_features.add_missing)
                        added_columns.applyLazyDefaults();
                }
            }
            find_result_index += (prev_replicated_row != row_replicate_offset[i]);

            if constexpr (join_features.need_replication)
            {
                (*added_columns.offsets_to_replicate)[left_start_row + i - 1] = total_added_rows;
            }
            prev_replicated_row = row_replicate_offset[i];
        }
    };

    while (left_row_iter < left_block_rows && !exceeded_max_block_rows)
    {
        auto left_start_row = left_row_iter;
        collect_keys_matched_rows_refs();
        if (selected_rows.size() != current_added_rows || row_replicate_offset.size() != left_row_iter - left_start_row + 1)
        {
            throw Exception(
                ErrorCodes::LOGICAL_ERROR,
                "Sizes are mismatched. selected_rows.size:{}, current_added_rows:{}, row_replicate_offset.size:{}, left_row_iter: {}, "
                "left_start_row: {}",
                selected_rows.size(),
                current_added_rows,
                row_replicate_offset.size(),
                left_row_iter,
                left_start_row);
        }
        auto filter_col = buildAdditionalFilter(left_start_row, selected_rows, row_replicate_offset, added_columns);
        copy_final_matched_rows(left_start_row, filter_col);

        if constexpr (join_features.need_replication)
        {
            // Add a check for current_added_rows to avoid run the filter expression on too small size batch.
            if (total_added_rows >= max_joined_block_rows || current_added_rows < 1024)
                exceeded_max_block_rows = true;
        }
    }

    if constexpr (join_features.need_replication)
    {
        added_columns.offsets_to_replicate->resize_assume_reserved(left_row_iter);
        added_columns.filter.resize_assume_reserved(left_row_iter);
    }
    added_columns.applyLazyDefaults();
    return left_row_iter;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
ColumnWithTypeAndName HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::copyLeftKeyColumnToRight(
    const DataTypePtr & right_key_type,
    const String & renamed_right_column,
    const ColumnWithTypeAndName & left_column,
    const IColumn::Filter * null_map_filter)
{
    ColumnWithTypeAndName right_column = left_column;
    right_column.name = renamed_right_column;

    if (null_map_filter)
        right_column.column = JoinCommon::filterWithBlanks(right_column.column, *null_map_filter);

    bool should_be_nullable = isNullableOrLowCardinalityNullable(right_key_type);
    if (null_map_filter)
        correctNullabilityInplace(right_column, should_be_nullable, *null_map_filter);
    else
        correctNullabilityInplace(right_column, should_be_nullable);

    if (!right_column.type->equals(*right_key_type))
    {
        right_column.column = castColumnAccurate(right_column, right_key_type);
        right_column.type = right_key_type;
    }

    right_column.column = right_column.column->convertToFullColumnIfConst();
    return right_column;
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::correctNullabilityInplace(ColumnWithTypeAndName & column, bool nullable)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
    }
    else
    {
        /// We have to replace values masked by NULLs with defaults.
        if (column.column)
            if (const auto * nullable_column = checkAndGetColumn<ColumnNullable>(&*column.column))
                column.column = JoinCommon::filterWithBlanks(column.column, nullable_column->getNullMapColumn().getData(), true);

        JoinCommon::removeColumnNullability(column);
    }
}

template <JoinKind KIND, JoinStrictness STRICTNESS, typename MapsTemplate>
void HashJoinMethods<KIND, STRICTNESS, MapsTemplate>::correctNullabilityInplace(
    ColumnWithTypeAndName & column, bool nullable, const IColumn::Filter & negative_null_map)
{
    if (nullable)
    {
        JoinCommon::convertColumnToNullable(column);
        if (column.type->isNullable() && !negative_null_map.empty())
        {
            MutableColumnPtr mutable_column = IColumn::mutate(std::move(column.column));
            assert_cast<ColumnNullable &>(*mutable_column).applyNegatedNullMap(negative_null_map);
            column.column = std::move(mutable_column);
        }
    }
    else
        JoinCommon::removeColumnNullability(column);
}
}
