#include <Interpreters/HashJoin/AddedColumns.h>
#include <DataTypes/NullableUtils.h>

namespace DB
{

JoinOnKeyColumns::JoinOnKeyColumns(
    const ScatteredBlock & block, const Names & key_names_, const String & cond_column_name, const Sizes & key_sizes_)
    : key_names(key_names_)
    /// Rare case, when keys are constant or low cardinality. To avoid code bloat, simply materialize them.
    , materialized_keys_holder(JoinCommon::materializeColumns(block.getSourceBlock(), key_names))
    , key_columns(JoinCommon::getRawPointers(materialized_keys_holder))
    , null_map(nullptr)
    , null_map_holder(extractNestedColumnsAndNullMap(key_columns, null_map))
    , join_mask_column(JoinCommon::getColumnAsMask(block.getSourceBlock(), cond_column_name))
    , key_sizes(key_sizes_)
{
}

size_t LazyOutput::buildOutput(size_t size_to_reserve,
    MutableColumns & columns,
    const UInt64 * row_refs_begin,
    const UInt64 * row_refs_end,
    size_t rows_offset,
    size_t rows_limit) const
{
    if (!output_by_row_list)
        buildOutputFromBlocks<false>(size_to_reserve, columns, row_refs_begin, row_refs_end);
    else
    {
        if (rows_limit)
            return buildOutputFromBlocksLimitAndOffset(columns, row_refs_begin, row_refs_end, rows_offset, rows_limit);
        if (!join_data_sorted && join_data_avg_perkey_rows < output_by_row_list_threshold)
            buildOutputFromBlocks<true>(size_to_reserve, columns, row_refs_begin, row_refs_end);
        else
            buildOutputFromRowRefLists(size_to_reserve, columns, row_refs_begin, row_refs_end);
    }
    /// Without rows_limit, all possible rows are added and result value is not used.
    return 0;
}

void LazyOutput::buildOutputFromRowRefLists(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    for (size_t i = 0; i < columns.size(); ++i)
    {
        auto & col = columns[i];
        col->reserve(col->size() + size_to_reserve);
        col->fillFromRowRefs(type_name[i].type, right_indexes[i], row_refs_begin, row_refs_end, join_data_sorted);
    }
}

void LazyOutput::buildJoinGetOutput(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    for (size_t i = 0; i < columns.size(); ++i)
    {
        auto & col = columns[i];
        col->reserve(col->size() + size_to_reserve);
        for (const UInt64 * row_ref_i = row_refs_begin; row_ref_i != row_refs_end; ++row_ref_i)
        {
            if (!*row_ref_i)
            {
                type_name[i].type->insertDefaultInto(*col);
                continue;
            }
            const auto * row_ref = reinterpret_cast<const RowRef *>(*row_ref_i);
            const auto & column_from_block = *(*row_ref->columns)[right_indexes[i]];
            if (auto * nullable_col = typeid_cast<ColumnNullable *>(col.get()); nullable_col && !column_from_block.isNullable())
                nullable_col->insertFromNotNullable(column_from_block, row_ref->row_num);
            else
                col->insertFrom(column_from_block, row_ref->row_num);
        }
    }
}

/// Returns how many rows were added to columns, up to rows_limit
size_t LazyOutput::buildOutputFromBlocksLimitAndOffset(
    MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end,
    size_t rows_offset, size_t rows_limit) const
{
    if (columns.empty())
        return rows_limit;
    std::vector<const Columns *> many_columns;
    std::vector<UInt32> row_nums;
    many_columns.reserve(rows_limit);
    row_nums.reserve(rows_limit);

    for (const UInt64 * row_ref_i = row_refs_begin; rows_limit > 0 && row_ref_i != row_refs_end; ++row_ref_i)
    {
        if (*row_ref_i)
        {
            const RowRefList * row_ref_list = reinterpret_cast<const RowRefList *>(*row_ref_i);
            for (auto it = row_ref_list->begin(); rows_limit > 0 && it.ok(); ++it)
            {
                if (rows_offset)
                {
                    --rows_offset;
                    continue;
                }
                many_columns.emplace_back(it->columns);
                row_nums.emplace_back(it->row_num);
                --rows_limit;
            }
        }
        else
        {
            if (rows_offset)
            {
                --rows_offset;
                continue;
            }
            many_columns.emplace_back(nullptr);
            row_nums.emplace_back(0);
            --rows_limit;
        }
    }

    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns[i]->fillFromBlocksAndRowNumbers(type_name[i].type, right_indexes[i], many_columns, row_nums);
    }
    return row_nums.size();
}


template<bool from_row_list>
void LazyOutput::buildOutputFromBlocks(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    if (columns.empty())
        return;
    std::vector<const Columns *> many_columns;
    std::vector<UInt32> row_nums;
    many_columns.reserve(size_to_reserve);
    row_nums.reserve(size_to_reserve);
    for (const UInt64 * row_ref_i = row_refs_begin; row_ref_i != row_refs_end; ++row_ref_i)
    {
        if (*row_ref_i)
        {
            if constexpr (from_row_list)
            {
                const RowRefList * row_ref_list = reinterpret_cast<const RowRefList *>(*row_ref_i);
                for (auto it = row_ref_list->begin(); it.ok(); ++it)
                {
                    many_columns.emplace_back(it->columns);
                    row_nums.emplace_back(it->row_num);
                }
            }
            else
            {
                const RowRef * row_ref = reinterpret_cast<const RowRefList *>(*row_ref_i);
                many_columns.emplace_back(row_ref->columns);
                row_nums.emplace_back(row_ref->row_num);
            }
        }
        else
        {
            many_columns.emplace_back(nullptr);
            row_nums.emplace_back(0);
        }
    }
    for (size_t i = 0; i < columns.size(); ++i)
    {
        columns[i]->fillFromBlocksAndRowNumbers(type_name[i].type, right_indexes[i], many_columns, row_nums);
    }
}

template<>
void AddedColumns<false>::applyLazyDefaults()
{
    if (lazy_defaults_count)
    {
        for (size_t j = 0, size = lazy_output.right_indexes.size(); j < size; ++j)
            JoinCommon::addDefaultValues(*columns[j], lazy_output.type_name[j].type, lazy_defaults_count);
        lazy_defaults_count = 0;
    }
}

template<>
void AddedColumns<true>::applyLazyDefaults() {}

template <>
void AddedColumns<false>::appendFromBlock(const RowRef * row_ref, const bool has_defaults)
{
    if (has_defaults)
        applyLazyDefaults();

#ifndef NDEBUG
    checkColumns(*row_ref->columns);
#endif
    if (is_join_get)
    {
        size_t right_indexes_size = lazy_output.right_indexes.size();
        for (size_t j = 0; j < right_indexes_size; ++j)
        {
            const auto & column_from_block = (*row_ref->columns)[lazy_output.right_indexes[j]];
            if (auto * nullable_col = nullable_column_ptrs[j])
                nullable_col->insertFromNotNullable(*column_from_block, row_ref->row_num);
            else
                columns[j]->insertFrom(*column_from_block, row_ref->row_num);
        }
    }
    else
    {
        size_t right_indexes_size = lazy_output.right_indexes.size();
        for (size_t j = 0; j < right_indexes_size; ++j)
        {
            const auto & column_from_block = (*row_ref->columns)[lazy_output.right_indexes[j]];
            columns[j]->insertFrom(*column_from_block, row_ref->row_num);
        }
    }
}

template <>
void AddedColumns<true>::appendFromBlock(const RowRef * row_ref, bool)
{
#ifndef NDEBUG
    checkColumns(*row_ref->columns);
#endif
    if (has_columns_to_add)
    {
        lazy_output.addRowRef(row_ref);
    }
}

}
