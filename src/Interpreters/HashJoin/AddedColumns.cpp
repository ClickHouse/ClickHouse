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

template<typename F>
void LazyOutput::dispatchRowStore(F && f) const
{
    if (row_store_outputs.empty())
        f.template operator()<false>();
    else
        f.template operator()<true>();
}

size_t LazyOutput::buildOutput(
    size_t size_to_reserve,
    const Block & left_block,
    const IColumn::Offsets & left_offsets,
    MutableColumns & columns,
    const UInt64 * row_refs_begin,
    const UInt64 * row_refs_end,
    size_t rows_offset,
    size_t rows_limit,
    size_t bytes_limit) const
{
    if (!output_by_row_list)
        dispatchRowStore([&]<bool from_row_store>()
        {
            buildOutputFromBlocks<false, from_row_store>(size_to_reserve, columns, row_refs_begin, row_refs_end);
        });
    else
    {
        if (rows_limit)
        {
            PaddedPODArray<UInt64> left_sizes;
            if (bytes_limit)
            {
                for (const auto & col : left_block)
                    col.column->collectSerializedValueSizes(left_sizes, nullptr, nullptr);
            }
            
            size_t added_rows = 0;
            dispatchRowStore([&]<bool from_row_store>()
            {
                added_rows = buildOutputFromBlocksLimitAndOffset<from_row_store>(columns, row_refs_begin, row_refs_end, left_sizes, left_offsets, rows_offset, rows_limit, bytes_limit);
            });
            return added_rows;
        }
        if (!join_data_sorted && join_data_avg_perkey_rows < output_by_row_list_threshold)
            dispatchRowStore([&]<bool from_row_store>()
            {
                buildOutputFromBlocks<true, from_row_store>(size_to_reserve, columns, row_refs_begin, row_refs_end);
            });

        else
            dispatchRowStore([&]<bool from_row_store>()
            {
                buildOutputFromRowRefLists<from_row_store>(size_to_reserve, columns, row_refs_begin, row_refs_end);
            });
    }
    /// Without rows_limit, all possible rows are added and result value is not used.
    return 0;
}

template<bool from_row_store>
void LazyOutput::buildOutputFromRowRefLists(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    if constexpr (from_row_store)
    {
        chassert(!join_data_sorted, "Row store should be disabled when join data rearange optimization is used.");
        for (auto [dst_idx, offset, size] : row_store_outputs)
        {
            auto & col = columns[dst_idx];
            col->reserve(col->size() + size_to_reserve);
            columns[dst_idx]->fillFromRowRefsWithRowStore(type_name[dst_idx].type, offset, size, row_refs_begin, row_refs_end);
        }
    }

    for (auto [dst_idx, column_idx] : column_outputs)
    {
        auto & col = columns[dst_idx];
        col->reserve(col->size() + size_to_reserve);
        col->fillFromRowRefs(type_name[dst_idx].type, column_idx, row_refs_begin, row_refs_end, join_data_sorted);
    }
}

std::pair<const IColumn *, size_t> getBlockColumnAndRow(const RowRef * row_ref, size_t column_index)
{
    if (const auto * replicated_column_from_block = (*row_ref->columns_info).replicated_columns[column_index])
        return {replicated_column_from_block->getNestedColumn().get(), replicated_column_from_block->getIndexes().getIndexAt(row_ref->row_num)};
    return {(*row_ref->columns_info).columns[column_index].get(), row_ref->row_num};
}

template<bool from_row_store>
void LazyOutput::buildJoinGetOutput(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    if constexpr (from_row_store)
    {
        for (auto [dst_idx, offset, size] : row_store_outputs)
        {
            auto & col = columns[dst_idx];
            col->reserve(col->size() + size_to_reserve);
            for (const UInt64 * row_ref_i = row_refs_begin; row_ref_i != row_refs_end; ++row_ref_i)
            {
                if (!*row_ref_i)
                {
                    type_name[dst_idx].type->insertDefaultInto(*col);
                    continue;
                }
                const auto * row_ref = reinterpret_cast<const RowRef *>(*row_ref_i);
                const char * row_data = row_ref->columns_info->row_store->getRowAt(row_ref->row_num);
                col->insertData(row_data + offset, size);
            }
        }
    }
    
    for (auto [dst_idx, column_idx] : column_outputs)      
    {
        auto & col = columns[dst_idx];
        col->reserve(col->size() + size_to_reserve);
        for (const UInt64 * row_ref_i = row_refs_begin; row_ref_i != row_refs_end; ++row_ref_i)
        {
            if (!*row_ref_i)
            {
                type_name[dst_idx].type->insertDefaultInto(*col);
                continue;
            }
            const auto * row_ref = reinterpret_cast<const RowRef *>(*row_ref_i);
            const auto [column_from_block, row_num] = getBlockColumnAndRow(row_ref, column_idx);
            if (auto * nullable_col = typeid_cast<ColumnNullable *>(col.get()); nullable_col && !column_from_block->isNullable())
                nullable_col->insertFromNotNullable(*column_from_block, row_num);
            else
                col->insertFrom(*column_from_block, row_num);
        }
    }
}

void LazyOutput::buildJoinGetOutput(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    dispatchRowStore([&]<bool from_row_store>()
    {
        buildJoinGetOutput<from_row_store>(size_to_reserve, columns, row_refs_begin, row_refs_end);
    });
}

/// Returns how many rows were added to columns, up to rows_limit
template<bool from_row_store>
size_t LazyOutput::buildOutputFromBlocksLimitAndOffset(
    MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end,
    const PaddedPODArray<UInt64> & left_sizes, const IColumn::Offsets & left_offsets,
    size_t rows_offset, size_t rows_limit, size_t bytes_limit) const
{
    if (columns.empty())
        return rows_limit;

    ColumnsWithRowNumbers columns_with_row_numbers;
    auto & many_columns = columns_with_row_numbers.columns;
    auto & row_nums = columns_with_row_numbers.row_numbers;
    many_columns.reserve(rows_limit);
    row_nums.reserve(rows_limit);

    [[maybe_unused]] PaddedPODArray<const char *> row_store_ptrs;
    if constexpr (from_row_store)
        row_store_ptrs.reserve(rows_limit);

    size_t row_idx = 0;
    size_t total_byte_size = 0;
    size_t left_idx = 0; /// position in non-replicated left block
    for (const UInt64 * row_ref_i = row_refs_begin; rows_limit > 0 && row_ref_i != row_refs_end; ++row_ref_i)
    {
        if (*row_ref_i)
        {
            const RowRefList * row_ref_list = reinterpret_cast<const RowRefList *>(*row_ref_i);
            for (auto it = row_ref_list->begin(); rows_limit > 0 && it.ok(); ++it)
            {
                if (row_idx < rows_offset)
                {
                    ++row_idx;
                    continue;
                }

                if (bytes_limit)
                {
                    /// Check if we are still in the same left row or moved to next one
                    while (row_idx >= left_offsets[left_idx])
                        ++left_idx;
                    chassert(left_sizes.size() > left_idx);
                    total_byte_size += left_sizes[left_idx];

                    /// Add size of right matched rows
                    if constexpr (from_row_store)
                        total_byte_size += (*it->columns_info).row_store->byteSizeAt(it->row_num);
                    for (const auto & col: (*it->columns_info).columns)
                        total_byte_size += col->byteSizeAt(it->row_num);
                }

                ++row_idx;
                --rows_limit;
                many_columns.emplace_back(it->columns_info);
                row_nums.emplace_back(it->row_num);
                if constexpr (from_row_store)
                    row_store_ptrs.emplace_back(it->columns_info->row_store->getRowAt(it->row_num));

                if (bytes_limit && total_byte_size > bytes_limit)
                    rows_limit = 0;
            }
        }
        else
        {
            if (row_idx < rows_offset)
            {
                ++row_idx;
                continue;
            }
            many_columns.emplace_back(nullptr);
            row_nums.emplace_back(0);
            if constexpr (from_row_store)
                row_store_ptrs.emplace_back(nullptr);
            ++row_idx;
            --rows_limit;
            /// Here we do not account byte size, since limit targets to avoid only huge blocks with large strings being replicated many times.
            /// In case of non-matched rows, left row is added only once and right columns are filled with defaults which have fixed small size.
        }
    }

    if constexpr (from_row_store)
        for (auto [dst_idx, offset, size] : row_store_outputs)
            columns[dst_idx]->fillFromRowStorePtrs(type_name[dst_idx].type, row_store_ptrs, offset, size);

    for (auto [dst_idx, column_idx] : column_outputs)
        columns[dst_idx]->fillFromBlocksAndRowNumbers(type_name[dst_idx].type, column_idx, columns_with_row_numbers);

    return row_nums.size();
}

template<bool from_row_list, bool from_row_store>
void LazyOutput::buildOutputFromBlocks(size_t size_to_reserve, MutableColumns & columns, const UInt64 * row_refs_begin, const UInt64 * row_refs_end) const
{
    if (columns.empty())
        return;

    ColumnsWithRowNumbers columns_with_row_numbers;
    auto & many_columns = columns_with_row_numbers.columns;
    auto & row_nums = columns_with_row_numbers.row_numbers;
    many_columns.reserve(size_to_reserve);
    row_nums.reserve(size_to_reserve);

    [[maybe_unused]] PaddedPODArray<const char *> row_store_ptrs;
    if constexpr (from_row_store)
        row_store_ptrs.reserve(size_to_reserve);

    auto collect = [&](const ColumnsInfo * columns_info, size_t row_num)
    {
        many_columns.emplace_back(columns_info);
        row_nums.emplace_back(row_num);
        if constexpr (from_row_store)
            row_store_ptrs.emplace_back(columns_info->row_store->getRowAt(row_num));                                                                                                                                     
    };

    auto collect_null = [&]()
    {
        many_columns.emplace_back(nullptr);
        row_nums.emplace_back(0);
        if constexpr (from_row_store)
            row_store_ptrs.emplace_back(nullptr);
    };

    for (const UInt64 * row_ref_i = row_refs_begin; row_ref_i != row_refs_end; ++row_ref_i)
    {
        if (!*row_ref_i)
        {
            collect_null();
            continue;
        }
        
        if constexpr (from_row_list)
        {
            const RowRefList * row_ref_list = reinterpret_cast<const RowRefList *>(*row_ref_i);
            for (auto it = row_ref_list->begin(); it.ok(); ++it)
                collect(it->columns_info, it->row_num);
        }
        else
        {
            const RowRef * row_ref = reinterpret_cast<const RowRefList *>(*row_ref_i);
            collect(row_ref->columns_info, row_ref->row_num);
        }
    }

    if constexpr (from_row_store)
        for (auto [dst_idx, offset, size] : row_store_outputs)                                                                                                                                                                                     
            columns[dst_idx]->fillFromRowStorePtrs(type_name[dst_idx].type, row_store_ptrs, offset, size);

    for (auto [dst_idx, column_idx] : column_outputs)                                                                                                                                                                                     
        columns[dst_idx]->fillFromBlocksAndRowNumbers(type_name[dst_idx].type, column_idx, columns_with_row_numbers);
}

template<>
void AddedColumns<false>::applyLazyDefaults()
{
    if (lazy_defaults_count)
    {
        for (size_t j = 0, size = lazy_output.type_name.size(); j < size; ++j)
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
    checkColumns(*row_ref->columns_info);
#endif
    if (!lazy_output.row_store_outputs.empty())
    {
        const char * row_data = row_ref->columns_info->row_store->getRowAt(row_ref->row_num);
        for (auto & [dst_idx, offset, size] : lazy_output.row_store_outputs)
            columns[dst_idx]->insertData(row_data + offset, size);
    }

    if (is_join_get)
    {        
        for (auto & [dst_idx, col_idx] : lazy_output.column_outputs)
        {
            const auto [column_from_block, row_num] = getBlockColumnAndRow(row_ref, col_idx);
            if (auto * nullable_col = nullable_column_ptrs[dst_idx])
                nullable_col->insertFromNotNullable(*column_from_block, row_num);
            else
                columns[dst_idx]->insertFrom(*column_from_block, row_num);
        }
    }
    else
    {
        for (auto & [dst_idx, col_idx] : lazy_output.column_outputs)
        {
            const auto [column_from_block, row_num] = getBlockColumnAndRow(row_ref, col_idx);
            columns[dst_idx]->insertFrom(*column_from_block, row_num);
        }
    }
}

template <>
void AddedColumns<true>::appendFromBlock(const RowRef * row_ref, bool)
{
#ifndef NDEBUG
    checkColumns(*row_ref->columns_info);
#endif
    if (has_columns_to_add)
    {
        lazy_output.addRowRef(row_ref);
    }
}

}
